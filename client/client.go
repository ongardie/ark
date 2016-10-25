/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package client

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"salesforce.com/zoolater/intframe"
	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
)

type Client struct {
	options      Options
	givenServers []string

	serversMutex       sync.Mutex
	knownServers       []string
	connectionAttempts uint64

	sessionMutex    sync.Mutex
	sessionId       proto.SessionId
	sessionPassword proto.SessionPassword
	lastZxidSeen    proto.ZXID

	shutdownMutex sync.Mutex
	shutdown      bool
	shutdownCh    chan struct{}

	watchesMutex sync.Mutex
	watches      map[proto.Path][]watch

	connMutex sync.Mutex
	conn      *Conn
}

type Conn struct {
	client         *Client
	sessionTimeout time.Duration

	requestCh chan request

	mutex         sync.Mutex
	netConn       net.Conn
	outstanding   []outstanding
	closed        bool
	closedCh      chan struct{}
	lastPingStart time.Time
	lastHeartbeat time.Time
}

func Go(watcher Watcher) Watcher {
	return func(event proto.EventType) {
		go watcher(event)
	}
}

type Watcher func(proto.EventType)

type watch struct {
	events  []proto.EventType
	watcher Watcher
}

type outstanding struct {
	xid     proto.Xid
	sent    time.Time
	handler func(Reply)
}

type request struct {
	opCode  proto.OpCode
	buf     []byte
	handler func(Reply)
}

type Options struct {
	SessionTimeout  time.Duration
	SessionId       proto.SessionId
	SessionPassword proto.SessionPassword
}

type Reply struct {
	Xid  proto.Xid
	Zxid proto.ZXID
	Err  proto.ErrCode
	Buf  []byte
}

func New(servers []string, options *Options) *Client {
	if len(servers) == 0 {
		log.Panic("client.New requires at least one server")
	}
	var opt Options
	if options != nil {
		opt = *options // TODO: deep clone
	}
	if opt.SessionTimeout == 0 {
		opt.SessionTimeout = time.Millisecond * 10000
	}
	if opt.SessionPassword == nil {
		opt.SessionPassword = make([]byte, 16)
	}

	knownServers := make([]string, len(servers))
	perm := rand.Perm(len(servers))
	for i, v := range perm {
		knownServers[v] = servers[i]
	}

	client := &Client{
		options:         opt,
		givenServers:    servers,
		knownServers:    knownServers,
		sessionId:       opt.SessionId,
		sessionPassword: opt.SessionPassword,
		shutdownCh:      make(chan struct{}),
		watches:         make(map[proto.Path][]watch),
	}
	client.conn = client.makeConn()
	go client.runConnect()
	return client
}

func (client *Client) Close() {
	client.shutdownMutex.Lock()
	defer client.shutdownMutex.Unlock()
	if !client.shutdown {
		client.shutdown = true
		close(client.shutdownCh)
	}
	client.Conn().close(proto.ErrClosing.Error())
}

func (client *Client) Session() (proto.SessionId, proto.SessionPassword) {
	client.sessionMutex.Lock()
	defer client.sessionMutex.Unlock()
	return client.sessionId, client.sessionPassword
}

func (client *Client) Conn() *Conn {
	client.connMutex.Lock()
	defer client.connMutex.Unlock()
	return client.conn
}

func (client *Client) RegisterWatcher(
	watcher Watcher,
	path proto.Path,
	firstEvent proto.EventType,
	additional ...proto.EventType) {
	client.watchesMutex.Lock()
	defer client.watchesMutex.Unlock()
	client.watches[path] = append(client.watches[path], watch{
		events:  append([]proto.EventType{firstEvent}, additional...),
		watcher: watcher,
	})
}

func (client *Client) closed() bool {
	select {
	case <-client.shutdownCh:
		return true
	default:
		return false
	}
}

func (client *Client) makeConn() *Conn {
	return &Conn{
		client:    client,
		requestCh: make(chan request, 64),
		closedCh:  make(chan struct{}),
	}
}

func (conn *Conn) Client() *Client {
	return conn.client
}

func (conn *Conn) RequestAsync(opCode proto.OpCode, req []byte, handler func(Reply)) {
	select {
	case conn.requestCh <- request{opCode, req, handler}:
		if conn.isClosed() {
			conn.drainRequestCh()
		}
	case <-conn.closedCh:
		handler(Reply{Err: proto.ErrConnectionLoss})
	}
}

func (conn *Conn) RequestSync(opCode proto.OpCode, req []byte) Reply {
	ch := make(chan Reply)
	conn.RequestAsync(opCode, req, func(reply Reply) {
		ch <- reply
	})
	return <-ch
}

func (conn *Conn) drainRequestCh() {
	for {
		select {
		case request := <-conn.requestCh:
			request.handler(Reply{Err: proto.ErrConnectionLoss})
		default:
			return
		}
	}
}

func (client *Client) runConnect() {
	first := true
reconnect:
	for !client.closed() {
		client.connMutex.Lock()
		if first {
			first = false
		} else {
			client.conn = client.makeConn()
		}
		conn := client.conn
		client.connMutex.Unlock()

		err := conn.connect()
		if err != nil {
			conn.close(err)
			continue reconnect
		}
		err = conn.handshake()
		if err != nil {
			conn.close(err)
			continue reconnect
		}
		go conn.runReceiver()
		go conn.runPings()
		xid := proto.Xid(1)
		for _, request := range client.restoreWatches(conn) {
			err := conn.sendRequest(xid, request)
			if err != nil {
				conn.close(err)
				continue reconnect
			}
			xid++
		}
		for {
			select {
			case request := <-conn.requestCh:
				err := conn.sendRequest(xid, request)
				if err != nil {
					conn.close(err)
					continue reconnect
				}
				xid++
			case <-conn.closedCh:
				continue reconnect
			case <-client.shutdownCh:
				conn.close(proto.ErrClosing.Error())
				return
			}
		}
	}
}

func (client *Client) getNextAddress() string {
	client.serversMutex.Lock()
	defer client.serversMutex.Unlock()
	i := client.connectionAttempts % uint64(len(client.knownServers))
	client.connectionAttempts++
	return client.knownServers[i]
}

func (conn *Conn) connect() error {
	address := conn.client.getNextAddress()
	log.Printf("Connecting to %v", address)
	netConn, err := net.Dial("tcp", address)
	if err != nil {
		log.Printf("Error connecting to %v, trying next server", err)
		// TODO: backoff
		return err
	}
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.closed {
		netConn.Close()
		return proto.ErrClosing.Error()
	} else {
		conn.netConn = netConn
		return nil
	}
}

// Thread-safe.
func (conn *Conn) isClosed() bool {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	return conn.closed
}

// Thread-safe.
func (conn *Conn) close(err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if !conn.closed {
		if err != proto.ErrClosing.Error() {
			log.Printf("Closing connection due to error: %v", err)
		}
		if conn.netConn != nil {
			conn.netConn.Close()
		}
		for _, h := range conn.outstanding {
			h.handler(Reply{
				Xid: h.xid,
				Err: proto.ErrConnectionLoss,
			})
		}
		conn.outstanding = nil
		conn.closed = true
		close(conn.closedCh)
	}
}

func (conn *Conn) handshake() error {
	conn.client.sessionMutex.Lock()
	req := proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    conn.client.lastZxidSeen,
		Timeout:         int32(conn.client.options.SessionTimeout / time.Millisecond),
		SessionID:       conn.client.sessionId,
		Passwd:          conn.client.sessionPassword,
	}
	conn.client.sessionMutex.Unlock()
	buf, err := jute.Encode(&req)
	if err != nil {
		return err
	}
	err = intframe.Send(conn.netConn, buf)
	if err != nil {
		return err
	}
	respBuf, err := intframe.Receive(conn.netConn)
	if err != nil {
		return err
	}
	var resp proto.ConnectResponse
	err = jute.Decode(respBuf, &resp)
	if err != nil {
		return err
	}
	if resp.ProtocolVersion != 0 {
		return fmt.Errorf("Server sent unknown Protocol Version (%v) in ConnectResponse",
			resp.ProtocolVersion)
	}
	conn.sessionTimeout = time.Millisecond * time.Duration(resp.Timeout)
	conn.client.sessionMutex.Lock()
	conn.client.sessionId = resp.SessionID
	conn.client.sessionPassword = resp.Passwd
	conn.client.sessionMutex.Unlock()
	return nil
}

func (conn *Conn) sendRequest(xid proto.Xid, request request) error {
	now := time.Now()
	conn.mutex.Lock()
	conn.outstanding = append(conn.outstanding, outstanding{xid, now, request.handler})
	conn.mutex.Unlock()
	header := proto.RequestHeader{
		Xid:    xid,
		OpCode: request.opCode,
	}
	headerBuf, err := jute.Encode(&header)
	if err != nil {
		return err
	}
	packet := append(headerBuf, request.buf...)
	err = intframe.Send(conn.netConn, packet)
	return err
}

func (conn *Conn) runPings() {
	pingInterval := conn.sessionTimeout / 3

	header := proto.RequestHeader{
		Xid:    proto.XidPing,
		OpCode: proto.OpPing,
	}
	buf, err := jute.Encode(&header)
	if err != nil {
		log.Fatalf("Couldn't encode ping header: %v", err)
	}

	timer := time.NewTimer(pingInterval)
	for {
		select {
		case now := <-timer.C:
			conn.mutex.Lock()
			nextPing := conn.lastHeartbeat.Add(pingInterval)
			pingDue := now.After(nextPing)
			if pingDue {
				conn.lastPingStart = now
			}
			conn.mutex.Unlock()
			if pingDue {
				err = intframe.Send(conn.netConn, buf)
				if err != nil {
					conn.close(err)
					return
				}
				timer.Reset(pingInterval)
			} else {
				timer.Reset(nextPing.Sub(now))
			}
		case <-conn.closedCh:
			return
		}
	}
}

func (conn *Conn) pong() {
	conn.mutex.Lock()
	if conn.lastPingStart.After(conn.lastHeartbeat) {
		conn.lastHeartbeat = conn.lastPingStart
	}
	conn.mutex.Unlock()
}

func (conn *Conn) runReceiver() {
	for {
		err := conn.receive()
		if err != nil {
			conn.close(err)
			break
		}
	}
}

func (conn *Conn) receive() error {
	msg, err := intframe.Receive(conn.netConn)
	if err != nil {
		return err
	}

	var header proto.ResponseHeader
	more, err := jute.DecodeSome(msg, &header)
	if err != nil {
		return fmt.Errorf("error reading response header: %v", err)
	}

	switch header.Xid {
	case proto.XidWatcherEvent:
		var resp proto.WatcherEvent
		err = jute.Decode(more, &resp)
		if err != nil {
			return err
		}
		conn.client.triggerWatch(resp)
		return nil

	case proto.XidPing:
		if header.Err != proto.ErrOk {
			return header.Err.Error()
		}
		conn.pong()
		return nil
	}

	if header.Zxid > 0 {
		conn.client.sessionMutex.Lock()
		if header.Zxid > conn.client.lastZxidSeen {
			conn.client.lastZxidSeen = header.Zxid
		}
		conn.client.sessionMutex.Unlock()
	}

	reply := Reply{
		Xid:  header.Xid,
		Zxid: header.Zxid,
		Buf:  more,
		Err:  header.Err,
	}

	handler, ok := conn.getHandler(header.Xid)
	if !ok {
		return fmt.Errorf("Error: handler for Xid %v not found", header.Xid)
	}
	handler(reply)
	return nil
}

func (conn *Conn) getHandler(xid proto.Xid) (func(Reply), bool) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if len(conn.outstanding) == 0 {
		return nil, false
	}
	first := conn.outstanding[0]
	if first.xid != xid {
		return nil, false
	}
	conn.outstanding = conn.outstanding[1:]
	if first.sent.After(conn.lastHeartbeat) {
		conn.lastHeartbeat = first.sent
	}
	return first.handler, true
}

func (client *Client) triggerWatch(event proto.WatcherEvent) {
	client.watchesMutex.Lock()
	defer client.watchesMutex.Unlock()
	list := client.watches[event.Path]
	for i, watch := range list {
		for _, e := range watch.events {
			if e == event.Type {
				if len(list) == 1 {
					delete(client.watches, event.Path)
				} else {
					client.watches[event.Path] = append(list[:i-1], list[i+1:]...)
				}
				watch.watcher(event.Type)
				return
			}
		}
	}
	log.Printf("Warning: received unexpected notification from server: %+v", event)
}

func (client *Client) restoreWatches(conn *Conn) []request {
	conn.client.sessionMutex.Lock()
	req := proto.SetWatchesRequest{
		RelativeZxid: client.lastZxidSeen,
	}
	conn.client.sessionMutex.Unlock()

	client.watchesMutex.Lock()
	for path, watches := range client.watches {
		for _, watch := range watches {
			for _, event := range watch.events {
				switch event {
				case proto.EventNodeCreated:
					fallthrough
				case proto.EventNodeDeleted:
					req.ExistWatches = append(req.ExistWatches, path)
				case proto.EventNodeDataChanged:
					req.DataWatches = append(req.DataWatches, path)
				case proto.EventNodeChildrenChanged:
					req.ChildWatches = append(req.ChildWatches, path)
				default:
					log.Printf("Warning: cannot restore watch for unknown event type %v", event)
				}
			}
		}
	}
	client.watchesMutex.Unlock()

	if len(req.ExistWatches) == 0 &&
		len(req.DataWatches) == 0 &&
		len(req.ChildWatches) == 0 {
		return nil
	}
	log.Printf("Restoring watches: %+v", req)
	// TODO: if req is too big, split into multiple reqeusts
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		conn.close(err)
		return nil
	}
	return []request{{
		opCode: proto.OpSetWatches,
		buf:    reqBuf,
		handler: func(reply Reply) {
			err := reply.Err.Error()
			if err != nil {
				conn.close(err)
			}
		},
	}}
}
