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

	requestCh chan request
}

type conn struct {
	client         *Client
	netConn        net.Conn
	sessionTimeout time.Duration

	mutex    sync.Mutex
	closed   bool
	handlers []registeredHandler
}

type registeredHandler struct {
	xid     proto.Xid
	handler func(Reply)
}

type request struct {
	opCode  proto.OpCode
	buf     []byte
	watcher *Watcher
	handler func(Reply)
}

type Watcher struct {
	Events  []proto.EventType
	Handler func(proto.EventType, proto.Path)
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
		requestCh:       make(chan request, 1024),
		sessionId:       opt.SessionId,
		sessionPassword: opt.SessionPassword,
		shutdownCh:      make(chan struct{}),
	}
	go client.runSender()
	return client
}

func (client *Client) Close() {
	client.shutdownMutex.Lock()
	defer client.shutdownMutex.Unlock()
	if !client.shutdown {
		client.shutdown = true
		close(client.shutdownCh)
	}
	client.drainRequestCh()
}

func (client *Client) Session() (proto.SessionId, proto.SessionPassword) {
	client.sessionMutex.Lock()
	defer client.sessionMutex.Unlock()
	return client.sessionId, client.sessionPassword
}

func (client *Client) Request(
	opCode proto.OpCode,
	req []byte,
	watcher *Watcher,
	handler func(Reply)) {
	select {
	case client.requestCh <- request{opCode, req, watcher, handler}:
		if client.closed() {
			client.drainRequestCh()
		}
	case <-client.shutdownCh:
		handler(Reply{Err: proto.ErrClosing})
	}
}

func (client *Client) SyncRequest(
	opCode proto.OpCode,
	req []byte,
	watcher *Watcher) Reply {
	ch := make(chan Reply)
	client.Request(opCode, req, watcher, func(reply Reply) {
		ch <- reply
	})
	return <-ch
}

func (client *Client) closed() bool {
	select {
	case <-client.shutdownCh:
		return true
	default:
		return false
	}
}

func (client *Client) drainRequestCh() {
	for {
		select {
		case request := <-client.requestCh:
			request.handler(Reply{Err: proto.ErrClosing})
		default:
			return
		}
	}
}

func (client *Client) runSender() {
reconnect:
	for {
		conn, err := client.connect()
		if err != nil {
			continue reconnect
		}
		err = conn.handshake()
		if err != nil {
			conn.close(err)
			continue reconnect
		}
		go conn.runReceiver()
		xid := proto.Xid(1)
		for {
			select {
			case request := <-client.requestCh:
				conn.mutex.Lock()
				conn.handlers = append(conn.handlers, registeredHandler{xid, request.handler})
				conn.mutex.Unlock()
				err := conn.sendRequest(xid, request)
				if err != nil {
					conn.close(err)
					continue reconnect
				}
				xid++
			case <-client.shutdownCh:
				conn.close(proto.ErrClosing.Error())
				return
			}
		}
	}
}

func (client *Client) connect() (*conn, error) {
	client.serversMutex.Lock()
	i := client.connectionAttempts % uint64(len(client.knownServers))
	address := client.knownServers[i]
	client.connectionAttempts++
	client.serversMutex.Unlock()
	netConn, err := net.Dial("tcp", address)
	if err != nil {
		log.Printf("Error connecting to %v, trying next server", err)
		// TODO: backoff
		return nil, err
	}
	return &conn{
		client:  client,
		netConn: netConn,
	}, nil
}

// Thread-safe.
func (conn *conn) close(err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if !conn.closed {
		if err != proto.ErrClosing.Error() {
			log.Printf("Closing connection due to error: %v", err)
		}
		conn.netConn.Close()
		for _, h := range conn.handlers {
			h.handler(Reply{
				Xid: h.xid,
				Err: proto.ErrConnectionLoss,
			})
		}
		conn.closed = true
		conn.handlers = nil
	}
}

func (conn *conn) handshake() error {
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

func (conn *conn) sendRequest(xid proto.Xid, request request) error {
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

func (conn *conn) runReceiver() {
	for {
		err := conn.receive()
		if err != nil {
			conn.close(err)
			break
		}
	}
}

func (conn *conn) receive() error {
	msg, err := intframe.Receive(conn.netConn)
	if err != nil {
		return err
	}

	var header proto.ResponseHeader
	more, err := jute.DecodeSome(msg, &header)
	if err != nil {
		return fmt.Errorf("error reading response header: %v", err)
	}

	if header.Zxid > 0 {
		conn.client.sessionMutex.Lock()
		if header.Zxid > conn.client.lastZxidSeen {
			conn.client.lastZxidSeen = header.Zxid
		}
		conn.client.sessionMutex.Unlock()
	}

	switch header.Xid {
	case proto.XidWatcherEvent:
		var resp proto.WatcherEvent
		err = jute.Decode(more, resp)
		if err != nil {
			return err
		}
	case proto.XidPing:
		return nil
	}

	reply := Reply{
		Xid:  header.Xid,
		Zxid: header.Zxid,
		Buf:  more,
		Err:  header.Err,
	}

	handler, err := conn.getHandler(header.Xid)
	if err != nil {
		return err
	}
	handler(reply)
	return nil
}

func (conn *conn) getHandler(xid proto.Xid) (func(Reply), error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if len(conn.handlers) == 0 {
		return nil, fmt.Errorf("Error: handler for Xid %v not found (empty list)", xid)
	}
	first := conn.handlers[0]
	if first.xid != xid {
		return nil, fmt.Errorf("Error: handler for Xid %v not found (firt Xid is %v)",
			xid, first.xid)
	}
	conn.handlers = conn.handlers[1:]
	return first.handler, nil
}
