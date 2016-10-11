/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"

	"salesforce.com/zoolater/intframe"
	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
	"salesforce.com/zoolater/statemachine"
	"salesforce.com/zoolater/x500"
)

type InfiniteQueue struct {
	mutex  sync.Mutex
	queue  [][]byte // note: nil value means close connection
	signal chan struct{}
}

func NewInfiniteQueue() *InfiniteQueue {
	return &InfiniteQueue{
		signal: make(chan struct{}),
	}
}

func (q *InfiniteQueue) Push(msg []byte) {
	q.mutex.Lock()
	q.queue = append(q.queue, msg)
	q.mutex.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *InfiniteQueue) Pop() ([]byte, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if len(q.queue) > 0 {
		first := q.queue[0]
		q.queue = q.queue[1:]
		return first, true
	}
	return nil, false
}

type ConnectRPC struct {
	conn     statemachine.Connection
	reqJute  []byte
	req      *proto.ConnectRequest
	errReply func(proto.ErrCode)
	reply    func(*proto.ConnectResponse, statemachine.ConnectionId)
}

type RPC struct {
	conn           statemachine.Connection
	cmdId          statemachine.CommandId
	lastCmdId      statemachine.CommandId
	reqHeaderJute  []byte
	reqHeader      proto.RequestHeader
	opName         string
	req            []byte
	errReply       func(proto.ZXID, proto.ErrCode)
	reply          func(proto.ZXID, []byte)
	replyThenClose func(proto.ZXID, []byte)
}

type ZKCPServer struct {
	handler func(interface{})
}

type ZKCPConnection struct {
	server     *ZKCPServer
	netConn    net.Conn
	sendQueue  *InfiniteQueue
	closeCh    chan struct{}
	sessionId  proto.SessionId
	connId     statemachine.ConnectionId
	lastCmdId  statemachine.CommandId
	identities []proto.Identity // world:anyone is assumed and not stored explicitly
}

func (conn *ZKCPConnection) String() string {
	return fmt.Sprintf("ZKCP connection %v on session %v",
		conn.connId, conn.sessionId)
}

func (conn *ZKCPConnection) SessionId() proto.SessionId {
	return conn.sessionId
}

func (conn *ZKCPConnection) ConnId() statemachine.ConnectionId {
	return conn.connId
}

func (conn *ZKCPConnection) Identity() []proto.Identity {
	return conn.identities
}

func (conn *ZKCPConnection) Notify(zxid proto.ZXID, event statemachine.TreeEvent) {
	respHeader := proto.ResponseHeader{
		Xid:  proto.XidWatcherEvent,
		Zxid: zxid,
		Err:  proto.ErrOk,
	}
	msg := proto.WatcherEvent{
		Type:  event.Which,
		State: proto.StateConnected,
		Path:  event.Path,
	}
	log.Printf("Queuing watch notification %+v %+v", respHeader, msg)
	headerBuf, err := conn.Encode(&respHeader)
	if err != nil {
		log.Printf("Error encoding watch header: %v", err)
		return
	}
	msgBuf, err := conn.Encode(&msg)
	if err != nil {
		log.Printf("Error encoding watch: %v", err)
		return
	}
	conn.sendQueue.Push(append(headerBuf, msgBuf...))
}

func (conn *ZKCPConnection) handshake() {
	log.Printf("New connection with identity %v", conn.identities)

	// Receive connection request from the client
	req, err := intframe.Receive(conn.netConn)
	if err != nil {
		log.Printf("Error receiving connection request (%v), closing connection", err)
		conn.Close()
		return
	}

	// Start to process it
	err = conn.processConnReq(req)
	if err != nil {
		log.Printf("connection encountered error: %v", err)
		conn.Close()
		return
	}

	// Wait for the response
	msg := conn.toSend()
	if msg == nil {
		return
	}

	// Send it back to the client
	err = intframe.Send(conn.netConn, msg)
	if err != nil {
		log.Printf("Error sending connection response (%v), closing connection", err)
		conn.Close()
		return
	}

	// Start up the send and receive routines
	log.Printf("Done setting up new connection")
	go conn.sendLoop()
	go conn.receiveLoop()
}

func (conn *ZKCPConnection) Encode(msg interface{}) ([]byte, error) {
	buf, err := jute.Encode(msg)
	if err != nil {
		conn.Close()
	}
	return buf, err
}

func (conn *ZKCPConnection) DecodeSome(buf []byte, msg interface{}) ([]byte, error) {
	more, err := jute.DecodeSome(buf, msg)
	if err != nil {
		conn.Close()
	}
	return more, err
}

func (conn *ZKCPConnection) Decode(buf []byte, msg interface{}) error {
	err := jute.Decode(buf, msg)
	if err != nil {
		conn.Close()
	}
	return err
}

// It's safe to call close() more than once. To make this work, we can't simply
// close(closeCh).
func (conn *ZKCPConnection) Close() {
	select {
	case conn.closeCh <- struct{}{}:
	default:
	}
	err := conn.netConn.Close()
	if err != nil {
		log.Printf("Closing connection returned error %v", err)
	}
}

func (conn *ZKCPConnection) receiveLoop() {
	for {
		req, err := intframe.Receive(conn.netConn)
		if err != nil {
			log.Printf("Error receiving message (%v), closing connection", err)
			conn.Close()
			return
		}
		err = conn.process(req)
		if err != nil {
			log.Printf("connection encountered error: %v", err)
			conn.Close()
			return
		}
	}
}

func (conn *ZKCPConnection) toSend() []byte {
	for {
		msg, ok := conn.sendQueue.Pop()
		if ok {
			if msg == nil {
				conn.Close()
				return nil
			}
			return msg
		}
		select {
		case <-conn.sendQueue.signal:
		case <-conn.closeCh:
			conn.closeCh <- struct{}{}
			return nil
		}
	}
}

func (conn *ZKCPConnection) sendLoop() {
	for {
		msg := conn.toSend()
		if msg == nil {
			return
		}
		err := intframe.Send(conn.netConn, msg)
		if err != nil {
			log.Printf("Error sending message (%v), closing connection", err)
			conn.Close()
			return
		}
	}
}

func (s *ZKCPServer) listen(addr string, handler func(net.Conn)) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Error listening on %v: %v", addr, err)
	}
	go func() {
		for {
			netConn, err := listener.Accept()
			if err == nil {
				go handler(netConn)
			} else {
				log.Printf("Error from Accept, ignoring: %v", err)
			}
		}
	}()
	return nil
}

func (s *ZKCPServer) ListenCleartext(addr string) error {
	return s.listen(addr, func(netConn net.Conn) {
		conn := s.newConnection(netConn)
		conn.handshake()
	})
}

func (s *ZKCPServer) ListenTLS(addr string, config *tls.Config) error {
	return s.listen(addr, func(netConn net.Conn) {
		tlsConn := tls.Server(netConn, config)
		err := tlsConn.Handshake()
		if err != nil {
			log.Printf("Error in TLS handshake, closing: %v", err)
			tlsConn.Close()
		}
		conn := s.newConnection(tlsConn)
		tlsState := tlsConn.ConnectionState()
		for _, cert := range tlsState.PeerCertificates {
			id, err := x500.Principal(&cert.Subject)
			if err != nil {
				log.Printf("Warning: %v. Skipping", err)
				continue
			}
			conn.identities = append(conn.identities, proto.Identity{
				Scheme: "x509",
				ID:     id,
			})
		}
		conn.handshake()
	})
}

func (s *ZKCPServer) newConnection(netConn net.Conn) *ZKCPConnection {
	conn := &ZKCPConnection{
		server:    s,
		netConn:   netConn,
		sendQueue: NewInfiniteQueue(),
		closeCh:   make(chan struct{}, 1),
	}

	host, _, err := net.SplitHostPort(netConn.RemoteAddr().String())
	if err == nil {
		conn.identities = append(conn.identities, proto.Identity{
			Scheme: "ip",
			ID:     host,
		})
	} else {
		log.Printf("Could not parse IP address from connection from %v: %v",
			netConn.RemoteAddr().String(), err)
	}
	return conn
}

func (conn *ZKCPConnection) processConnReq(reqBuf []byte) error {
	req := &proto.ConnectRequest{}
	more, err := jute.DecodeSome(reqBuf, req)
	if err != nil {
		return fmt.Errorf("error reading ConnectRequest: %v", err)
	}
	sendReadOnlyByte := false
	if len(more) == 1 {
		// Modern ZK clients send 1 more byte to indicate whether they support
		// read-only mode. We ignore it but send a 0 byte back.
		sendReadOnlyByte = true
	} else if len(more) > 1 {
		return fmt.Errorf("unexpected bytes after ConnectRequest: %#v", more)
	}
	log.Printf("connection request: %#v", req)

	reply := func(resp *proto.ConnectResponse, connId statemachine.ConnectionId) {
		log.Printf("Replying to connection request with %#v", resp)
		buf, err := conn.Encode(resp)
		if err != nil {
			log.Printf("Error serializing ConnectResponse: %v", err)
			return
		}
		if sendReadOnlyByte {
			buf = append(buf, 0)
		}
		conn.sessionId = resp.SessionID
		conn.connId = connId
		conn.lastCmdId = 1
		conn.sendQueue.Push(buf)
	}
	conn.server.handler(&ConnectRPC{
		conn:    conn,
		req:     req,
		reqJute: reqBuf[:len(reqBuf)-len(more)],
		errReply: func(errCode proto.ErrCode) {
			log.Printf("Can't satisfy connection request (%v), dropping", errCode.Error())
			// There's no place to send back the error code. ZooKeeper seems to send
			// back all zeros instead to indicate session expired.
			if errCode == proto.ErrSessionExpired {
				reply(&proto.ConnectResponse{}, 0)
				conn.sendQueue.Push(nil)
			} else {
				conn.Close()
			}
		},
		reply: reply,
	})
	return nil
}

func (conn *ZKCPConnection) process(msg []byte) error {
	rpc := &RPC{
		conn: conn,
	}

	more, err := conn.DecodeSome(msg, &rpc.reqHeader)
	if err != nil {
		return fmt.Errorf("error reading request header: %v", err)
	}
	rpc.reqHeaderJute = msg[:len(msg)-len(more)]
	rpc.req = more

	if name, ok := proto.OpNames[rpc.reqHeader.OpCode]; ok {
		rpc.opName = name
	} else {
		rpc.opName = fmt.Sprintf("unknown (%v)", rpc.reqHeader.OpCode)
	}

	log.Printf("Received %v", rpc.opName)
	rpc.lastCmdId = conn.lastCmdId
	if isReadOnly(rpc.reqHeader.OpCode) {
		rpc.cmdId = 0
	} else {
		conn.lastCmdId++
		rpc.cmdId = conn.lastCmdId
	}

	rpc.errReply = func(zxid proto.ZXID, errCode proto.ErrCode) {
		respHeader := proto.ResponseHeader{
			Xid:  rpc.reqHeader.Xid,
			Zxid: zxid,
			Err:  errCode,
		}
		log.Printf("Replying with error header to %v: %+v", rpc.opName, respHeader)
		buf, err := conn.Encode(&respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", rpc.opName, err)
			return
		}
		conn.sendQueue.Push(buf)
		if errCode == proto.ErrSessionExpired {
			conn.sendQueue.Push(nil)
		}
	}

	rpc.reply = func(zxid proto.ZXID, msgBuf []byte) {
		respHeader := proto.ResponseHeader{
			Xid:  rpc.reqHeader.Xid,
			Zxid: zxid,
			Err:  proto.ErrOk,
		}
		headerBuf, err := conn.Encode(&respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", rpc.opName, err)
			return
		}
		conn.sendQueue.Push(append(headerBuf, msgBuf...))
	}

	rpc.replyThenClose = func(zxid proto.ZXID, msgBuf []byte) {
		rpc.reply(zxid, msgBuf)
		conn.sendQueue.Push(nil)
	}

	conn.server.handler(rpc)
	return nil
}
