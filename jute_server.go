package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
	"salesforce.com/zoolater/statemachine"
)

type InfiniteQueue struct {
	mutex  sync.Mutex
	queue  [][]byte
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

func (q *InfiniteQueue) Pop() []byte {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if len(q.queue) > 0 {
		first := q.queue[0]
		q.queue = q.queue[1:]
		return first
	}
	return nil
}

type JuteServer struct {
	handler func(RPCish)
}

type JuteConnection struct {
	server    *JuteServer
	netConn   net.Conn
	sendQueue *InfiniteQueue
	closeCh   chan struct{}
	sessionId proto.SessionId
	connId    statemachine.ConnectionId
	lastCmdId statemachine.CommandId
}

func (conn *JuteConnection) String() string {
	return fmt.Sprintf("Jute connection %v on session %v",
		conn.connId, conn.sessionId)
}

func (conn *JuteConnection) SessionId() proto.SessionId {
	return conn.sessionId
}

func (conn *JuteConnection) ConnId() statemachine.ConnectionId {
	return conn.connId
}

func (conn *JuteConnection) Notify(zxid proto.ZXID, event statemachine.TreeEvent) {
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

func (conn *JuteConnection) handshake() {
	// Receive connection request from the client
	req, err := receiveFrame(conn.netConn)
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
	err = sendFrame(conn.netConn, msg)
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

func (conn *JuteConnection) Encode(msg interface{}) ([]byte, error) {
	buf, err := jute.Encode(msg)
	if err != nil {
		conn.Close()
	}
	return buf, err
}

func (conn *JuteConnection) DecodeSome(buf []byte, msg interface{}) ([]byte, error) {
	more, err := jute.DecodeSome(buf, msg)
	if err != nil {
		conn.Close()
	}
	return more, err
}

func (conn *JuteConnection) Decode(buf []byte, msg interface{}) error {
	err := jute.Decode(buf, msg)
	if err != nil {
		conn.Close()
	}
	return err
}

// It's safe to call close() more than once. To make this work, we can't simply
// close(closeCh).
func (conn *JuteConnection) Close() {
	select {
	case conn.closeCh <- struct{}{}:
	default:
	}
	err := conn.netConn.Close()
	if err != nil {
		log.Printf("Closing connection returned error %v", err)
	}
}

func receiveFrame(conn net.Conn) ([]byte, error) {
	log.Printf("Waiting for incoming message")
	buf := make([]byte, 4)
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	bytes := binary.BigEndian.Uint32(buf[:n])
	log.Printf("Expecting %v bytes", bytes)
	buf = make([]byte, bytes)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (conn *JuteConnection) receiveLoop() {
	for {
		req, err := receiveFrame(conn.netConn)
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

func sendFrame(conn net.Conn, msg []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	_, err := conn.Write(buf)
	return err
}

func (conn *JuteConnection) toSend() []byte {
	msg := conn.sendQueue.Pop()
	for msg == nil {
		select {
		case <-conn.sendQueue.signal:
		case <-conn.closeCh:
			conn.closeCh <- struct{}{}
			return nil
		}
		msg = conn.sendQueue.Pop()
	}
	return msg
}

func (conn *JuteConnection) sendLoop() {
	for {
		msg := conn.toSend()
		if msg == nil {
			return
		}
		err := sendFrame(conn.netConn, msg)
		if err != nil {
			log.Printf("Error sending message (%v), closing connection", err)
			conn.Close()
			return
		}
	}
}

func (s *JuteServer) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Error listening on %v: %v", addr, err)
	}
	go s.acceptLoop(listener)
	return nil
}

func (s *JuteServer) acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err == nil {
			s.newConnection(conn)
		} else {
			log.Printf("Error from Accept, ignoring: %v", err)
		}
	}
}

func (s *JuteServer) newConnection(netConn net.Conn) {
	conn := &JuteConnection{
		server:    s,
		netConn:   netConn,
		sendQueue: NewInfiniteQueue(),
		closeCh:   make(chan struct{}, 1),
	}
	go conn.handshake()
}

func (conn *JuteConnection) processConnReq(reqBuf []byte) error {
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
	conn.server.handler(&ConnectRPC{
		conn:    conn,
		req:     req,
		reqJute: reqBuf[:len(reqBuf)-len(more)],
		errReply: func(errCode proto.ErrCode) {
			// TODO: what am I supposed to do with this?
			log.Printf("Can't satisfy connection request (%v), dropping", errCode.Error())
			conn.Close()
		},
		reply: func(resp *proto.ConnectResponse, connId statemachine.ConnectionId) {
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
		},
	})
	return nil
}

func (conn *JuteConnection) process(msg []byte) error {
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

	respHeader := proto.ResponseHeader{
		Xid:  rpc.reqHeader.Xid,
		Zxid: 0, // usually overridden during reply
		Err:  proto.ErrOk,
	}

	rpc.errReply = func(errCode proto.ErrCode) {
		respHeader.Err = errCode
		log.Printf("Replying with error header to %v: %+v", rpc.opName, respHeader)
		buf, err := conn.Encode(&respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", rpc.opName, err)
			return
		}
		conn.sendQueue.Push(buf)
	}

	rpc.reply = func(zxid proto.ZXID, msgBuf []byte) {
		headerBuf, err := conn.Encode(&respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", rpc.opName, err)
			return
		}
		conn.sendQueue.Push(append(headerBuf, msgBuf...))
	}

	conn.server.handler(rpc)
	return nil
}
