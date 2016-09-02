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

type Received struct {
	conn      *JuteConnection
	msg       []byte
	isConnReq bool
}

type JuteConnection struct {
	netConn   net.Conn
	received  chan<- Received
	sendQueue *InfiniteQueue
	closeCh   chan struct{}
	sessionId proto.SessionId
}

func (conn *JuteConnection) String() string {
	return fmt.Sprintf("Jute connection with session ID %v", conn.sessionId)
}

func (conn *JuteConnection) SessionId() proto.SessionId {
	return conn.sessionId
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
	req, err := conn.receive()
	if err != nil {
		log.Printf("Error receiving connection request (%v), closing connection", err)
		conn.Close()
		return
	}

	// Queue it for processing
	select {
	case conn.received <- Received{conn: conn, msg: req, isConnReq: true}:
	case <-conn.closeCh:
		conn.closeCh <- struct{}{}
		return
	}

	// Wait for the response
	msg := conn.toSend()
	if msg == nil {
		return
	}

	// Send it back to the client
	err = conn.send(msg)
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

func (conn *JuteConnection) receive() ([]byte, error) {
	log.Printf("Waiting for incoming message")
	buf := make([]byte, 4)
	n, err := io.ReadFull(conn.netConn, buf)
	if err != nil {
		return nil, err
	}
	bytes := binary.BigEndian.Uint32(buf[:n])
	log.Printf("Expecting %v bytes", bytes)
	buf = make([]byte, bytes)
	_, err = io.ReadFull(conn.netConn, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (conn *JuteConnection) receiveLoop() {
	for {
		req, err := conn.receive()
		if err != nil {
			log.Printf("Error receiving message (%v), closing connection", err)
			conn.Close()
			return
		}
		select {
		case conn.received <- Received{conn: conn, msg: req, isConnReq: false}:
		case <-conn.closeCh:
			conn.closeCh <- struct{}{}
			return
		}
	}
}

func (conn *JuteConnection) send(msg []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	_, err := conn.netConn.Write(buf)
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
		err := conn.send(msg)
		if err != nil {
			log.Printf("Error sending message (%v), closing connection", err)
			conn.Close()
			return
		}
	}
}

type JuteServer struct {
	received chan Received
	rpcChan  chan<- RPCish
}

func NewJuteServer(rpcChan chan<- RPCish) *JuteServer {
	s := &JuteServer{
		received: make(chan Received),
		rpcChan:  rpcChan,
	}
	go s.receive()
	return s
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
		netConn:   netConn,
		received:  s.received,
		sendQueue: NewInfiniteQueue(),
		closeCh:   make(chan struct{}, 1),
	}
	go conn.handshake()
}

func (s *JuteServer) processConnReq(received Received) error {
	req := &proto.ConnectRequest{}
	buf, err := jute.DecodeSome(received.msg, req)
	if err != nil {
		return fmt.Errorf("error reading ConnectRequest: %v", err)
	}
	sendReadOnlyByte := false
	if len(buf) == 1 {
		// Modern ZK clients send 1 more byte to indicate whether they support
		// read-only mode. We ignore it but send a 0 byte back.
		sendReadOnlyByte = true
	} else if len(buf) > 1 {
		return fmt.Errorf("unexpected bytes after ConnectRequest: %#v", buf)
	}
	log.Printf("connection request: %#v", req)
	s.rpcChan <- &ConnectRPC{
		conn:    received.conn,
		req:     req,
		reqJute: received.msg[:len(received.msg)-len(buf)],
		errReply: func(errCode proto.ErrCode) {
			// TODO: what am I supposed to do with this?
			log.Printf("Can't satisfy connection request (%v), dropping", errCode.Error())
			received.conn.Close()
		},
		reply: func(resp *proto.ConnectResponse) {
			log.Printf("Replying to connection request with %#v", resp)
			buf, err = received.conn.Encode(resp)
			if err != nil {
				log.Printf("Error serializing ConnectResponse: %v", err)
				return
			}
			if sendReadOnlyByte {
				buf = append(buf, 0)
			}
			received.conn.sessionId = resp.SessionID
			received.conn.sendQueue.Push(buf)
		},
	}
	return nil
}

func (s *JuteServer) process(received Received) error {
	if received.isConnReq {
		return s.processConnReq(received)
	}
	rpc := &RPC{
		conn:      received.conn,
		sessionId: received.conn.sessionId,
	}

	more, err := received.conn.DecodeSome(received.msg, &rpc.reqHeader)
	if err != nil {
		return fmt.Errorf("error reading request header: %v", err)
	}
	rpc.reqHeaderJute = received.msg[:len(received.msg)-len(more)]
	rpc.req = more

	if name, ok := proto.OpNames[rpc.reqHeader.OpCode]; ok {
		rpc.opName = name
	} else {
		rpc.opName = fmt.Sprintf("unknown (%v)", rpc.reqHeader.OpCode)
	}

	respHeader := proto.ResponseHeader{
		Xid:  rpc.reqHeader.Xid,
		Zxid: 0, // usually overridden during reply
		Err:  proto.ErrOk,
	}

	rpc.errReply = func(errCode proto.ErrCode) {
		respHeader.Err = errCode
		log.Printf("Replying with error header to %v: %+v", rpc.opName, respHeader)
		buf, err := received.conn.Encode(&respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", rpc.opName, err)
			return
		}
		received.conn.sendQueue.Push(buf)
	}

	rpc.reply = func(zxid proto.ZXID, msgBuf []byte) {
		headerBuf, err := received.conn.Encode(&respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", rpc.opName, err)
			return
		}
		received.conn.sendQueue.Push(append(headerBuf, msgBuf...))
	}

	s.rpcChan <- rpc
	return nil
}

func (s *JuteServer) receive() {
	for {
		received := <-s.received
		err := s.process(received)
		if err != nil {
			log.Printf("connection encountered error: %v", err)
			received.conn.Close()
		}
	}
}
