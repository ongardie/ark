/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"salesforce.com/zoolater/intframe"
	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
)

var listenAddr string
var serverAddr string

var mutex sync.Mutex
var connCount uint64

type proxyConn struct {
	id       uint64
	requests uint64
	replies  uint64
	client   net.Conn
	server   net.Conn
	opsMutex sync.Mutex
	ops      map[int32]proto.OpCode
}

func opName(opCode proto.OpCode) string {
	name, ok := proto.OpNames[opCode]
	if ok {
		return name
	}
	return fmt.Sprintf("unknown (%v)", opCode)
}

func decodeRequest(count uint64, msg []byte, withHeader func(proto.RequestHeader)) string {
	if count == 0 {
		req := &proto.ConnectRequest{}
		more, err := jute.DecodeSome(msg, req)
		if err != nil {
			return err.Error()
		}
		var extra string
		if len(more) == 1 {
			// Modern ZK clients send 1 more byte to indicate whether they support
			// read-only mode.
			if more[0] == 1 {
				extra = " [ro]"
			} else if more[0] == 0 {
				extra = " [!ro]"
			} else {
				extra = " [?]"
			}
		} else if len(more) > 1 {
			extra = " [?+]"
		}
		return fmt.Sprintf("connect request %+v%v", req, extra)
	}

	var reqHeader proto.RequestHeader
	more, err := jute.DecodeSome(msg, &reqHeader)
	if err != nil {
		return fmt.Sprintf("error reading request header: %v", err)
	}
	withHeader(reqHeader)

	req := proto.RequestStructForOp(reqHeader.OpCode)
	if req == nil {
		return fmt.Sprintf("%+v %v (unknown struct)",
			reqHeader, opName(reqHeader.OpCode))
	}
	err = jute.Decode(more, req)
	if err != nil {
		return fmt.Sprintf("%+v %v with decode error: %v",
			reqHeader, opName(reqHeader.OpCode), err)
	}
	return fmt.Sprintf("%+v %v request %+v", reqHeader, opName(reqHeader.OpCode), req)
}

func decodeReply(count uint64, msg []byte, getOpCode func(xid int32) (proto.OpCode, bool)) string {
	if count == 0 {
		req := &proto.ConnectResponse{}
		more, err := jute.DecodeSome(msg, req)
		if err != nil {
			return err.Error()
		}
		var extra string
		if len(more) == 1 {
			// Servers echo back one more byte for read-only mode.
			if more[0] == 1 {
				extra = " [ro]"
			} else if more[0] == 0 {
				extra = " [!ro]"
			} else {
				extra = " [?]"
			}
		} else if len(more) > 1 {
			extra = " [?+]"
		}
		return fmt.Sprintf("connect response %+v%v", req, extra)
	}

	var respHeader proto.ResponseHeader
	more, err := jute.DecodeSome(msg, &respHeader)
	if err != nil {
		return fmt.Sprintf("error reading response header: %v", err)
	}

	var resp interface{}
	var name string
	switch respHeader.Xid {
	case proto.XidWatcherEvent:
		resp = &proto.WatcherEvent{}
		name = "watcher event"
	case proto.XidPing:
		resp = &proto.PingResponse{}
		name = "ping response"
	default:
		opCode, ok := getOpCode(respHeader.Xid)
		if !ok {
			return fmt.Sprintf("%+v %v", respHeader, more)
		}
		name = fmt.Sprintf("%s response", opName(opCode))
		resp = proto.ResponseStructForOp(opCode)
		if resp == nil {
			return fmt.Sprintf("%+v %v (unknown struct)",
				respHeader, name)
		}
	}

	if respHeader.Err != proto.ErrOk {
		return fmt.Sprintf("%+v %v %v", respHeader, name, respHeader.Err.Error())
	}

	err = jute.Decode(more, resp)
	if err != nil {
		return fmt.Sprintf("%+v %v with decode error: %v",
			respHeader, name, err)
	}

	return fmt.Sprintf("%+v %v %+v", respHeader, name, resp)
}

func (conn *proxyConn) requestLoop() {
	for {
		msg, err := intframe.Receive(conn.client)
		if err != nil {
			log.Printf("[conn %v] Could not receive from client: %v. Closing connections",
				conn.id, err)
			conn.client.Close()
			conn.server.Close()
			return
		}
		str := decodeRequest(conn.requests, msg, func(hdr proto.RequestHeader) {
			if hdr.Xid >= 0 {
				conn.opsMutex.Lock()
				conn.ops[hdr.Xid] = hdr.OpCode
				conn.opsMutex.Unlock()
			}
		})
		log.Printf("[conn %v] client sent %v", conn.id, str)
		conn.requests++
		err = intframe.Send(conn.server, msg)
		if err != nil {
			log.Printf("[conn %v] Could not send to server: %v. Closing connections",
				conn.id, err)
			conn.server.Close()
			conn.client.Close()
			return
		}
	}
}

func (conn *proxyConn) replyLoop() {
	for {
		msg, err := intframe.Receive(conn.server)
		if err != nil {
			log.Printf("[conn %v] Could not receive from server: %v. Closing connections",
				conn.id, err)
			conn.server.Close()
			conn.client.Close()
			return
		}
		str := decodeReply(conn.replies, msg, func(xid int32) (proto.OpCode, bool) {
			conn.opsMutex.Lock()
			opCode, ok := conn.ops[xid]
			if ok {
				delete(conn.ops, xid)
			}
			conn.opsMutex.Unlock()
			return opCode, ok
		})
		log.Printf("[conn %v] server sent %v",
			conn.id, str)
		conn.replies++
		err = intframe.Send(conn.client, msg)
		if err != nil {
			log.Printf("[conn %v] Could not send to client: %v. Closing connections",
				conn.id, err)
			conn.client.Close()
			conn.server.Close()
			return
		}
	}
}

func handle(client net.Conn) {
	server, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("Could not connect to server: %v. Closing client connection", err)
		client.Close()
		return
	}
	mutex.Lock()
	connCount++
	conn := proxyConn{
		id:     connCount,
		client: client,
		server: server,
		ops:    make(map[int32]proto.OpCode),
	}
	mutex.Unlock()
	go conn.requestLoop()
	go conn.replyLoop()
}

func listen() error {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("Error listening on %v: %v", listenAddr, err)
	}
	for {
		conn, err := listener.Accept()
		if err == nil {
			go handle(conn)
		} else {
			log.Printf("Error from Accept, ignoring: %v", err)
		}
	}
}

func main() {
	listenAddr = os.Args[1]
	serverAddr = os.Args[2]
	err := listen()
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}
