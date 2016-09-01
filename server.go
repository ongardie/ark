/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	cryptoRand "crypto/rand"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Consensus struct {
	mutex        sync.Mutex
	zxid         ZXID
	stateMachine *StateMachine
}

var consensus = Consensus{
	stateMachine: NewStateMachine(),
}

func getContext(conn *Connection) *Context {
	consensus.zxid++
	ctx := &Context{
		zxid:      consensus.zxid,
		time:      time.Now().Unix(),
		sessionId: conn.sessionId,
	}
	ctx.rand = make([]byte, SessionPasswordLen)
	_, err := cryptoRand.Read(ctx.rand)
	if err != nil {
		log.Fatalf("Could not get random bytes: %v", err)
	}
	return ctx
}

func processConnect(rpc *ConnectRPC) {
	resp := &connectResponse{
		ProtocolVersion: 12, // TODO: set this like ZooKeeper does
		TimeOut:         rpc.req.TimeOut,
	}
	if rpc.req.SessionID == 0 {
		resp.SessionID, resp.Passwd = consensus.stateMachine.createSession(getContext(rpc.conn))
	} else {
		resp.SessionID = rpc.req.SessionID
		resp.Passwd = rpc.req.Passwd
	}
	err := consensus.stateMachine.setConn(resp.SessionID, resp.Passwd, rpc.conn)
	if err != errOk {
		rpc.errReply(err)
		return
	}
	rpc.conn.sessionId = resp.SessionID
	rpc.reply(resp)
}

func handler(rpcChan <-chan RPC) {
	for {
		rpc := <-rpcChan
		consensus.mutex.Lock()
		switch rpc := rpc.(type) {
		case *ConnectRPC:
			processConnect(rpc)

		case *PingRPC:
			log.Printf("Sending pong")
			rpc.reply()

		case *CreateRPC:
			resp, errCode := consensus.stateMachine.Create(getContext(rpc.conn), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		case *GetChildrenRPC:
			resp, errCode := consensus.stateMachine.GetChildren(getContext(rpc.conn), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		case *GetDataRPC:
			resp, errCode := consensus.stateMachine.GetData(getContext(rpc.conn), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		case *SetDataRPC:
			resp, errCode := consensus.stateMachine.SetData(getContext(rpc.conn), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		default:
			log.Printf("Unimplemended RPC: %T", rpc)
			rpc.errReply(errUnimplemented)
		}
		consensus.mutex.Unlock()
	}
}

func serve() error {
	rpcChan := make(chan RPC)

	log.Print("listening for ZooKeeper clients on port 2181")
	juteServer := NewJuteServer(rpcChan)
	err := juteServer.Listen(":2181")
	if err != nil {
		return fmt.Errorf("error listening: %v", err)
	}

	for i := 0; i < 32; i++ {
		go handler(rpcChan)
	}
	select {} // block forever
}

func main() {
	err := serve()
	if err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}
