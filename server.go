/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	cryptoRand "crypto/rand"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type Connection interface {
	Notify(zxid ZXID, event TreeEvent)
}

type Server struct {
	raft struct {
		settings    *raft.Config
		stableStore raft.StableStore
		logStore    raft.LogStore
		snapStore   raft.SnapshotStore
		transport   raft.Transport
		handle      *raft.Raft
	}
}

type Consensus struct {
	mutex        sync.Mutex
	zxid         ZXID
	stateMachine *StateMachine
}

var consensus = Consensus{
	stateMachine: NewStateMachine(),
}

func getContext(sessionId SessionId) *Context {
	consensus.zxid++
	ctx := &Context{
		zxid:      consensus.zxid,
		time:      time.Now().Unix(),
		sessionId: sessionId,
	}
	ctx.rand = make([]byte, SessionPasswordLen)
	_, err := cryptoRand.Read(ctx.rand)
	if err != nil {
		log.Fatalf("Could not get random bytes: %v", err)
	}
	return ctx
}

func (s *Server) processConnect(rpc *ConnectRPC) {
	resp := &connectResponse{
		ProtocolVersion: 12, // TODO: set this like ZooKeeper does
		TimeOut:         rpc.req.TimeOut,
	}
	if rpc.req.SessionID == 0 {
		resp.SessionID, resp.Passwd = consensus.stateMachine.createSession(getContext(0))
	} else {
		resp.SessionID = rpc.req.SessionID
		resp.Passwd = rpc.req.Passwd
	}
	err := consensus.stateMachine.setConn(resp.SessionID, resp.Passwd, rpc.conn)
	if err != errOk {
		rpc.errReply(err)
		return
	}
	rpc.reply(resp)
}

func (s *Server) handler(rpcChan <-chan RPC) {
	for {
		rpc := <-rpcChan
		consensus.mutex.Lock()
		switch rpc := rpc.(type) {
		case *ConnectRPC:
			s.processConnect(rpc)

		case *PingRPC:
			log.Printf("Sending pong")
			rpc.reply()

		case *CreateRPC:
			resp, errCode := consensus.stateMachine.Create(getContext(rpc.sessionId), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		case *GetChildrenRPC:
			resp, errCode := consensus.stateMachine.GetChildren(getContext(rpc.sessionId), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		case *GetDataRPC:
			resp, errCode := consensus.stateMachine.GetData(getContext(rpc.sessionId), rpc.req)
			if errCode == errOk {
				rpc.reply(resp)
			} else {
				rpc.errReply(errCode)
			}

		case *SetDataRPC:
			resp, errCode := consensus.stateMachine.SetData(getContext(rpc.sessionId), rpc.req)
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

func (s *Server) startRaft() error {
	s.raft.settings = raft.DefaultConfig()
	s.raft.settings.LocalID = "server0"

	store, err := raftboltdb.NewBoltStore("store.bolt")
	if err != nil {
		return fmt.Errorf("Unable to initialize bolt store %v\n", err)
	}
	s.raft.stableStore = store
	s.raft.logStore = store

	s.raft.snapStore, err = raft.NewFileSnapshotStoreWithLogger("snapshot", 1, nil)
	if err != nil {
		return fmt.Errorf("Unable to initialize snapshot store %v\n", err)
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{
			raft.Server{
				Suffrage: raft.Voter,
				ID:       s.raft.settings.LocalID,
				Address:  "127.0.0.1:2180",
			},
		},
	}

	err = raft.BootstrapCluster(s.raft.settings,
		s.raft.logStore, s.raft.stableStore, s.raft.snapStore,
		configuration)
	if err != nil {
		return fmt.Errorf("Unable to bootstrap Raft server: %v\n", err)
	}

	s.raft.transport, err = raft.NewTCPTransportWithLogger(
		"127.0.0.1:2180",
		&net.TCPAddr{
			IP:   []byte{127, 0, 0, 1},
			Port: 2180,
		},
		4, time.Second, nil)
	if err != nil {
		return fmt.Errorf("Unable to start Raft transport: %v\n", err)
	}

	var stateMachine raft.FSM // TODO

	s.raft.handle, err = raft.NewRaft(s.raft.settings, stateMachine,
		s.raft.logStore, s.raft.stableStore, s.raft.snapStore, s.raft.transport)
	if err != nil {
		return fmt.Errorf("Unable to start Raft server: %v\n", err)
	}

	return nil
}

func (s *Server) serve() error {
	rpcChan := make(chan RPC)

	log.Print("listening for ZooKeeper clients on port 2181")
	juteServer := NewJuteServer(rpcChan)
	err := juteServer.Listen(":2181")
	if err != nil {
		return fmt.Errorf("error listening: %v", err)
	}

	for i := 0; i < 32; i++ {
		go s.handler(rpcChan)
	}
	select {} // block forever
}

func main() {
	s := &Server{}
	err := s.startRaft()
	if err != nil {
		log.Printf("error starting Raft: %v", err)
		os.Exit(1)
	}

	err = s.serve()
	if err != nil {
		log.Printf("error serving clients: %v", err)
		os.Exit(1)
	}
}
