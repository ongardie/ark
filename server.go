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
	"time"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
	"salesforce.com/zoolater/statemachine"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type Server struct {
	stateMachine *statemachine.StateMachine
	raft         struct {
		settings    *raft.Config
		stableStore raft.StableStore
		logStore    raft.LogStore
		snapStore   raft.SnapshotStore
		transport   raft.Transport
		handle      *raft.Raft
	}
}

func getRand(n int) []byte {
	rand := make([]byte, n)
	_, err := cryptoRand.Read(rand)
	if err != nil {
		log.Fatalf("Could not get random bytes: %v", err)
		return nil
	}
	return rand
}

func (s *Server) processConnect(rpc *ConnectRPC) {
	header := statemachine.CommandHeader1{
		CmdType:   statemachine.ConnectCommand,
		SessionId: 0,
		ConnId:    0,
		Time:      time.Now().Unix(),
		Rand:      getRand(proto.SessionPasswordLen),
	}
	headerBuf, err := jute.Encode(&header)
	if err != nil {
		log.Printf("Failed to encode connect log entry header: %v", err)
		rpc.conn.Close()
		return
	}
	buf := append(append([]byte{1}, headerBuf...), rpc.reqJute...)

	future := s.raft.handle.Apply(buf, 0)
	err = future.Error()
	if err != nil {
		log.Printf("Failed to commit connect command: %v", err)
		rpc.errReply(proto.ErrOperationTimeout) // TODO
		return
	}
	result := future.Response()
	log.Printf("Committed entry %v and output %+v", future.Index(), result)

	switch result := result.(type) {
	case proto.ErrCode:
		rpc.errReply(result)
	case *statemachine.ConnectResult:
		rpc.reply(&result.Resp, result.ConnId)
	default:
		log.Fatalf("Unexpected output type for connect command: %T", result)
	}
}

func (s *Server) processCommand(rpc *RPC) {
	log.Printf("Processing %v command", rpc.opName)
	header := statemachine.CommandHeader1{
		CmdType:   statemachine.NormalCommand,
		SessionId: rpc.conn.SessionId(),
		ConnId:    rpc.conn.ConnId(),
		Time:      time.Now().Unix(),
		Rand:      getRand(proto.SessionPasswordLen),
	}
	headerBuf, err := jute.Encode(&header)
	if err != nil {
		log.Printf("Failed to encode log entry header: %v", err)
		rpc.conn.Close()
		return
	}
	buf := []byte{1}
	buf = append(buf, headerBuf...)
	buf = append(buf, rpc.reqHeaderJute...)
	buf = append(buf, rpc.req...)
	future := s.raft.handle.Apply(buf, 0)
	err = future.Error()
	if err != nil {
		log.Printf("Failed to commit %v command: %v", rpc.opName, err)
		rpc.errReply(proto.ErrOperationTimeout) // TODO
		return
	}
	resp := future.Response()
	log.Printf("Committed entry %v", future.Index())

	switch resp := resp.(type) {
	case proto.ErrCode:
		rpc.errReply(resp)
	case []byte:
		rpc.reply(proto.ZXID(future.Index()), resp)
	default:
		log.Fatalf("Unexpected output type for %v command: %T (%#v)", rpc.opName, resp, resp)
	}
}

func (s *Server) processQuery(rpc *RPC) {
	log.Printf("Processing %v query", rpc.opName)
	zxid, resp, err := s.stateMachine.Query(rpc.conn,
		proto.ZXID(0), // TODO: ensure client sees ZXID going forward
		rpc.reqHeader.OpCode, rpc.req)
	if err == proto.ErrOk {
		rpc.reply(zxid, resp)
	} else {
		rpc.errReply(err)
	}
}

func isReadOnly(opCode proto.OpCode) bool {
	switch opCode {
	case proto.OpExists:
		return true
	case proto.OpGetAcl:
		return true
	case proto.OpGetChildren:
		return true
	case proto.OpGetChildren2:
		return true
	case proto.OpGetData:
		return true
	default:
		return false
	}
}

func (s *Server) handler(rpcChan <-chan RPCish) {
	for {
		rpc := <-rpcChan
		switch rpc := rpc.(type) {
		case *ConnectRPC:
			s.processConnect(rpc)
		case *RPC:
			if isReadOnly(rpc.reqHeader.OpCode) {
				s.processQuery(rpc)
			} else {
				s.processCommand(rpc)
			}
		}
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

	s.raft.handle, err = raft.NewRaft(s.raft.settings, s.stateMachine,
		s.raft.logStore, s.raft.stableStore, s.raft.snapStore, s.raft.transport)
	if err != nil {
		return fmt.Errorf("Unable to start Raft server: %v\n", err)
	}

	return nil
}

func (s *Server) serve() error {
	rpcChan := make(chan RPCish)

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
	s := &Server{
		stateMachine: statemachine.NewStateMachine(),
	}
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
