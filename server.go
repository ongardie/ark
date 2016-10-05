/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	cryptoRand "crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
	"salesforce.com/zoolater/statemachine"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type Server struct {
	options struct {
		bootstrap         bool
		serverId          uint64
		storeDir          string
		peerAddress       string
		clientAddress     string
		adminAddress      string
		heartbeatInterval time.Duration
	}
	stateMachine *statemachine.StateMachine
	raft         struct {
		settings    *raft.Config
		stableStore raft.StableStore
		logStore    raft.LogStore
		snapStore   raft.SnapshotStore
		transport   raft.Transport
		handle      *raft.Raft
	}
	logAppender     *logAppender
	failureDetector *failureDetector
}

const HEARTBEAT_INTERVAL = 250 * time.Millisecond

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
		Server:    s.options.serverId,
		SessionId: 0,
		ConnId:    0,
		CmdId:     1,
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

	doneCh := make(chan struct{})
	s.stateMachine.ExpectConnect(header.Rand,
		func(resp *proto.ConnectResponse, connId statemachine.ConnectionId, errCode proto.ErrCode) {
			log.Printf("Committed connect request with output %+v", resp)
			if errCode != proto.ErrOk {
				rpc.errReply(errCode)
			} else {
				rpc.reply(resp, connId)
			}
			close(doneCh)
		})
	errCh := s.logAppender.Append(buf, doneCh)
	go func() {
		select {
		case err := <-errCh:
			log.Printf("Failed to commit connect command: %v", err)
			rpc.conn.Close()
			s.stateMachine.CancelConnectResult(header.Rand)
		case <-doneCh:
		}
	}()
}

func (s *Server) processCommand(rpc *RPC) {
	log.Printf("Processing %v command", rpc.opName)
	header := statemachine.CommandHeader1{
		CmdType:   statemachine.NormalCommand,
		Server:    s.options.serverId,
		SessionId: rpc.conn.SessionId(),
		ConnId:    rpc.conn.ConnId(),
		CmdId:     rpc.cmdId,
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

	doneCh := make(chan struct{})
	s.stateMachine.ExpectCommand(header.SessionId, header.ConnId, header.CmdId,
		func(index proto.ZXID, output []byte, errCode proto.ErrCode) {
			log.Printf("Committed entry %v", index)
			if errCode == proto.ErrOk {
				if rpc.reqHeader.OpCode == proto.OpClose {
					rpc.replyThenClose(index, output)
				} else {
					rpc.reply(index, output)
				}
			} else if errCode == proto.ErrInvalidState {
				log.Printf("Could not apply %v command: arrived out of order", rpc.opName)
				rpc.conn.Close()
			} else {
				rpc.errReply(index, errCode)
			}
			close(doneCh)
		})
	errCh := s.logAppender.Append(buf, doneCh)
	go func() {
		select {
		case err := <-errCh:
			log.Printf("Failed to commit %v command: %v", rpc.opName, err)
			rpc.conn.Close()
			s.stateMachine.CancelCommandResult(header.SessionId, header.ConnId, header.CmdId)
		case <-doneCh:
		}
	}()
}

func (s *Server) processPing(rpc *RPC) {
	log.Printf("Processing ping")

	var lastLeaderContact time.Duration
	if isLeader(s.raft.handle) {
		// TODO: should expose lastContact from raft.go:checkLeaderLease().
		lastLeaderContact = s.raft.settings.LeaderLeaseTimeout
	} else {
		lastLeaderContact = time.Now().Sub(s.failureDetector.LastLeaderContact())
	}
	err := s.stateMachine.Ping(rpc.conn.SessionId(), lastLeaderContact)
	switch err {
	case nil:
		log.Printf("Pong")
		rpc.reply(0, []byte{})
	case statemachine.ErrSessionExpired:
		log.Printf("Session expired")
		rpc.errReply(0, proto.ErrSessionExpired)
	case statemachine.ErrDisconnect:
		log.Printf("Last leader contact too old. Disconnecting")
		rpc.conn.Close()
	default:
		log.Printf("Unknown ping error: %v. Disconnecting", err)
		rpc.conn.Close()

	}
}

func (s *Server) processQuery(rpc *RPC) {
	log.Printf("Processing %v query", rpc.opName)
	s.stateMachine.Query(rpc.conn, rpc.lastCmdId, rpc.reqHeader.OpCode, rpc.req,
		func(zxid proto.ZXID, output []byte, errCode proto.ErrCode) {
			log.Printf("Got result for %v query", rpc.opName)
			if errCode == proto.ErrOk {
				rpc.reply(zxid, output)
			} else {
				rpc.errReply(zxid, errCode)
			}
		})
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
	case proto.OpPing:
		return true
	case proto.OpSetWatches:
		return true
	default:
		return false
	}
}

func (s *Server) handler(rpc RPCish) {
	switch rpc := rpc.(type) {
	case *ConnectRPC:
		s.processConnect(rpc)
	case *RPC:
		if rpc.reqHeader.OpCode == proto.OpPing {
			s.processPing(rpc)
		} else if isReadOnly(rpc.reqHeader.OpCode) {
			s.processQuery(rpc)
		} else {
			s.processCommand(rpc)
		}
	}
}

const (
	RAFT_PROTO        byte = 10
	COMMAND_FORWARDER byte = 11
	FAILURE_DETECTOR  byte = 12
)

func (s *Server) startRaft(streamLayer raft.StreamLayer) error {
	s.raft.settings = raft.DefaultConfig()
	s.raft.settings.LocalID = raft.ServerID(fmt.Sprintf("server%v", s.options.serverId))

	err := os.MkdirAll(s.options.storeDir, os.ModeDir|0755)
	if err != nil {
		return fmt.Errorf("Unable to create store directory %v: %v\n",
			s.options.storeDir, err)
	}

	store, err := raftboltdb.NewBoltStore(s.options.storeDir + "/store.bolt")
	if err != nil {
		return fmt.Errorf("Unable to initialize bolt store %v\n", err)
	}
	s.raft.stableStore = store
	s.raft.logStore = store

	s.raft.snapStore, err = raft.NewFileSnapshotStoreWithLogger(s.options.storeDir, 1, nil)
	if err != nil {
		return fmt.Errorf("Unable to initialize snapshot store %v\n", err)
	}

	s.raft.transport = raft.NewNetworkTransportWithLogger(streamLayer, 4, time.Second, nil)

	if s.options.bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				raft.Server{
					Suffrage: raft.Voter,
					ID:       s.raft.settings.LocalID,
					Address:  raft.ServerAddress(s.options.peerAddress),
				},
			},
		}

		err = raft.BootstrapCluster(s.raft.settings,
			s.raft.logStore, s.raft.stableStore, s.raft.snapStore, s.raft.transport,
			configuration)
		if err != nil {
			return fmt.Errorf("Unable to bootstrap Raft server: %v\n", err)
		}

		os.Exit(0)
	}

	s.raft.handle, err = raft.NewRaft(s.raft.settings, s.stateMachine,
		s.raft.logStore, s.raft.stableStore, s.raft.snapStore, s.raft.transport)
	if err != nil {
		return fmt.Errorf("Unable to start Raft server: %v\n", err)
	}

	return nil
}

type addVoterRequest struct {
	ServerId  raft.ServerID
	Address   raft.ServerAddress
	PrevIndex uint64
}

func (s *Server) serve() error {
	log.Printf("listening for admin requests on %v", s.options.adminAddress)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		fmt.Fprintf(w, "admin http server")
	})
	adminMux.HandleFunc("/raft/stats", func(w http.ResponseWriter, req *http.Request) {
		for k, v := range s.raft.handle.Stats() {
			fmt.Fprintf(w, "%v: %v\n", k, v)
		}
	})
	adminMux.HandleFunc("/raft/membership", func(w http.ResponseWriter, req *http.Request) {
		future := s.raft.handle.GetConfiguration()
		err := future.Error()
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}
		fmt.Fprintf(w, "index: %v, configuration: %+v",
			future.Index(), future.Configuration())
	})
	adminMux.HandleFunc("/api/raft/addvoter", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" {
			decoder := json.NewDecoder(req.Body)
			args := addVoterRequest{}
			err := decoder.Decode(&args)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error decoding JSON: %v", err), http.StatusBadRequest)
				return
			}
			log.Printf("AddVoter %+v\n", args)
			future := s.raft.handle.AddVoter(
				args.ServerId,
				args.Address,
				args.PrevIndex,
				time.Second*5)
			err = future.Error()
			if err != nil {
				http.Error(w, fmt.Sprintf("AddVoter error: %v", err), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "AddVoter succeeded")
		} else {
			http.NotFound(w, req)
			return
		}
	})
	adminServer := &http.Server{
		Addr:    s.options.adminAddress,
		Handler: adminMux,
	}
	go func() { log.Fatal(adminServer.ListenAndServe()) }()

	log.Printf("listening for ZooKeeper clients on %v", s.options.clientAddress)
	juteServer := &JuteServer{handler: s.handler}
	err := juteServer.Listen(s.options.clientAddress)
	if err != nil {
		return fmt.Errorf("error listening: %v", err)
	}

	select {} // block forever
}

func isLeader(r *raft.Raft) bool {
	return r.State() == raft.Leader
}
func getTerm(r *raft.Raft) uint64 {
	str := r.Stats()["term"]
	var term uint64
	_, err := fmt.Sscanf(str, "%d", &term)
	if err != nil {
		log.Fatalf("sscanf failed to convert '%v' to uint64: %v", str, err)
	}
	return term
}

func (s *Server) expireSessions() {
	const tick = 50 * time.Millisecond
	raft := s.raft.handle

	lastLeader := isLeader(raft)
	lastTerm := getTerm(raft)
	start := time.Now()

	for {
		time.Sleep(tick)
		leader := isLeader(raft)
		term := getTerm(raft)
		if leader != lastLeader || term != lastTerm {
			lastLeader = leader
			lastTerm = term
			start = time.Now()
			continue
		}
		now := time.Now()
		var lastLeaderContact time.Time
		if leader {
			lastLeaderContact = now
		} else {
			lastLeaderContact = s.failureDetector.LastLeaderContact()
		}
		// A tick elapsed while leader and term were steady.
		expire := s.stateMachine.Elapsed(now, term,
			lastLeaderContact,
			leader, start, s.failureDetector.LastContact)
		if expire == nil {
			continue
		}
		log.Printf("expire: %+v", expire)

		header := statemachine.CommandHeader1{
			CmdType:   statemachine.ExpireCommand,
			Server:    s.options.serverId,
			SessionId: 0,
			ConnId:    0,
			CmdId:     0,
			Time:      time.Now().Unix(),
			Rand:      getRand(proto.SessionPasswordLen),
		}
		headerBuf, err := jute.Encode(&header)
		if err != nil {
			log.Fatalf("Failed to encode expire log entry header: %v", err)
		}
		cmdBuf, err := jute.Encode(expire)
		if err != nil {
			log.Fatalf("Failed to encode expire list: %v", err)
		}
		buf := append(append([]byte{1}, headerBuf...), cmdBuf...)

		doneCh := make(chan struct{})
		close(doneCh)
		errCh := s.logAppender.Append(buf, doneCh)
		select {
		case err := <-errCh:
			log.Printf("Failed to commit expire command: %v", err)
		default:
		}
	}
}

func main() {
	s := &Server{}

	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.BoolVar(&s.options.bootstrap, "bootstrap", false,
		"initialize a new cluster containing just this server and immediately exit")
	flags.Uint64Var(&s.options.serverId, "id", 0,
		"local Server ID (must be unique across Raft cluster; required)")
	flags.StringVar(&s.options.peerAddress, "peeraddr", "",
		"local address given to other servers (required)")
	flags.StringVar(&s.options.clientAddress, "clientaddr", "0:2181",
		"local address on which to listen for client requests")
	flags.StringVar(&s.options.adminAddress, "adminaddr", "0:2182",
		"local address on which to listen for management requests")
	flags.StringVar(&s.options.storeDir, "store", "store",
		"directory to store Raft log and snapshots")
	flags.Parse(os.Args[1:])

	if s.options.serverId == 0 {
		log.Printf("Error: -id is required")
		flags.PrintDefaults()
		os.Exit(2)
	}
	if s.options.peerAddress == "" {
		log.Printf("Error: -peeraddr is required")
		flags.PrintDefaults()
		os.Exit(2)
	}

	s.options.storeDir = fmt.Sprintf("%s/server%v",
		s.options.storeDir, s.options.serverId)

	s.stateMachine = statemachine.NewStateMachine(s.options.serverId, HEARTBEAT_INTERVAL*3)

	addr, err := net.ResolveTCPAddr("tcp", s.options.peerAddress)
	if err != nil {
		log.Printf("Unable to resolve %v: %v\n", s.options.peerAddress, err)
		os.Exit(1)
	}
	stream, err := NewTCPTransport(s.options.peerAddress, addr)
	if err != nil {
		log.Printf("Unable to start peer transport: %v\n", err)
		os.Exit(1)
	}
	streamLayers := NewDemuxStreamLayer(stream,
		RAFT_PROTO, COMMAND_FORWARDER, FAILURE_DETECTOR)

	err = s.startRaft(streamLayers[RAFT_PROTO])
	if err != nil {
		log.Printf("error starting Raft: %v", err)
		os.Exit(1)
	}

	s.logAppender = newLogAppender(s.raft.handle, streamLayers[COMMAND_FORWARDER])
	s.failureDetector = newFailureDetector(s.stateMachine, s.raft.handle, streamLayers[FAILURE_DETECTOR],
		s.options.serverId, HEARTBEAT_INTERVAL)

	go s.expireSessions()

	err = s.serve()
	if err != nil {
		log.Printf("error serving clients: %v", err)
		os.Exit(1)
	}
}
