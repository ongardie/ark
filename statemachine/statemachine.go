/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"

	"github.com/hashicorp/raft"
)

type StateMachine struct {
	serverId          uint64
	mutex             sync.Mutex
	lastApplied       proto.ZXID
	tree              *Tree
	sessions          map[proto.SessionId]*Session
	watches           map[TreeEvent]Connections
	output            map[fullCommandId]CommandResultFn
	connectOutput     map[string]ConnectResultFn
	minSessionTimeout time.Duration
}

type fullCommandId struct {
	session    proto.SessionId
	connection ConnectionId
	cmd        CommandId
}

type Connection interface {
	Close()
	Notify(zxid proto.ZXID, event TreeEvent)
	String() string
	SessionId() proto.SessionId
	ConnId() ConnectionId
}

type Connections []Connection

type context struct {
	zxid      proto.ZXID
	term      uint64
	time      int64
	rand      []byte
	server    uint64
	sessionId proto.SessionId
	connId    ConnectionId
	cmdId     CommandId
}

type CommandType int32

const (
	NoOpCommand    CommandType = 0
	NormalCommand              = 1
	ConnectCommand             = 2
	ExpireCommand              = 3
)

// This is preceded by a 1-byte version number set to 1.
type CommandHeader1 struct {
	CmdType   CommandType
	Server    uint64
	SessionId proto.SessionId
	ConnId    ConnectionId
	CmdId     CommandId
	Time      int64
	Rand      []byte
}

type ExpireRequest struct {
	Term     uint64
	Sessions []ExpireEntry
}
type ExpireEntry struct {
	Session  proto.SessionId
	LastConn ConnectionId
}

type TreeEvent struct {
	Path  proto.Path
	Which proto.EventType
}

type NotifyEvents []TreeEvent
type RegisterEvents []TreeEvent

type ConnectionId int64
type CommandId int64

type CommandResultFn func(index proto.ZXID, output []byte, errCode proto.ErrCode)
type ConnectResultFn func(resp *proto.ConnectResponse, connId ConnectionId, errCode proto.ErrCode)
type QueryResultFn func(zxid proto.ZXID, output []byte, errCode proto.ErrCode)

type Session struct {
	password  proto.SessionPassword
	connId    ConnectionId
	connOwner uint64
	lastCmdId CommandId
	queries   []*asyncQuery // these are waiting for lastCmdId to advance
	timeout   time.Duration
	// Expire session with owner == self after a session timeout
	// since contact with client.
	// If leader, expire session with owner != self after owner
	// has not made contact for two session timeouts.
	lastContact time.Time
}

func NewStateMachine(serverId uint64, minSessionTimeout time.Duration) *StateMachine {
	return &StateMachine{
		serverId:          serverId,
		lastApplied:       0,
		tree:              NewTree(),
		sessions:          make(map[proto.SessionId]*Session),
		watches:           make(map[TreeEvent]Connections),
		output:            make(map[fullCommandId]CommandResultFn),
		connectOutput:     make(map[string]ConnectResultFn),
		minSessionTimeout: minSessionTimeout,
	}
}

var ErrSessionExpired = proto.ErrSessionExpired.Error()
var ErrDisconnect = errors.New("No contact with leader in a while")

func (sm *StateMachine) Ping(sessionId proto.SessionId,
	lastLeaderContact time.Duration) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	session, ok := sm.sessions[sessionId]
	if !ok {
		return ErrSessionExpired
	}
	session.lastContact = time.Now()
	if lastLeaderContact > session.timeout*time.Duration(2)/time.Duration(3) {
		return ErrDisconnect
	}
	return nil
}

func (sm *StateMachine) Elapsed(
	now time.Time,
	term uint64,
	lastLeaderContact time.Time,
	leader bool,
	stableSince time.Time,
	serverContact func(uint64) time.Time) *ExpireRequest {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	expire := &ExpireRequest{Term: term}
	for sessionId, session := range sm.sessions {
		hasExpired := false
		if now.Sub(session.lastContact) > session.timeout {
			if session.connOwner == sm.serverId {
				hasExpired = true
			} else if leader {
				lastContact := serverContact(session.connOwner)
				if lastContact.After(stableSince) &&
					now.Sub(lastContact) > session.timeout*time.Duration(2) {
					hasExpired = true
				}
			}
			if hasExpired {
				log.Printf("Session %v is out of time", sessionId)
				expire.Sessions = append(expire.Sessions,
					ExpireEntry{Session: sessionId, LastConn: session.connId})
			}
		}
	}
	if len(expire.Sessions) > 0 {
		return expire
	} else {
		return nil
	}
}

func (sm *StateMachine) notifyWatch(zxid proto.ZXID, event TreeEvent) {
	log.Printf("Firing %+v", event)
	for _, conn := range sm.watches[event] {
		log.Printf("Notifying connection %v of %+v", conn, event)
		conn.Notify(zxid, event)
	}
	delete(sm.watches, event)
}

func (sm *StateMachine) notifyWatches(zxid proto.ZXID, events NotifyEvents) {
	for _, event := range events {
		sm.notifyWatch(zxid, event)
	}
}

func (sm *StateMachine) registerWatch(event TreeEvent, conn Connection) {
	watching := sm.watches[event]
	for _, other := range watching {
		if other == conn {
			return
		}
	}
	log.Printf("Registering connection %v for %+v", conn, event)
	sm.watches[event] = append(watching, conn)
}

func (sm *StateMachine) registerWatches(events RegisterEvents, conn Connection) {
	for _, event := range events {
		sm.registerWatch(event, conn)
	}
}

func (sm *StateMachine) applyConnect(ctx *context, cmdBuf []byte, resultFn ConnectResultFn) {
	req := proto.ConnectRequest{}
	err := jute.Decode(cmdBuf, &req)
	if err != nil {
		log.Printf("Error decoding connect command: %v", err)
		resultFn(nil, 0, proto.ErrBadArguments)
		return
	}
	log.Printf("State machine got connect request %+v", req)

	var sessionId proto.SessionId
	var session *Session
	if req.SessionID == 0 { // create session
		if len(ctx.rand) < proto.SessionPasswordLen {
			log.Fatalf("Need %v random bytes for createSession, have %v",
				proto.SessionPasswordLen, len(ctx.rand))
		}
		sessionId = proto.SessionId(ctx.zxid)
		session = &Session{
			password: ctx.rand[:proto.SessionPasswordLen],
			connId:   1,
		}
		sm.sessions[sessionId] = session
	} else { // attach to existing session
		sessionId = req.SessionID
		var ok bool
		session, ok = sm.sessions[sessionId]
		if !ok {
			log.Printf("Session %v not found", sessionId)
			resultFn(nil, 0, proto.ErrSessionExpired)
			return
		}
		if !bytes.Equal(session.password, req.Passwd) {
			log.Printf("Bad pasword for session %v", sessionId)
			resultFn(nil, 0, proto.ErrSessionExpired)
			return
		}
		session.connId++
	}
	session.connOwner = ctx.server
	session.lastCmdId = ctx.cmdId
	session.timeout = time.Millisecond * time.Duration(req.Timeout)
	if session.timeout < sm.minSessionTimeout {
		session.timeout = sm.minSessionTimeout
	}
	session.lastContact = time.Now()
	resp := &proto.ConnectResponse{
		ProtocolVersion: 0,
		Timeout:         int32(session.timeout / time.Millisecond),
		SessionID:       sessionId,
		Passwd:          session.password,
	}
	resultFn(resp, session.connId, proto.ErrOk)
}

func (sm *StateMachine) applyExpire(ctx *context, cmdBuf []byte) {
	req := ExpireRequest{}
	err := jute.Decode(cmdBuf, &req)
	if err != nil {
		log.Printf("Error decoding expire list: %v", err)
		return
	}
	if req.Term != ctx.term {
		log.Printf("Ignoring expire list with stale term")
		return
	}
	for _, expire := range req.Sessions {
		session, ok := sm.sessions[expire.Session]
		if !ok {
			continue
		}
		if session.connId != expire.LastConn {
			continue
		}
		delete(sm.sessions, expire.Session)
		tree, notify := sm.tree.ExpireSession(ctx.zxid, expire.Session)
		sm.tree = tree
		sm.notifyWatches(ctx.zxid, notify)
	}
}

func (sm *StateMachine) applyMulti(ctx *context, cmdBuf []byte) (*Tree, []byte, NotifyEvents, proto.ErrCode) {
	more := cmdBuf
	tree := sm.tree
	var respHeaders []*proto.MultiHeader
	var results []interface{}
	var notify NotifyEvents
	failed := false

	for {
		opHeader := &proto.MultiHeader{}
		var err error
		more, err = jute.DecodeSome(more, opHeader)
		if err != nil {
			log.Printf("Ignoring multi request with op header decode error: %v", err)
			return nil, nil, nil, proto.ErrAPIError
		}
		if opHeader.Err != -1 {
			log.Printf("Ignoring multi request with bad op header: %v", opHeader)
			return nil, nil, nil, proto.ErrAPIError
		}
		if opHeader.Done {
			if opHeader.Type != -1 {
				log.Printf("Ignoring multi request with bad op header: %v", opHeader)
				return nil, nil, nil, proto.ErrAPIError
			}
			if len(more) > 0 {
				log.Printf("Ignoring multi request with %v extra bytes", len(more))
				return nil, nil, nil, proto.ErrAPIError
			}
			break
		}

		if failed {
			respHeaders = append(respHeaders, &proto.MultiHeader{
				Type: proto.OpError,
				Done: false,
				Err:  proto.ErrRuntimeInconsistency,
			})
		}

		switch opHeader.Type {
		case proto.OpCreate:
			log.Printf("multi create")
			op := &proto.CreateRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode create op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			t, resp, n, errCode := sm.tree.Create(ctx, op)
			notify = append(notify, n...)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpCreate,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
				tree = t
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		case proto.OpSetData:
			log.Printf("multi setData")
			op := &proto.SetDataRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode setData op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			t, resp, n, errCode := sm.tree.SetData(ctx, op)
			notify = append(notify, n...)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpSetData,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
				tree = t
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		case proto.OpDelete:
			log.Printf("multi delete")
			op := &proto.DeleteRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode delete op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			t, resp, n, errCode := sm.tree.Delete(ctx, op)
			notify = append(notify, n...)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpDelete,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
				tree = t
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		case proto.OpCheck:
			log.Printf("multi check")
			op := &proto.CheckVersionRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode check op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			resp, errCode := sm.tree.CheckVersion(ctx, op)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpCheck,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		default:
			log.Printf("Ignoring multi request with unknown type: %v", opHeader)
			return nil, nil, nil, proto.ErrAPIError
		}
	}

	if failed {
		tree = sm.tree
		notify = nil
		results = nil
		for _, hdr := range respHeaders {
			if hdr.Type != proto.OpError {
				hdr.Type = proto.OpError
			}
			results = append(results, &proto.MultiErrorResponse{hdr.Err})
		}
	}

	respHeaders = append(respHeaders, &proto.MultiHeader{
		Type: -1,
		Done: true,
		Err:  -1,
	})

	var respBuf []byte
	for i, hdr := range respHeaders {
		log.Printf("multi %+v", hdr)
		buf, err := jute.Encode(hdr)
		if err != nil {
			log.Printf("Could not encode %+v", hdr)
			return nil, nil, nil, proto.ErrAPIError
		}
		respBuf = append(respBuf, buf...)
		if len(results) > i {
			log.Printf("multi %+v", results[i])
			buf, err := jute.Encode(results[i])
			if err != nil {
				log.Printf("Could not encode %+v", results[i])
				return nil, nil, nil, proto.ErrAPIError
			}
			respBuf = append(respBuf, buf...)
		}
	}

	return tree, respBuf, notify, proto.ErrOk
}

func (sm *StateMachine) applyCommand(ctx *context, cmdBuf []byte) ([]byte, proto.ErrCode) {
	reqHeader := proto.RequestHeader{}
	reqBuf, err := jute.DecodeSome(cmdBuf, &reqHeader)
	if err != nil {
		log.Printf("Error decoding command header: %v", err)
		return nil, proto.ErrBadArguments
	}
	log.Printf("command request header: %+v", reqHeader)

	var opName string
	if name, ok := proto.OpNames[reqHeader.OpCode]; ok {
		opName = name
	} else {
		opName = fmt.Sprintf("unknown (%v)", reqHeader.OpCode)
	}

	var req2 interface{}
	var resp interface{}
	var notify NotifyEvents
	var tree *Tree
	var errCode proto.ErrCode = proto.ErrAPIError

	decode := func(req interface{}) bool {
		err := jute.Decode(reqBuf, req)
		if err != nil {
			log.Printf("Could not decode %v bytes into %T. Ignoring.", len(reqBuf), req)
			return false
		}
		req2 = req
		return true
	}
	var respBuf []byte
	switch reqHeader.OpCode {
	case proto.OpClose:
		if req := new(proto.CloseRequest); decode(req) {
			delete(sm.sessions, ctx.sessionId)
			tree, resp, notify, errCode = sm.tree.CloseSession(ctx, req)
		}

	case proto.OpCreate:
		if req := new(proto.CreateRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.Create(ctx, req)
		}

	case proto.OpDelete:
		if req := new(proto.DeleteRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.Delete(ctx, req)
		}

	case proto.OpMulti:
		tree, respBuf, notify, errCode = sm.applyMulti(ctx, reqBuf)

	case proto.OpSetData:
		if req := new(proto.SetDataRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.SetData(ctx, req)
		}

	case proto.OpSync:
		if req := new(proto.SyncRequest); decode(req) {
			tree = sm.tree
			resp = &proto.SyncResponse{Path: req.Path}
			notify = nil
			errCode = proto.ErrOk
		}
	}

	if errCode != proto.ErrOk {
		log.Printf("%v(%+v) -> %v", opName, req2, errCode.Error())
		return nil, errCode
	}
	log.Printf("%v(%+v) -> %+v, %v notifies", opName, req2, resp, len(notify))
	sm.tree = tree
	sm.notifyWatches(ctx.zxid, notify)

	if respBuf == nil && resp != nil {
		respBuf, err = jute.Encode(resp)
		if err != nil {
			log.Printf("Could not encode %+v", resp)
			return nil, proto.ErrAPIError
		}
	}
	return respBuf, proto.ErrOk
}

func (sm *StateMachine) Apply(entry *raft.Log) (unused interface{}) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.lastApplied = proto.ZXID(entry.Index)

	if entry.Type != raft.LogCommand {
		return
	}

	if len(entry.Data) == 0 {
		log.Fatalf("Empty log entry command. Crashing.")
	}
	version := entry.Data[0]
	if version != 1 {
		log.Fatalf("Unknown version %d of log entry command header. Crashing.", version)
	}

	header := CommandHeader1{}
	cmdBuf, err := jute.DecodeSome(entry.Data[1:], &header)
	if err != nil {
		log.Fatalf("Error decoding log entry command header (%v). Crashing.", err)
	}

	ctx := &context{
		zxid:      proto.ZXID(entry.Index),
		term:      entry.Term,
		server:    header.Server,
		time:      header.Time,
		rand:      header.Rand,
		sessionId: header.SessionId,
		connId:    header.ConnId,
		cmdId:     header.CmdId,
	}

	reply := func(output []byte, errCode proto.ErrCode) {
		fullId := fullCommandId{ctx.sessionId, ctx.connId, ctx.cmdId}
		replyFn, ok := sm.output[fullId]
		if !ok {
			return
		}
		replyFn(ctx.zxid, output, errCode)
		delete(sm.output, fullId)
	}

	switch header.CmdType {
	case ConnectCommand:
		resultFn, ok := sm.connectOutput[string(ctx.rand)]
		if ok {
			delete(sm.connectOutput, string(ctx.rand))
		} else {
			resultFn = func(*proto.ConnectResponse, ConnectionId, proto.ErrCode) {}
		}
		sm.applyConnect(ctx, cmdBuf, resultFn)
		return

	case ExpireCommand:
		sm.applyExpire(ctx, cmdBuf)
		return

	case NoOpCommand:
		break
	case NormalCommand:
		break
	default:
		reply(nil, proto.ErrUnimplemented)
		return
	}

	session, ok := sm.sessions[ctx.sessionId]
	if !ok {
		log.Printf("Session %v not found", ctx.sessionId)
		reply(nil, proto.ErrSessionExpired)
		return
	}
	if ctx.connId != session.connId {
		log.Printf("Expired connection ID %v for session %v", ctx.connId, ctx.sessionId)
		reply(nil, proto.ErrSessionMoved)
		return
	}
	if ctx.cmdId != session.lastCmdId+1 {
		log.Printf("Unexpected command ID %v for session %v (last was %v). Ignoring.",
			ctx.cmdId, ctx.sessionId, session.lastCmdId)
		reply(nil, proto.ErrInvalidState)
		return
	}

	var result []byte
	var errCode proto.ErrCode
	if header.CmdType == NormalCommand {
		result, errCode = sm.applyCommand(ctx, cmdBuf)
	}
	reply(result, errCode)
	session.lastCmdId = ctx.cmdId

	all := session.queries
	session.queries = nil
	for _, query := range all {
		sm.queueQuery(query)
	}

	return
}

func (sm *StateMachine) ExpectCommand(
	session proto.SessionId, connection ConnectionId, cmd CommandId,
	fn CommandResultFn) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.output[fullCommandId{session, connection, cmd}] = fn
}

func (sm *StateMachine) ExpectConnect(rand []byte, fn ConnectResultFn) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.connectOutput[string(rand)] = fn
}

func (sm *StateMachine) CancelCommandResult(
	session proto.SessionId, connection ConnectionId, cmd CommandId) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	delete(sm.output, fullCommandId{session, connection, cmd})
}

func (sm *StateMachine) CancelConnectResult(rand []byte) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	delete(sm.connectOutput, string(rand))
}

type asyncQuery struct {
	conn      Connection
	lastCmdId CommandId
	opCode    proto.OpCode
	reqBuf    []byte
	resultFn  QueryResultFn
}

func (sm *StateMachine) Query(
	conn Connection, lastCmdId CommandId,
	opCode proto.OpCode, reqBuf []byte, resultFn QueryResultFn) {
	query := &asyncQuery{
		conn:      conn,
		lastCmdId: lastCmdId,
		opCode:    opCode,
		reqBuf:    reqBuf,
		resultFn:  resultFn,
	}
	log.Printf("Created async query %+v", query)

	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.queueQuery(query)
	session, ok := sm.sessions[query.conn.SessionId()]
	if ok {
		session.lastContact = time.Now()
	}
}

func (sm *StateMachine) queueQuery(query *asyncQuery) {
	session, ok := sm.sessions[query.conn.SessionId()]
	if !ok {
		log.Printf("Session %v not found", query.conn.SessionId())
		query.resultFn(sm.lastApplied, nil, proto.ErrSessionExpired)
		return
	}

	if query.conn.ConnId() != session.connId {
		log.Printf("Expired connection ID %v for session %v",
			query.conn.ConnId(), query.conn.SessionId())
		query.resultFn(sm.lastApplied, nil, proto.ErrSessionExpired)
		return
	}

	if query.lastCmdId < session.lastCmdId {
		log.Fatalf("last command ID too old, can't satisfy")
	} else if query.lastCmdId > session.lastCmdId {
		log.Printf("last command ID too new, need to wait")
		session.queries = append(session.queries, query)
		return
	}

	ctx := &context{
		zxid:      sm.lastApplied,
		time:      time.Now().Unix(),
		sessionId: query.conn.SessionId(),
		connId:    query.conn.ConnId(),
		cmdId:     0,
	}

	decode := func(req interface{}) bool {
		err := jute.Decode(query.reqBuf, req)
		if err != nil {
			log.Printf("Could not decode %v bytes into %T. Closing connection",
				len(query.reqBuf), req)
			query.conn.Close()
			return false
		}
		return true
	}

	var resp interface{}
	var register RegisterEvents
	errCode := proto.ErrMarshallingError
	switch query.opCode {
	case proto.OpExists:
		if req := new(proto.ExistsRequest); decode(req) {
			resp, register, errCode = sm.tree.Exists(ctx, req)
			log.Printf("Exists(%+v) -> %+v", req, resp)
		}

	case proto.OpGetChildren:
		if req := new(proto.GetChildrenRequest); decode(req) {
			resp, register, errCode = sm.tree.GetChildren(ctx, req)
			log.Printf("GetChildren(%+v) -> %+v", req, resp)
		}

	case proto.OpGetChildren2:
		if req := new(proto.GetChildren2Request); decode(req) {
			resp, register, errCode = sm.tree.GetChildren2(ctx, req)
			log.Printf("GetChildren2(%+v) -> %+v", req, resp)
		}

	case proto.OpGetData:
		if req := new(proto.GetDataRequest); decode(req) {
			resp, register, errCode = sm.tree.GetData(ctx, req)
			log.Printf("GetData(%+v) -> %+v", req, resp)
		}

	case proto.OpSetWatches:
		if req := new(proto.SetWatchesRequest); decode(req) {
			var localNotify NotifyEvents
			resp, register, localNotify, errCode = sm.tree.SetWatches(ctx, req)
			log.Printf("SetWatches(%+v) -> %+v", req, resp)
			for _, event := range localNotify {
				query.conn.Notify(ctx.zxid, event)
			}
		}

	default:
		errCode = proto.ErrUnimplemented
	}

	// We're supposed to register watches even if query returns an error. For
	// instance, Exists() can return ErrNoNode but still set watches on the
	// creation of the node.
	sm.registerWatches(register, query.conn)

	if errCode != proto.ErrOk {
		query.resultFn(sm.lastApplied, nil, errCode)
		return
	}
	respBuf, err := jute.Encode(resp)
	if err != nil {
		log.Printf("Could not encode %+v. Closing connection", resp)
		query.conn.Close()
		query.resultFn(sm.lastApplied, nil, proto.ErrMarshallingError)
		return
	}

	query.resultFn(sm.lastApplied, respBuf, proto.ErrOk)
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return nil, errors.New("Unimplemented")
}

func (sm *StateMachine) Restore(io.ReadCloser) error {
	return errors.New("Unimplemented")
}
