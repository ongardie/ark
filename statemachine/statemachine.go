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
	mutex         sync.Mutex
	lastApplied   proto.ZXID
	tree          *Tree
	sessions      map[proto.SessionId]*Session
	watches       map[TreeEvent]Connections
	output        map[fullCommandId]chan<- CommandResult
	connectOutput map[string]chan<- ConnectResult
}

type fullCommandId struct {
	session    proto.SessionId
	connection ConnectionId
	cmd        CommandId
}

type CommandResult struct {
	Index   proto.ZXID
	Output  []byte
	ErrCode proto.ErrCode
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
	time      int64
	rand      []byte
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
	SessionId proto.SessionId
	ConnId    ConnectionId
	CmdId     CommandId
	Time      int64
	Rand      []byte
}

type ExpireRequest struct {
	Sessions []proto.SessionId
}

type TreeEvent struct {
	Path  proto.Path
	Which proto.EventType
}

type NotifyEvents []TreeEvent
type RegisterEvents []TreeEvent

type ConnectionId int64
type CommandId int64

type ConnectResult struct {
	Resp    *proto.ConnectResponse
	ConnId  ConnectionId
	ErrCode proto.ErrCode
}

type Session struct {
	password  proto.SessionPassword
	connId    ConnectionId
	lastCmdId CommandId
	queries   []*asyncQuery // these are waiting for lastCmdId to advance
	timeout   time.Duration
	lease     time.Duration
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		lastApplied:   0,
		tree:          NewTree(),
		sessions:      make(map[proto.SessionId]*Session),
		watches:       make(map[TreeEvent]Connections),
		output:        make(map[fullCommandId]chan<- CommandResult),
		connectOutput: make(map[string]chan<- ConnectResult),
	}
}

func (sm *StateMachine) Ping(sessionId proto.SessionId) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	session, ok := sm.sessions[sessionId]
	if ok {
		session.lease = session.timeout
		return true
	}
	return false
}

func (sm *StateMachine) ResetLeases() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	log.Printf("Resetting leases")
	for _, session := range sm.sessions {
		session.lease = session.timeout
	}
}

func (sm *StateMachine) Elapsed(duration time.Duration) *ExpireRequest {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	expire := &ExpireRequest{}
	for sessionId, session := range sm.sessions {
		if session.lease > duration {
			session.lease -= duration
		} else {
			session.lease = 0
			log.Printf("Session %v is out of time", sessionId)
			expire.Sessions = append(expire.Sessions, sessionId)
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

func (sm *StateMachine) applyConnect(ctx *context, cmdBuf []byte) ConnectResult {
	req := proto.ConnectRequest{}
	err := jute.Decode(cmdBuf, &req)
	if err != nil {
		log.Printf("Error decoding connect command: %v", err)
		return ConnectResult{ErrCode: proto.ErrBadArguments}
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
			return ConnectResult{ErrCode: proto.ErrSessionExpired}
		}
		if !bytes.Equal(session.password, req.Passwd) {
			log.Printf("Bad pasword for session %v", sessionId)
			return ConnectResult{ErrCode: proto.ErrSessionExpired}
		}
		session.connId++
	}
	session.lastCmdId = ctx.cmdId
	session.timeout = time.Millisecond * time.Duration(req.Timeout)
	session.lease = session.timeout
	return ConnectResult{
		Resp: &proto.ConnectResponse{
			ProtocolVersion: 0,
			Timeout:         int32(session.timeout / time.Millisecond),
			SessionID:       sessionId,
			Passwd:          session.password,
		},
		ConnId:  session.connId,
		ErrCode: proto.ErrOk,
	}
}

func (sm *StateMachine) applyExpire(ctx *context, cmdBuf []byte) {
	req := ExpireRequest{}
	err := jute.Decode(cmdBuf, &req)
	if err != nil {
		log.Printf("Error decoding expire list: %v", err)
		return
	}
	for _, sessionId := range req.Sessions {
		delete(sm.sessions, sessionId)
		tree, notify := sm.tree.ExpireSession(ctx.zxid, sessionId)
		sm.tree = tree
		sm.notifyWatches(ctx.zxid, notify)
	}
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

	case proto.OpSetData:
		if req := new(proto.SetDataRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.SetData(ctx, req)
		}
	}

	if errCode != proto.ErrOk {
		log.Printf("%v(%+v) -> %v", opName, req2, errCode.Error())
		return nil, errCode
	}
	log.Printf("%v(%+v) -> %+v, %v notifies", opName, req2, resp, len(notify))
	sm.tree = tree
	sm.notifyWatches(ctx.zxid, notify)

	respBuf, err := jute.Encode(resp)
	if err != nil {
		log.Printf("Could not encode %+v", resp)
		return nil, proto.ErrAPIError
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
		log.Fatalf("Error decoding log entry command header. Crashing.")
	}

	ctx := &context{
		zxid:      proto.ZXID(entry.Index),
		time:      header.Time,
		rand:      header.Rand,
		sessionId: header.SessionId,
		connId:    header.ConnId,
		cmdId:     header.CmdId,
	}

	reply := func(output []byte, errCode proto.ErrCode) {
		fullId := fullCommandId{ctx.sessionId, ctx.connId, ctx.cmdId}
		replyCh, ok := sm.output[fullId]
		if !ok {
			return
		}
		replyCh <- CommandResult{Index: ctx.zxid, Output: output, ErrCode: errCode}
		delete(sm.output, fullId)
	}

	switch header.CmdType {
	case ConnectCommand:
		result := sm.applyConnect(ctx, cmdBuf)
		replyCh, ok := sm.connectOutput[string(ctx.rand)]
		if ok {
			replyCh <- result
			delete(sm.connectOutput, string(ctx.rand))
		}
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
	session proto.SessionId, connection ConnectionId, cmd CommandId) <-chan CommandResult {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	ch := make(chan CommandResult, 1)
	sm.output[fullCommandId{session, connection, cmd}] = ch
	return ch
}

func (sm *StateMachine) ExpectConnect(rand []byte) <-chan ConnectResult {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	ch := make(chan ConnectResult, 1)
	sm.connectOutput[string(rand)] = ch
	return ch
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

type QueryResult struct {
	Zxid    proto.ZXID
	Output  []byte
	ErrCode proto.ErrCode
}

type asyncQuery struct {
	conn      Connection
	lastCmdId CommandId
	opCode    proto.OpCode
	reqBuf    []byte
	respCh    chan<- QueryResult
}

func (sm *StateMachine) Query(
	conn Connection, lastCmdId CommandId,
	opCode proto.OpCode, reqBuf []byte) <-chan QueryResult {
	respCh := make(chan QueryResult, 1)
	query := &asyncQuery{
		conn:      conn,
		lastCmdId: lastCmdId,
		opCode:    opCode,
		reqBuf:    reqBuf,
		respCh:    respCh,
	}
	log.Printf("Created async query %+v", query)

	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.queueQuery(query)
	return respCh
}

func (sm *StateMachine) queueQuery(query *asyncQuery) {
	session, ok := sm.sessions[query.conn.SessionId()]
	if !ok {
		log.Printf("Session %v not found", query.conn.SessionId())
		query.respCh <- QueryResult{Zxid: sm.lastApplied, ErrCode: proto.ErrSessionExpired}
		return
	}

	if query.conn.ConnId() != session.connId {
		log.Printf("Expired connection ID %v for session %v",
			query.conn.ConnId(), query.conn.SessionId())
		query.respCh <- QueryResult{Zxid: sm.lastApplied, ErrCode: proto.ErrSessionExpired}
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
		query.respCh <- QueryResult{Zxid: sm.lastApplied, ErrCode: errCode}
		return
	}
	respBuf, err := jute.Encode(resp)
	if err != nil {
		log.Printf("Could not encode %+v. Closing connection", resp)
		query.conn.Close()
		query.respCh <- QueryResult{Zxid: sm.lastApplied, ErrCode: proto.ErrMarshallingError}
		return
	}

	query.respCh <- QueryResult{Zxid: sm.lastApplied, Output: respBuf, ErrCode: proto.ErrOk}
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return nil, errors.New("Unimplemented")
}

func (sm *StateMachine) Restore(io.ReadCloser) error {
	return errors.New("Unimplemented")
}
