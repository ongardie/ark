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
	emptyContainers   map[proto.Path]emptyContainer
}

type emptyContainer struct {
	start time.Time
	czxid proto.ZXID
	pzxid proto.ZXID
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
	Identity() []proto.Identity
}

type Connections []Connection

type context struct {
	zxid      proto.ZXID
	term      uint64
	time      proto.Time
	rand      []byte
	server    uint64
	sessionId proto.SessionId
	connId    ConnectionId
	cmdId     CommandId
	identity  []proto.Identity
}

type CommandType int32

const (
	NoOpCommand             CommandType = 0
	NormalCommand                       = 1
	ConnectCommand                      = 2
	ExpireSessionsCommand               = 3
	ExpireContainersCommand             = 4
)

// This is preceded by a 1-byte version number set to 1.
type CommandHeader1 struct {
	CmdType   CommandType
	Server    uint64
	SessionId proto.SessionId
	ConnId    ConnectionId
	CmdId     CommandId
	Time      proto.Time
	Rand      []byte
	Identity  []proto.Identity
}

type ExpireSessionsRequest struct {
	Term     uint64
	Sessions []ExpireSessionsEntry
}
type ExpireSessionsEntry struct {
	Session  proto.SessionId
	LastConn ConnectionId
}

type ExpireContainersRequest struct {
	Containers []ExpireContainersEntry
}
type ExpireContainersEntry struct {
	Path  proto.Path
	Czxid proto.ZXID
	Pzxid proto.ZXID
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
		emptyContainers:   make(map[proto.Path]emptyContainer),
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
	serverContact func(uint64) time.Time) *ExpireSessionsRequest {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	expire := &ExpireSessionsRequest{Term: term}
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
					ExpireSessionsEntry{Session: sessionId, LastConn: session.connId})
			}
		}
	}
	if len(expire.Sessions) > 0 {
		return expire
	} else {
		return nil
	}
}

func (sm *StateMachine) ExpiredContainers() *ExpireContainersRequest {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	expire := &ExpireContainersRequest{}
	now := time.Now()
	for path, container := range sm.emptyContainers {
		if now.After(container.start.Add(time.Minute)) {
			empty, czxid, pzxid := sm.tree.IsEmptyContainer(path)
			if empty {
				if czxid == container.czxid && pzxid == container.pzxid {
					expire.Containers = append(expire.Containers, ExpireContainersEntry{
						Path:  path,
						Czxid: container.czxid,
						Pzxid: container.pzxid,
					})
				}
			} else {
				delete(sm.emptyContainers, path)
			}
		}
	}
	if len(expire.Containers) > 0 {
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

	// Check if the last child of a container node got removed.
	if event.Which == proto.EventNodeChildrenChanged {
		empty, czxid, pzxid := sm.tree.IsEmptyContainer(event.Path)
		if empty {
			log.Printf("Container %v is now empty", event.Path)
			sm.emptyContainers[event.Path] = emptyContainer{
				start: time.Now(),
				czxid: czxid,
				pzxid: pzxid,
			}
		}
	}
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

func (sm *StateMachine) applyExpireSession(ctx *context, cmdBuf []byte) {
	req := ExpireSessionsRequest{}
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

func (sm *StateMachine) applyExpireContainer(ctx *context, cmdBuf []byte) {
	req := ExpireContainersRequest{}
	err := jute.Decode(cmdBuf, &req)
	if err != nil {
		log.Printf("Error decoding expire list: %v", err)
		return
	}
	for _, expire := range req.Containers {
		container, ok := sm.emptyContainers[expire.Path]
		if ok && container.pzxid <= expire.Pzxid {
			delete(sm.emptyContainers, expire.Path)
		}
		tree, notify := sm.tree.ExpireContainer(ctx.zxid, expire.Path, expire.Czxid, expire.Pzxid)
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

	case proto.OpCreate2, proto.OpCreateContainer:
		if req := new(proto.Create2Request); decode(req) {
			tree, resp, notify, errCode = sm.tree.Create2(ctx, req)
		}

	case proto.OpDelete:
		if req := new(proto.DeleteRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.Delete(ctx, req)
		}

	case proto.OpMulti:
		tree, respBuf, notify, errCode = applyMulti(ctx, sm.tree, reqBuf)

	case proto.OpSetACL:
		if req := new(proto.SetACLRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.SetACL(ctx, req)
		}

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
		identity:  header.Identity,
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

	case ExpireSessionsCommand:
		sm.applyExpireSession(ctx, cmdBuf)
		return
	case ExpireContainersCommand:
		sm.applyExpireContainer(ctx, cmdBuf)
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
	sessionId proto.SessionId, connId ConnectionId, cmdId CommandId,
	fn CommandResultFn) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.output[fullCommandId{sessionId, connId, cmdId}] = fn

	// commands act as signs of life too
	session, ok := sm.sessions[sessionId]
	if ok {
		session.lastContact = time.Now()
	}
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
		time:      proto.Time(time.Now().UnixNano() / 1e6),
		sessionId: query.conn.SessionId(),
		connId:    query.conn.ConnId(),
		cmdId:     0,
		identity:  query.conn.Identity(),
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

	case proto.OpGetACL:
		if req := new(proto.GetACLRequest); decode(req) {
			resp, register, errCode = sm.tree.GetACL(ctx, req)
			log.Printf("GetACL(%+v) -> %+v", req, resp)
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
