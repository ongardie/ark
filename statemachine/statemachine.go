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
	mutex       sync.Mutex
	lastApplied proto.ZXID
	tree        *Tree
	sessions    map[proto.SessionId]*Session
	watches     map[TreeEvent]Connections
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
}

type CommandType int32

const (
	NoOpCommand    CommandType = 0
	NormalCommand              = 1
	ConnectCommand             = 2
)

// This is preceded by a 1-byte version number set to 1.
type CommandHeader1 struct {
	CmdType   CommandType
	SessionId proto.SessionId
	ConnId    ConnectionId
	Time      int64
	Rand      []byte
}

type TreeEvent struct {
	Path  proto.Path
	Which proto.EventType
}

type NotifyEvents []TreeEvent
type RegisterEvents []TreeEvent

type ConnectionId int64

type ConnectResult struct {
	Resp   proto.ConnectResponse
	ConnId ConnectionId
}

type Session struct {
	password proto.SessionPassword
	connId   ConnectionId
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		lastApplied: 0,
		tree:        NewTree(),
		sessions:    make(map[proto.SessionId]*Session),
		watches:     make(map[TreeEvent]Connections),
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

func (sm *StateMachine) applyConnect(ctx *context, cmdBuf []byte) (*ConnectResult, proto.ErrCode) {
	req := proto.ConnectRequest{}
	err := jute.Decode(cmdBuf, &req)
	if err != nil {
		log.Printf("Error decoding connect command: %v", err)
		return nil, proto.ErrBadArguments
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
			return nil, proto.ErrSessionExpired
		}
		if !bytes.Equal(session.password, req.Passwd) {
			log.Printf("Bad pasword for session %v", sessionId)
			return nil, proto.ErrSessionExpired
		}
		session.connId++
	}
	return &ConnectResult{
		proto.ConnectResponse{
			ProtocolVersion: 12, // TODO: set this like ZooKeeper does
			TimeOut:         req.TimeOut,
			SessionID:       sessionId,
			Passwd:          session.password,
		},
		session.connId,
	}, proto.ErrOk
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
	case proto.OpCreate:
		if req := new(proto.CreateRequest); decode(req) {
			tree, resp, notify, errCode = sm.tree.Create(ctx, req)
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

func (sm *StateMachine) gateOperation(ctx *context) proto.ErrCode {
	session, ok := sm.sessions[ctx.sessionId]
	if !ok {
		log.Printf("Session %v not found", ctx.sessionId)
		return proto.ErrSessionExpired
	}
	if ctx.connId != session.connId {
		log.Printf("Expired connection ID %v for session %v", ctx.connId, ctx.sessionId)
		return proto.ErrSessionMoved
	}
	return proto.ErrOk
}

func (sm *StateMachine) Apply(entry *raft.Log) interface{} {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	if entry.Type != raft.LogCommand {
		return nil
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
	}

	switch header.CmdType {
	case ConnectCommand:
		result, err := sm.applyConnect(ctx, cmdBuf)
		if err != proto.ErrOk {
			return err
		}
		return result

	case NoOpCommand:
		return sm.gateOperation(ctx)

	case NormalCommand:
		err := sm.gateOperation(ctx)
		if err != proto.ErrOk {
			return err
		}
		result, err := sm.applyCommand(ctx, cmdBuf)
		if err != proto.ErrOk {
			return err
		}
		return result
	}

	return proto.ErrUnimplemented
}

func (sm *StateMachine) Query(
	conn Connection, lastZxid proto.ZXID, opCode proto.OpCode, reqBuf []byte) (
	proto.ZXID, []byte, proto.ErrCode) {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	zxid := sm.lastApplied
	if zxid < lastZxid {
		log.Printf("last applied too old, need to wait")
		return 0, nil, proto.ErrUnimplemented
	}

	ctx := &context{
		zxid:      zxid,
		time:      time.Now().Unix(),
		sessionId: conn.SessionId(),
		connId:    conn.ConnId(),
	}

	errCode := sm.gateOperation(ctx)
	if errCode != proto.ErrOk {
		return 0, nil, errCode
	}

	decode := func(req interface{}) bool {
		err := jute.Decode(reqBuf, req)
		if err != nil {
			log.Printf("Could not decode %v bytes into %T. Closing connection", len(reqBuf), req)
			conn.Close()
			return false
		}
		return true
	}

	var resp interface{}
	var register RegisterEvents
	errCode = proto.ErrBadArguments
	switch opCode {
	case proto.OpGetChildren2:
		if req := new(proto.GetChildren2Request); decode(req) {
			resp, register, errCode = sm.tree.GetChildren(ctx, req)
			log.Printf("GetChildren(%+v) -> %+v", req, resp)
		}
	case proto.OpGetData:
		if req := new(proto.GetDataRequest); decode(req) {
			resp, register, errCode = sm.tree.GetData(ctx, req)
			log.Printf("GetData(%+v) -> %+v", req, resp)
		}
	default:
		return 0, nil, proto.ErrUnimplemented
	}
	if errCode != proto.ErrOk {
		return 0, nil, errCode
	}
	sm.registerWatches(register, conn)
	respBuf, err := jute.Encode(resp)
	if err != nil {
		log.Printf("Could not encode %+v. Closing connection", resp)
		conn.Close()
		return 0, nil, proto.ErrAPIError
	}
	return sm.lastApplied, respBuf, errCode
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return nil, errors.New("Unimplemented")
}

func (sm *StateMachine) Restore(io.ReadCloser) error {
	return errors.New("Unimplemented")
}
