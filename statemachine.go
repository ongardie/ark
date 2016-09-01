/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"bytes"
	"log"
	"net"
)

type StateMachine struct {
	lastApplied ZXID
	tree        *Tree
	sessions    map[SessionId]*Session
	watches     map[TreeEvent]SessionIds
}

type Context struct {
	zxid      ZXID
	time      int64
	rand      []byte
	sessionId SessionId
}

type SessionIds []SessionId
type Connection struct {
	sessionId SessionId
	netConn   net.Conn
}

type TreeEvent struct {
	path  Path
	which EventType
}

type WatchUpdates struct {
	fire []TreeEvent
	add  []TreeEvent
}

type Session struct {
	password SessionPassword
	conn     *Connection
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		lastApplied: 0,
		tree:        NewTree(),
		sessions:    make(map[SessionId]*Session),
		watches:     make(map[TreeEvent]SessionIds),
	}
}

func (sm *StateMachine) createSession(ctx *Context) (SessionId, SessionPassword) {
	sessionId := SessionId(ctx.zxid)
	if len(ctx.rand) < SessionPasswordLen {
		log.Fatalf("Need %v random bytes for createSession, have %v",
			SessionPasswordLen, len(ctx.rand))
	}
	password := ctx.rand[:SessionPasswordLen]
	sm.sessions[sessionId] = &Session{
		password: password,
	}
	return sessionId, password
}

func (sm *StateMachine) setConn(sessionId SessionId, password SessionPassword, conn *Connection) ErrCode {
	session, ok := sm.sessions[sessionId]
	if !ok {
		log.Printf("Session %v not found", sessionId)
		return errSessionExpired
	}
	if !bytes.Equal(session.password, password) {
		log.Printf("Bad pasword for session %v", sessionId)
		return errSessionExpired
	}
	session.conn = conn
	return errOk
}

func (sm *StateMachine) fireWatch(zxid ZXID, event TreeEvent) {
	log.Printf("Firing %+v", event)
	sessionIds, ok := sm.watches[event]
	if !ok {
		log.Printf("No watches for %+v", event)
		return
	}
	for _, sessionId := range sessionIds {
		session, ok := sm.sessions[sessionId]
		if !ok {
			log.Printf("Expired session %v was watching %+v", sessionId, event)
			continue
		}
		if session.conn == nil {
			log.Printf("Disconnected session %v was watching %+v", sessionId, event)
			continue
		}
		log.Printf("Need to notify session %v of %+v", sessionId, event)
		// TODO: queue TreeEvent on session.conn
	}
	delete(sm.watches, event)
}

func (sm *StateMachine) addWatch(event TreeEvent, sessionId SessionId) {
	sessionIds, ok := sm.watches[event]
	if !ok {
		log.Printf("Registering session %v for %+v", sessionId, event)
		sm.watches[event] = []SessionId{sessionId}
		return
	}
	found := false
	for _, otherId := range sessionIds {
		if otherId == sessionId {
			found = true
			break
		}
	}
	if !found {
		sm.watches[event] = append(sessionIds, sessionId)
	}
}

func (sm *StateMachine) processRequest(ctx *Context, req interface{}) (resp interface{}, errCode ErrCode) {
	var tree *Tree
	watches := WatchUpdates{}

	switch req := req.(type) {
	case *CreateRequest:
		tree, resp, errCode = consensus.stateMachine.tree.Create(ctx, req, &watches)
	case *getChildren2Request:
		resp, errCode = consensus.stateMachine.tree.GetChildren(ctx, req, &watches)
	case *getDataRequest:
		resp, errCode = consensus.stateMachine.tree.GetData(ctx, req, &watches)
	case *SetDataRequest:
		tree, resp, errCode = consensus.stateMachine.tree.SetData(ctx, req, &watches)
	default:
		return nil, errUnimplemented
	}

	if errCode != errOk {
		return nil, errCode
	}
	if tree != nil {
		consensus.stateMachine.tree = tree
	}
	for _, event := range watches.fire {
		sm.fireWatch(ctx.zxid, event)
	}
	for _, event := range watches.add {
		sm.addWatch(event, ctx.sessionId)
	}
	return resp, errOk
}
