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
	sessions    map[SessionId]Session
	watches     map[TreeEvent]SessionIds
}

type Context struct {
	zxid ZXID
	time int64
	rand []byte
}

type SessionIds []SessionId
type Connection struct {
	netConn net.Conn
}

type TreeEvent struct {
	zxid  ZXID
	path  Path
	which EventType
}

type Session struct {
	password SessionPassword
	conn     *Connection
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		lastApplied: 0,
		tree:        NewTree(),
		sessions:    make(map[SessionId]Session),
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
	sm.sessions[sessionId] = Session{
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
