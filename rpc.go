/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"salesforce.com/zoolater/proto"
	"salesforce.com/zoolater/statemachine"
)

type RPCish interface {
	IsRPCish()
}

func (*ConnectRPC) IsRPCish() {}
func (*RPC) IsRPCish()        {}

type ConnectRPC struct {
	conn     statemachine.Connection
	reqJute  []byte
	req      *proto.ConnectRequest
	errReply func(proto.ErrCode)
	reply    func(*proto.ConnectResponse, statemachine.ConnectionId)
}

type RPC struct {
	conn           statemachine.Connection
	cmdId          statemachine.CommandId
	lastCmdId      statemachine.CommandId
	reqHeaderJute  []byte
	reqHeader      proto.RequestHeader
	opName         string
	req            []byte
	errReply       func(proto.ZXID, proto.ErrCode)
	reply          func(proto.ZXID, []byte)
	replyThenClose func(proto.ZXID, []byte)
}
