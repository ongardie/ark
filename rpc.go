/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

type RPC interface {
	errReply(ErrCode)
}

type baseRPC struct {
	conn       Connection
	sessionId  SessionId
	errReply_  func(ErrCode)
	reqHeader  requestHeader
	respHeader responseHeader
}

type ConnectRPC struct {
	conn      Connection
	errReply_ func(ErrCode)
	req       *connectRequest
	reply     func(*connectResponse)
}

func (rpc *ConnectRPC) errReply(errCode ErrCode) { rpc.errReply_(errCode) }

type PingRPC struct {
	baseRPC
	reply func()
}

func (rpc *PingRPC) errReply(errCode ErrCode) { rpc.errReply_(errCode) }

type CreateRPC struct {
	baseRPC
	req   *CreateRequest
	reply func(*createResponse)
}

func (rpc *CreateRPC) errReply(errCode ErrCode) { rpc.errReply_(errCode) }

type GetChildrenRPC struct {
	baseRPC
	req   *getChildren2Request
	reply func(*getChildren2Response)
}

func (rpc *GetChildrenRPC) errReply(errCode ErrCode) { rpc.errReply_(errCode) }

type GetDataRPC struct {
	baseRPC
	req   *getDataRequest
	reply func(*getDataResponse)
}

func (rpc *GetDataRPC) errReply(errCode ErrCode) { rpc.errReply_(errCode) }

type SetDataRPC struct {
	baseRPC
	req   *SetDataRequest
	reply func(*setDataResponse)
}

func (rpc *SetDataRPC) errReply(errCode ErrCode) { rpc.errReply_(errCode) }
