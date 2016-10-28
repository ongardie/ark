/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package client

import (
	"fmt"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
)

type Create1Response struct {
	Xid  proto.Xid
	Zxid proto.ZXID
	Path proto.Path
}

type CreateResponse struct {
	Xid  proto.Xid
	Zxid proto.ZXID
	Path proto.Path
	Stat proto.Stat
}

func (conn *Conn) CreateAsync(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode,
	handler func(CreateResponse, error)) {
	req := proto.Create2Request{
		Path: path,
		Data: data,
		ACL:  acl,
		Mode: mode,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(CreateResponse{}, err)
		return
	}
	conn.RequestAsync(proto.OpCreate2, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(CreateResponse{},
				fmt.Errorf("Error in Create(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.Create2Response
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(CreateResponse{}, err)
			return
		}
		handler(CreateResponse{
			Xid:  reply.Xid,
			Zxid: reply.Zxid,
			Path: resp.Path,
			Stat: resp.Stat,
		}, nil)
	})
}

func (conn *Conn) Create(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode) (
	CreateResponse,
	error) {
	type pair struct {
		resp CreateResponse
		err  error
	}
	ch := make(chan pair)
	conn.CreateAsync(path, data, acl, mode, func(resp CreateResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) Create(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode) (
	CreateResponse,
	error) {
	return client.Conn().Create(path, data, acl, mode)
}
