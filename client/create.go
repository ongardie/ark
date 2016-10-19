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

type CreateResponse struct {
	Xid  proto.Xid
	Zxid proto.ZXID
	Path proto.Path
	Stat proto.Stat
}

func (client *Client) Create(
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
	}
	client.Request(proto.OpCreate2, reqBuf, nil, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(CreateResponse{},
				fmt.Errorf("Error in Create(%v): %v", path, reply.Err.Error()))
		}
		var resp proto.Create2Response
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(CreateResponse{}, err)
		}
		handler(CreateResponse{
			Xid:  reply.Xid,
			Zxid: reply.Zxid,
			Path: resp.Path,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) CreateSync(
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
	client.Create(path, data, acl, mode, func(resp CreateResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
