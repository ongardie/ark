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

type GetACLResponse struct {
	Xid  proto.Xid
	ACL  []proto.ACL
	Stat proto.Stat
}

func (client *Client) GetACL(
	path proto.Path,
	handler func(GetACLResponse, error)) {
	req := proto.GetACLRequest{
		Path: path,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(GetACLResponse{}, err)
		return
	}
	client.Request(proto.OpGetACL, reqBuf, nil, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(GetACLResponse{},
				fmt.Errorf("Error in GetACL(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.GetACLResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(GetACLResponse{}, err)
			return
		}
		handler(GetACLResponse{
			Xid:  reply.Xid,
			ACL:  resp.ACL,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) GetACLSync(
	path proto.Path) (
	GetACLResponse,
	error) {
	type pair struct {
		resp GetACLResponse
		err  error
	}
	ch := make(chan pair)
	client.GetACL(path, func(resp GetACLResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
