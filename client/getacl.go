/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package client

import (
	"fmt"

	"salesforce.com/ark/jute"
	"salesforce.com/ark/proto"
)

type GetACLResponse struct {
	ACL  []proto.ACL
	Stat proto.Stat
}

func (conn *Conn) GetACLAsync(path proto.Path, handler func(GetACLResponse, error)) {
	req := proto.GetACLRequest{
		Path: path,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(GetACLResponse{}, err)
		return
	}
	conn.RequestAsync(proto.OpGetACL, reqBuf, func(reply Reply) {
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
			ACL:  resp.ACL,
			Stat: resp.Stat,
		}, nil)
	})
}

func (conn *Conn) GetACL(path proto.Path) (GetACLResponse, error) {
	type pair struct {
		resp GetACLResponse
		err  error
	}
	ch := make(chan pair)
	conn.GetACLAsync(path, func(resp GetACLResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) GetACL(path proto.Path) (GetACLResponse, error) {
	return client.Conn().GetACL(path)
}
