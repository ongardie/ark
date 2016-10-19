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

type SetACLResponse struct {
	Xid  proto.Xid
	Zxid proto.ZXID
	Path proto.Path
	Stat proto.Stat
}

func (client *Client) SetACL(
	path proto.Path,
	acl []proto.ACL,
	version proto.Version,
	handler func(SetACLResponse, error)) {
	req := proto.SetACLRequest{
		Path:    path,
		ACL:     acl,
		Version: version,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(SetACLResponse{}, err)
	}
	client.Request(proto.OpSetACL, reqBuf, nil, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(SetACLResponse{},
				fmt.Errorf("Error in SetACL(%v): %v", path, reply.Err.Error()))
		}
		var resp proto.SetACLResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(SetACLResponse{}, err)
		}
		handler(SetACLResponse{
			Xid:  reply.Xid,
			Zxid: reply.Zxid,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) SetACLSync(
	path proto.Path,
	acl []proto.ACL,
	version proto.Version) (
	SetACLResponse,
	error) {
	type pair struct {
		resp SetACLResponse
		err  error
	}
	ch := make(chan pair)
	client.SetACL(path, acl, version, func(resp SetACLResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}