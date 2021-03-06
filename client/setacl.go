/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package client

import (
	"fmt"

	"github.com/ongardie/ark/jute"
	"github.com/ongardie/ark/proto"
)

type SetACLResponse struct {
	Zxid proto.ZXID
	Path proto.Path
	Stat proto.Stat
}

func (conn *Conn) SetACLAsync(
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
		return
	}
	conn.RequestAsync(proto.OpSetACL, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(SetACLResponse{},
				fmt.Errorf("Error in SetACL(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.SetACLResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(SetACLResponse{}, err)
			return
		}
		handler(SetACLResponse{
			Zxid: reply.Zxid,
			Stat: resp.Stat,
		}, nil)
	})
}

func (conn *Conn) SetACL(
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
	conn.SetACLAsync(path, acl, version, func(resp SetACLResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) SetACL(
	path proto.Path,
	acl []proto.ACL,
	version proto.Version) (
	SetACLResponse,
	error) {
	return client.Conn().SetACL(path, acl, version)
}
