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

type Create2Response struct {
	Zxid proto.ZXID
	Path proto.Path
	Stat proto.Stat
}

func (conn *Conn) Create2Async(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode,
	handler func(Create2Response, error)) {
	req := proto.Create2Request{
		Path: path,
		Data: data,
		ACL:  acl,
		Mode: mode,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(Create2Response{}, err)
		return
	}
	conn.RequestAsync(proto.OpCreate2, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(Create2Response{},
				fmt.Errorf("Error in Create(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.Create2Response
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(Create2Response{}, err)
			return
		}
		handler(Create2Response{
			Zxid: reply.Zxid,
			Path: resp.Path,
			Stat: resp.Stat,
		}, nil)
	})
}

func (conn *Conn) Create2(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode) (
	Create2Response,
	error) {
	type pair struct {
		resp Create2Response
		err  error
	}
	ch := make(chan pair)
	conn.Create2Async(path, data, acl, mode, func(resp Create2Response, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) Create2(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode) (
	Create2Response,
	error) {
	return client.Conn().Create2(path, data, acl, mode)
}
