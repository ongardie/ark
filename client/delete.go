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

type DeleteResponse struct {
	Xid  proto.Xid
	Zxid proto.ZXID
}

func (client *Client) Delete(
	path proto.Path,
	version proto.Version,
	handler func(DeleteResponse, error)) {
	req := proto.DeleteRequest{
		Path:    path,
		Version: version,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(DeleteResponse{}, err)
	}
	client.Request(proto.OpDelete, reqBuf, nil, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(DeleteResponse{},
				fmt.Errorf("Error in Delete(%v): %v", path, reply.Err.Error()))
		}
		var resp proto.DeleteResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(DeleteResponse{}, err)
		}
		handler(DeleteResponse{
			Xid:  reply.Xid,
			Zxid: reply.Zxid,
		}, nil)
	})
}

func (client *Client) DeleteSync(
	path proto.Path,
	version proto.Version) (
	DeleteResponse,
	error) {
	type pair struct {
		resp DeleteResponse
		err  error
	}
	ch := make(chan pair)
	client.Delete(path, version, func(resp DeleteResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
