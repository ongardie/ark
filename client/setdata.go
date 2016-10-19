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

type SetDataResponse struct {
	Xid  proto.Xid
	Zxid proto.ZXID
	Path proto.Path
	Stat proto.Stat
}

func (client *Client) SetData(
	path proto.Path,
	data []byte,
	version proto.Version,
	handler func(SetDataResponse, error)) {
	req := proto.SetDataRequest{
		Path:    path,
		Data:    data,
		Version: version,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(SetDataResponse{}, err)
	}
	client.Request(proto.OpSetData, reqBuf, nil, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(SetDataResponse{},
				fmt.Errorf("Error in SetData(%v): %v", path, reply.Err.Error()))
		}
		var resp proto.SetDataResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(SetDataResponse{}, err)
		}
		handler(SetDataResponse{
			Xid:  reply.Xid,
			Zxid: reply.Zxid,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) SetDataSync(
	path proto.Path,
	data []byte,
	version proto.Version) (
	SetDataResponse,
	error) {
	type pair struct {
		resp SetDataResponse
		err  error
	}
	ch := make(chan pair)
	client.SetData(path, data, version, func(resp SetDataResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
