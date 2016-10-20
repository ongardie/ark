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

type GetDataResponse struct {
	Xid  proto.Xid
	Data []byte
	Stat proto.Stat
}

func (client *Client) GetData(
	path proto.Path,
	watcher func(proto.EventType, proto.Path),
	handler func(GetDataResponse, error)) {
	req := proto.GetDataRequest{
		Path:  path,
		Watch: watcher != nil,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(GetDataResponse{}, err)
		return
	}
	client.Request(proto.OpGetData, reqBuf, &Watcher{
		[]proto.EventType{proto.EventNodeDeleted, proto.EventNodeChildrenChanged},
		watcher,
	}, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(GetDataResponse{},
				fmt.Errorf("Error in GetData(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.GetDataResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(GetDataResponse{}, err)
			return
		}
		handler(GetDataResponse{
			Xid:  reply.Xid,
			Data: resp.Data,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) GetDataSync(
	path proto.Path,
	watcher func(proto.EventType, proto.Path)) (
	GetDataResponse,
	error) {
	type pair struct {
		resp GetDataResponse
		err  error
	}
	ch := make(chan pair)
	client.GetData(path, watcher, func(resp GetDataResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
