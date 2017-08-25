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

type SetDataResponse struct {
	Zxid proto.ZXID
	Stat proto.Stat
}

func (conn *Conn) SetDataAsync(
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
		return
	}
	conn.RequestAsync(proto.OpSetData, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(SetDataResponse{},
				fmt.Errorf("Error in SetData(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.SetDataResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(SetDataResponse{}, err)
			return
		}
		handler(SetDataResponse{
			Zxid: reply.Zxid,
			Stat: resp.Stat,
		}, nil)
	})
}

func (conn *Conn) SetData(
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
	conn.SetDataAsync(path, data, version, func(resp SetDataResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) SetData(
	path proto.Path,
	data []byte,
	version proto.Version) (
	SetDataResponse,
	error) {
	return client.Conn().SetData(path, data, version)
}
