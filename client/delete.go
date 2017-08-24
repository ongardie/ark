/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package client

import (
	"fmt"

	"salesforce.com/ark/jute"
	"salesforce.com/ark/proto"
)

type DeleteResponse struct {
	Zxid proto.ZXID
}

func (conn *Conn) DeleteAsync(
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
		return
	}
	conn.RequestAsync(proto.OpDelete, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(DeleteResponse{},
				fmt.Errorf("Error in Delete(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.DeleteResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(DeleteResponse{}, err)
			return
		}
		handler(DeleteResponse{
			Zxid: reply.Zxid,
		}, nil)
	})
}

func (conn *Conn) Delete(path proto.Path, version proto.Version) (DeleteResponse, error) {
	type pair struct {
		resp DeleteResponse
		err  error
	}
	ch := make(chan pair)
	conn.DeleteAsync(path, version, func(resp DeleteResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) Delete(path proto.Path, version proto.Version) (DeleteResponse, error) {
	return client.Conn().Delete(path, version)
}
