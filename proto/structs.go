/*
This file is copied from go-zookeeper with minor modifications:

Copyright (c) 2013, Samuel Stauffer <samuel@descolada.com>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of the author nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


The Salesforce modifications are:
Copyright (c) 2016, salesforce.com, inc.
All rights reserved.
*/

package proto

import (
	"log"
	"time"
)

// Diego added this
type Path string
type Component string
type ZXID int64
type SessionId int64
type SessionPassword []byte // 16 bytes

const SessionPasswordLen = 16

// end

type defaultLogger struct{}

func (defaultLogger) Printf(format string, a ...interface{}) {
	log.Printf(format, a...)
}

type ACL struct {
	Perms  int32
	Scheme string
	ID     string
}

type Stat struct {
	Czxid          ZXID  // The zxid of the change that caused this znode to be created.
	Mzxid          ZXID  // The zxid of the change that last modified this znode.
	Ctime          int64 // The time in milliseconds from epoch when this znode was created.
	Mtime          int64 // The time in milliseconds from epoch when this znode was last modified.
	Version        int32 // The number of changes to the data of this znode.
	Cversion       int32 // The number of changes to the children of this znode.
	Aversion       int32 // The number of changes to the ACL of this znode.
	EphemeralOwner int64 // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
	DataLength     int32 // The length of the data field of this znode.
	NumChildren    int32 // The number of children of this znode.
	Pzxid          ZXID  // last modified children
}

// ServerClient is the information for a single Zookeeper client and its session.
// This is used to parse/extract the output fo the `cons` command.
type ServerClient struct {
	Queued        int64
	Received      int64
	Sent          int64
	SessionID     SessionId
	Lcxid         ZXID
	Lzxid         ZXID
	Timeout       int32
	LastLatency   int32
	MinLatency    int32
	AvgLatency    int32
	MaxLatency    int32
	Established   time.Time
	LastResponse  time.Time
	Addr          string
	LastOperation string // maybe?
	Error         error
}

// ServerClients is a struct for the FLWCons() function. It's used to provide
// the list of Clients.
//
// This is needed because FLWCons() takes multiple servers.
type ServerClients struct {
	Clients []*ServerClient
	Error   error
}

// ServerStats is the information pulled from the Zookeeper `stat` command.
type ServerStats struct {
	Sent        int64
	Received    int64
	NodeCount   int64
	MinLatency  int64
	AvgLatency  int64
	MaxLatency  int64
	Connections int64
	Outstanding int64
	Epoch       int32
	Counter     int32
	BuildTime   time.Time
	Mode        Mode
	Version     string
	Error       error
}

type RequestHeader struct {
	Xid    int32
	OpCode OpCode
}

type ResponseHeader struct {
	Xid  int32
	Zxid ZXID
	Err  ErrCode
}

type multiHeader struct {
	Type int32
	Done bool
	Err  ErrCode
}

type auth struct {
	Type   int32
	Scheme string
	Auth   []byte
}

// Generic request structs

type pathRequest struct {
	Path Path
}

type PathVersionRequest struct {
	Path    Path
	Version int32
}

type pathWatchRequest struct {
	Path  Path
	Watch bool
}

type pathResponse struct {
	Path Path
}

type statResponse struct {
	Stat Stat
}

//

type CheckVersionRequest PathVersionRequest
type closeRequest struct{}
type closeResponse struct{}

type ConnectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    ZXID
	TimeOut         int32
	SessionID       SessionId
	Passwd          SessionPassword
}

type ConnectResponse struct {
	ProtocolVersion int32
	TimeOut         int32
	SessionID       SessionId
	Passwd          SessionPassword
}

type CreateRequest struct {
	Path  Path
	Data  []byte
	Acl   []ACL
	Flags int32
}

type CreateResponse pathResponse
type DeleteRequest PathVersionRequest
type DeleteResponse struct{}

type errorResponse struct {
	Err int32
}

type ExistsRequest pathWatchRequest
type ExistsResponse statResponse
type GetAclRequest pathRequest

type GetAclResponse struct {
	Acl  []ACL
	Stat Stat
}

type GetChildrenRequest pathRequest

type GetChildrenResponse struct {
	Children []Component
}

type GetChildren2Request pathWatchRequest

type GetChildren2Response struct {
	Children []Component
	Stat     Stat
}

type GetDataRequest pathWatchRequest

type GetDataResponse struct {
	Data []byte
	Stat Stat
}

type getMaxChildrenRequest pathRequest

type getMaxChildrenResponse struct {
	Max int32
}

type getSaslRequest struct {
	Token []byte
}

type PingRequest struct{}
type PingResponse struct{}

type SetAclRequest struct {
	Path    Path
	Acl     []ACL
	Version int32
}

type SetAclResponse statResponse

type SetDataRequest struct {
	Path    Path
	Data    []byte
	Version int32
}

type SetDataResponse statResponse

type setMaxChildren struct {
	Path Path
	Max  int32
}

type setSaslRequest struct {
	Token string
}

type setSaslResponse struct {
	Token string
}

type SetWatchesRequest struct {
	RelativeZxid ZXID
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type SetWatchesResponse struct{}

type SyncRequest pathRequest
type SyncResponse pathResponse

type SetAuthRequest auth
type SetAuthResponse struct{}

type multiRequestOp struct {
	Header multiHeader
	Op     interface{}
}
type MultiRequest struct {
	Ops        []multiRequestOp
	DoneHeader multiHeader
}
type multiResponseOp struct {
	Header multiHeader
	String string
	Stat   *Stat
	Err    ErrCode
}
type MultiResponse struct {
	Ops        []multiResponseOp
	DoneHeader multiHeader
}

/*
func (r *multiRequest) Encode(buf []byte) (int, error) {
	total := 0
	for _, op := range r.Ops {
		op.Header.Done = false
		n, err := encodePacketValue(buf[total:], reflect.ValueOf(op))
		if err != nil {
			return total, err
		}
		total += n
	}
	r.DoneHeader.Done = true
	n, err := encodePacketValue(buf[total:], reflect.ValueOf(r.DoneHeader))
	if err != nil {
		return total, err
	}
	total += n

	return total, nil
}

func (r *multiRequest) Decode(buf []byte) (int, error) {
	r.Ops = make([]multiRequestOp, 0)
	r.DoneHeader = multiHeader{-1, true, -1}
	total := 0
	for {
		header := &multiHeader{}
		n, err := decodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		req := requestStructForOp(header.Type)
		if req == nil {
			return total, ErrAPIError
		}
		n, err = decodePacketValue(buf[total:], reflect.ValueOf(req))
		if err != nil {
			return total, err
		}
		total += n
		r.Ops = append(r.Ops, multiRequestOp{*header, req})
	}
	return total, nil
}

func (r *multiResponse) Decode(buf []byte) (int, error) {
	var multiErr error

	r.Ops = make([]multiResponseOp, 0)
	r.DoneHeader = multiHeader{-1, true, -1}
	total := 0
	for {
		header := &multiHeader{}
		n, err := decodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		res := multiResponseOp{Header: *header}
		var w reflect.Value
		switch header.Type {
		default:
			return total, ErrAPIError
		case opError:
			w = reflect.ValueOf(&res.Err)
		case opCreate:
			w = reflect.ValueOf(&res.String)
		case opSetData:
			res.Stat = new(Stat)
			w = reflect.ValueOf(res.Stat)
		case opCheck, opDelete:
		}
		if w.IsValid() {
			n, err := decodePacketValue(buf[total:], w)
			if err != nil {
				return total, err
			}
			total += n
		}
		r.Ops = append(r.Ops, res)
		if multiErr == nil && res.Err != errOk {
			// Use the first error as the error returned from Multi().
			multiErr = res.Err.toError()
		}
	}
	return total, multiErr
}
*/

type WatcherEvent struct {
	Type  EventType
	State State
	Path  Path
}

func RequestStructForOp(op OpCode) interface{} {
	switch op {
	case OpClose:
		return &closeRequest{}
	case OpCreate:
		return &CreateRequest{}
	case OpDelete:
		return &DeleteRequest{}
	case OpExists:
		return &ExistsRequest{}
	case OpGetAcl:
		return &GetAclRequest{}
	case OpGetChildren:
		return &GetChildrenRequest{}
	case OpGetChildren2:
		return &GetChildren2Request{}
	case OpGetData:
		return &GetDataRequest{}
	case OpPing:
		return &PingRequest{}
	case OpSetAcl:
		return &SetAclRequest{}
	case OpSetData:
		return &SetDataRequest{}
	case OpSetWatches:
		return &SetWatchesRequest{}
	case OpSync:
		return &SyncRequest{}
	case OpSetAuth:
		return &SetAuthRequest{}
	case OpCheck:
		return &CheckVersionRequest{}
	case OpMulti:
		return &MultiRequest{}
	}
	return nil
}

func ResponseStructForOp(op OpCode) interface{} {
	switch op {
	case OpClose:
		return &closeResponse{}
	case OpCreate:
		return &CreateResponse{}
	case OpDelete:
		return &DeleteResponse{}
	case OpExists:
		return &ExistsResponse{}
	case OpGetAcl:
		return &GetAclResponse{}
	case OpGetChildren:
		return &GetChildrenResponse{}
	case OpGetChildren2:
		return &GetChildren2Response{}
	case OpGetData:
		return &GetDataResponse{}
	case OpPing:
		return &PingResponse{}
	case OpSetAcl:
		return &SetAclResponse{}
	case OpSetData:
		return &SetDataResponse{}
	case OpSetWatches:
		return &SetWatchesResponse{}
	case OpSync:
		return &SyncResponse{}
	case OpSetAuth:
		return &SetAuthResponse{}
	//case OpCheck:
	//return &CheckVersionResponse{}
	case OpMulti:
		return &MultiResponse{}
	}
	return nil
}
