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
	Perms    Permission
	Identity Identity
}

type Identity struct {
	Scheme string
	ID     string
}

type Stat struct {
	Czxid          ZXID      // The zxid of the change that caused this znode to be created.
	Mzxid          ZXID      // The zxid of the change that last modified this znode.
	Ctime          int64     // The time in milliseconds from epoch when this znode was created.
	Mtime          int64     // The time in milliseconds from epoch when this znode was last modified.
	Version        int32     // The number of changes to the data of this znode.
	Cversion       int32     // The number of changes to the children of this znode.
	Aversion       int32     // The number of changes to the ACL of this znode.
	EphemeralOwner SessionId // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
	DataLength     int32     // The length of the data field of this znode.
	NumChildren    int32     // The number of children of this znode.
	Pzxid          ZXID      // last modified children
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

type MultiHeader struct {
	Type OpCode
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
type CheckVersionResponse struct{}

type CloseRequest struct{}
type CloseResponse struct{}

type ConnectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    ZXID
	Timeout         int32
	SessionID       SessionId
	Passwd          SessionPassword
}

type ConnectResponse struct {
	ProtocolVersion int32
	Timeout         int32
	SessionID       SessionId
	Passwd          SessionPassword
}

type CreateRequest struct {
	Path Path
	Data []byte
	Acl  []ACL
	Mode CreateMode
}

type CreateResponse pathResponse

type Create2Request CreateRequest

type Create2Response struct {
	Path Path
	Stat Stat
}

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

type GetChildrenRequest pathWatchRequest

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
	DataWatches  []Path
	ExistWatches []Path
	ChildWatches []Path
}

type SetWatchesResponse struct{}

type SyncRequest pathRequest
type SyncResponse pathResponse

type SetAuthRequest auth
type SetAuthResponse struct{}

// A MultiRequest is a sequence of (MultiHeader Op)* MultiHeader,
// where Op is (CreateRequest|SetDataRequest|DeleteReqest|CheckVersionRequest)
// and only the final MultiHeader has Done set to true.

// A MultiResponse is a sequence of (MultiHeader Result)* MultiHeader,
// where Result is (MultiErrorResponse|CreateResponse|SetDataResponse|
// DeleteResponse|CheckVersionResponse)
// and only the final MultiHeader has Done set to true.

type MultiErrorResponse struct {
	Err ErrCode
}

type WatcherEvent struct {
	Type  EventType
	State State
	Path  Path
}

func RequestStructForOp(op OpCode) interface{} {
	switch op {
	case OpClose:
		return &CloseRequest{}
	case OpCreate:
		return &CreateRequest{}
	case OpCreate2, OpCreateContainer:
		return &Create2Request{}
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
	}
	return nil
}

func ResponseStructForOp(op OpCode) interface{} {
	switch op {
	case OpClose:
		return &CloseResponse{}
	case OpCreate:
		return &CreateResponse{}
	case OpCreate2, OpCreateContainer:
		return &Create2Response{}
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
	case OpCheck:
		return &CheckVersionResponse{}
	}
	return nil
}
