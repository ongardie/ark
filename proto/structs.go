/*
Copyright (c) 2016, salesforce.com, inc.
All rights reserved.

Some code comes from go-zookeeper (https://github.com/samuel/go-zookeeper)
and is:

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
*/

package proto

type Path string
type Component string
type ZXID int64
type SessionId int64
type SessionPassword []byte // 16 bytes
type Version int32
type Time int64 // milliseconds form Unix epoch

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
	Ctime          Time      // When this znode was created.
	Mtime          Time      // When this znode was last modified.
	Version        Version   // The number of changes to the data of this znode.
	Cversion       Version   // The number of changes to the children of this znode.
	Aversion       Version   // The number of changes to the ACL of this znode.
	EphemeralOwner SessionId // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
	DataLength     int32     // The length of the data field of this znode.
	NumChildren    int32     // The number of children of this znode.
	Pzxid          ZXID      // last modified children
}

type RequestHeader struct {
	Xid    Xid
	OpCode OpCode
}

type ResponseHeader struct {
	Xid  Xid
	Zxid ZXID
	Err  ErrCode
}

// Note: Apache ZooKeeper does not accept CheckVersion outside of a Multi Op,
// see https://issues.apache.org/jira/browse/ZOOKEEPER-2623.
type CheckVersionRequest struct {
	Path    Path
	Version Version
}

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
	ACL  []ACL
	Mode CreateMode
}

type CreateResponse struct {
	Path Path
}

type Create2Request struct {
	Path Path
	Data []byte
	ACL  []ACL
	Mode CreateMode
}

type Create2Response struct {
	Path Path
	Stat Stat
}

type DeleteRequest struct {
	Path    Path
	Version Version
}

type DeleteResponse struct{}

type ExistsRequest struct {
	Path  Path
	Watch bool
}

type ExistsResponse struct {
	Stat Stat
}

type GetACLRequest struct {
	Path Path
}

type GetACLResponse struct {
	ACL  []ACL
	Stat Stat
}

type GetChildrenRequest struct {
	Path  Path
	Watch bool
}

type GetChildrenResponse struct {
	Children []Component
}

type GetChildren2Request struct {
	Path  Path
	Watch bool
}

type GetChildren2Response struct {
	Children []Component
	Stat     Stat
}

type GetDataRequest struct {
	Path  Path
	Watch bool
}

type GetDataResponse struct {
	Data []byte
	Stat Stat
}

// A MultiRequest is a sequence of (MultiHeader Op)* MultiHeader,
// where Op is (CreateRequest|SetDataRequest|DeleteReqest|CheckVersionRequest)
// and only the final MultiHeader has Done set to true.

// A MultiResponse is a sequence of (MultiHeader Result)* MultiHeader,
// where Result is (MultiErrorResponse|CreateResponse|SetDataResponse|
// DeleteResponse|CheckVersionResponse)
// and only the final MultiHeader has Done set to true.

type MultiHeader struct {
	Type OpCode
	Done bool
	Err  ErrCode
}

type MultiErrorResponse struct {
	Err ErrCode
}

type PingRequest struct{}

type PingResponse struct{}

type SASLRequest struct {
	Token []byte
}

type SASLResponse struct {
	Token []byte
}

type SetACLRequest struct {
	Path    Path
	ACL     []ACL
	Version Version
}

type SetACLResponse struct {
	Stat Stat
}

type SetDataRequest struct {
	Path    Path
	Data    []byte
	Version Version
}

type SetDataResponse struct {
	Stat Stat
}

type SetWatchesRequest struct {
	RelativeZxid ZXID
	DataWatches  []Path
	ExistWatches []Path
	ChildWatches []Path
}

type SetWatchesResponse struct{}

type SyncRequest struct {
	Path Path
}

type SyncResponse struct {
	Path Path
}

type SetAuthRequest struct {
	Type   int32
	Scheme string
	Auth   []byte
}

type SetAuthResponse struct{}

type WatcherEvent struct {
	Type  EventType
	State State
	Path  Path
}

func RequestStructForOp(op OpCode) interface{} {
	switch op {
	case OpCheckVersion:
		return &CheckVersionRequest{}
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
	case OpGetACL:
		return &GetACLRequest{}
	case OpGetChildren:
		return &GetChildrenRequest{}
	case OpGetChildren2:
		return &GetChildren2Request{}
	case OpGetData:
		return &GetDataRequest{}
	case OpPing:
		return &PingRequest{}
	case OpSASL:
		return &SASLRequest{}
	case OpSetACL:
		return &SetACLRequest{}
	case OpSetAuth:
		return &SetAuthRequest{}
	case OpSetData:
		return &SetDataRequest{}
	case OpSetWatches:
		return &SetWatchesRequest{}
	case OpSync:
		return &SyncRequest{}
	}
	return nil
}

func ResponseStructForOp(op OpCode) interface{} {
	switch op {
	case OpCheckVersion:
		return &CheckVersionResponse{}
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
	case OpGetACL:
		return &GetACLResponse{}
	case OpGetChildren:
		return &GetChildrenResponse{}
	case OpGetChildren2:
		return &GetChildren2Response{}
	case OpGetData:
		return &GetDataResponse{}
	case OpPing:
		return &PingResponse{}
	case OpSASL:
		return &SASLResponse{}
	case OpSetACL:
		return &SetACLResponse{}
	case OpSetData:
		return &SetDataResponse{}
	case OpSetWatches:
		return &SetWatchesResponse{}
	case OpSync:
		return &SyncResponse{}
	case OpSetAuth:
		return &SetAuthResponse{}
	}
	return nil
}
