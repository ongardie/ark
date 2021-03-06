/*
Copyright (c) 2016, Salesforce.com, Inc.
All rights reserved.
Licensed under the BSD 3-Clause license.
For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause

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

import (
	"errors"
	"fmt"
)

const (
	SessionPasswordLen = 16
	DefaultPort        = 2181
)

type ErrCode int32

var (
	errConnectionClosed        = errors.New("zk: connection closed")
	errUnknown                 = errors.New("zk: unknown error")
	errAPIError                = errors.New("zk: api error")
	errNoNode                  = errors.New("zk: node does not exist")
	errNoAuth                  = errors.New("zk: not authenticated")
	errBadVersion              = errors.New("zk: version conflict")
	errNoChildrenForEphemerals = errors.New("zk: ephemeral nodes may not have children")
	errNodeExists              = errors.New("zk: node already exists")
	errNotEmpty                = errors.New("zk: node has children")
	errSessionExpired          = errors.New("zk: session has been expired by the server")
	errInvalidACL              = errors.New("zk: invalid ACL specified")
	errAuthFailed              = errors.New("zk: client authentication failed")
	errClosing                 = errors.New("zk: zookeeper is closing")
	errNothing                 = errors.New("zk: no server responses to process")
	errSessionMoved            = errors.New("zk: session moved to another server, so operation is ignored")

	// ErrInvalidCallback         = errors.New("zk: invalid callback specified")
	errCodeToError = map[ErrCode]error{
		ErrOk:                      nil,
		ErrAPIError:                errAPIError,
		ErrNoNode:                  errNoNode,
		ErrNoAuth:                  errNoAuth,
		ErrBadVersion:              errBadVersion,
		ErrNoChildrenForEphemerals: errNoChildrenForEphemerals,
		ErrNodeExists:              errNodeExists,
		ErrNotEmpty:                errNotEmpty,
		ErrSessionExpired:          errSessionExpired,
		// ErrInvalidCallback:         errInvalidCallback,
		ErrInvalidACL:   errInvalidACL,
		ErrAuthFailed:   errAuthFailed,
		ErrClosing:      errClosing,
		ErrNothing:      errNothing,
		ErrSessionMoved: errSessionMoved,
	}
)

func (e ErrCode) Error() error {
	if err, ok := errCodeToError[e]; ok {
		return err
	}
	return fmt.Errorf("zk: unknown error (%v)", e)
	//return errUnknown
}

const (
	ErrOk ErrCode = 0
	// System and server-side errors
	ErrSystemError          ErrCode = -1
	ErrRuntimeInconsistency ErrCode = -2
	ErrDataInconsistency    ErrCode = -3
	ErrConnectionLoss       ErrCode = -4
	ErrMarshallingError     ErrCode = -5
	ErrUnimplemented        ErrCode = -6
	ErrOperationTimeout     ErrCode = -7
	ErrBadArguments         ErrCode = -8
	ErrInvalidState         ErrCode = -9
	// API errors
	ErrAPIError                ErrCode = -100
	ErrNoNode                  ErrCode = -101 // *
	ErrNoAuth                  ErrCode = -102
	ErrBadVersion              ErrCode = -103 // *
	ErrNoChildrenForEphemerals ErrCode = -108
	ErrNodeExists              ErrCode = -110 // *
	ErrNotEmpty                ErrCode = -111
	ErrSessionExpired          ErrCode = -112
	ErrInvalidCallback         ErrCode = -113
	ErrInvalidACL              ErrCode = -114
	ErrAuthFailed              ErrCode = -115
	ErrClosing                 ErrCode = -116
	ErrNothing                 ErrCode = -117
	ErrSessionMoved            ErrCode = -118
)

type EventType int32

const (
	EventNodeCreated         EventType = 1
	EventNodeDeleted         EventType = 2
	EventNodeDataChanged     EventType = 3
	EventNodeChildrenChanged EventType = 4

	EventSession     EventType = -1
	EventNotWatching EventType = -2
)

var (
	eventNames = map[EventType]string{
		EventNodeCreated:         "EventNodeCreated",
		EventNodeDeleted:         "EventNodeDeleted",
		EventNodeDataChanged:     "EventNodeDataChanged",
		EventNodeChildrenChanged: "EventNodeChildrenChanged",
		EventSession:             "EventSession",
		EventNotWatching:         "EventNotWatching",
	}
)

func (t EventType) String() string {
	if name := eventNames[t]; name != "" {
		return name
	}
	return "Unknown"
}

type CreateMode int32

const (
	ModeDefault             CreateMode = ModePersistent
	ModePersistent          CreateMode = 0
	ModeEphemeral           CreateMode = 1
	ModeSequential          CreateMode = 2
	ModeEphemeralSequential CreateMode = 3
	ModeContainer           CreateMode = 4
)

type Permission int32

// Constants for ACL permissions
const (
	PermRead Permission = 1 << iota
	PermWrite
	PermCreate
	PermDelete
	PermAdmin
	PermAll Permission = 0x1f
)

type OpCode int32

const (
	OpNotify          OpCode = 0
	OpCreate                 = 1
	OpDelete                 = 2
	OpExists                 = 3
	OpGetData                = 4
	OpSetData                = 5
	OpGetACL                 = 6
	OpSetACL                 = 7
	OpGetChildren            = 8
	OpSync                   = 9
	OpPing                   = 11
	OpGetChildren2           = 12
	OpCheckVersion           = 13
	OpMulti                  = 14
	OpCreate2                = 15
	OpReconfig               = 16
	OpCheckWatches           = 17
	OpRemoveWatches          = 18
	OpCreateContainer        = 19
	OpDeleteContainer        = 20
	OpCreateTTL              = 21
	OpClose                  = -11
	OpSetAuth                = 100
	OpSetWatches             = 101
	OpSASL                   = 102
	OpError                  = -1
)

var (
	OpNames = map[OpCode]string{
		OpNotify:          "notify",
		OpCreate:          "create",
		OpDelete:          "delete",
		OpExists:          "exists",
		OpGetData:         "getData",
		OpSetData:         "setData",
		OpGetACL:          "getACL",
		OpSetACL:          "setACL",
		OpGetChildren:     "getChildren",
		OpSync:            "sync",
		OpPing:            "ping",
		OpGetChildren2:    "getChildren2",
		OpCheckVersion:    "checkVersion",
		OpMulti:           "multi",
		OpCreate2:         "create2",
		OpReconfig:        "reconfig",
		OpCheckWatches:    "checkWatches",
		OpRemoveWatches:   "removeWatches",
		OpCreateContainer: "createContainer",
		OpDeleteContainer: "deleteContainer",
		OpCreateTTL:       "createTTL",
		OpClose:           "close",
		OpSetAuth:         "setAuth",
		OpSetWatches:      "setWatches",
	}
)

const (
	StateUnknown           State = -1
	StateDisconnected      State = 0
	StateConnecting        State = 1
	StateConnected         State = 3
	StateAuthFailed        State = 4
	StateConnectedReadOnly State = 5
	StateSaslAuthenticated State = 6
	StateExpired           State = -112
)

var (
	stateNames = map[State]string{
		StateUnknown:           "StateUnknown",
		StateDisconnected:      "StateDisconnected",
		StateConnectedReadOnly: "StateConnectedReadOnly",
		StateSaslAuthenticated: "StateSaslAuthenticated",
		StateExpired:           "StateExpired",
		StateAuthFailed:        "StateAuthFailed",
		StateConnecting:        "StateConnecting",
		StateConnected:         "StateConnected",
	}
)

type State int32

func (s State) String() string {
	if name := stateNames[s]; name != "" {
		return name
	}
	return "Unknown"
}

type Xid int32

const (
	XidWatcherEvent Xid = -1
	XidPing         Xid = -2
	XidAuth         Xid = -4
	XidSetWatches   Xid = -8
)
