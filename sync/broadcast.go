/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package sync

type Broadcast struct {
	C         <-chan struct{}
	ch        chan struct{}
	wantClose chan bool
}

func NewBroadcast() Broadcast {
	ch := make(chan struct{})
	b := Broadcast{
		C:         ch,
		ch:        ch,
		wantClose: make(chan bool),
	}
	go b.wait()
	return b
}

func (b *Broadcast) wait() {
	if <-b.wantClose {
		close(b.ch)
	}
}

func (b *Broadcast) Notify() {
	select {
	case b.wantClose <- true:
	default:
	}
}

func (b *Broadcast) Cancel() {
	select {
	case b.wantClose <- false:
	default:
	}
}
