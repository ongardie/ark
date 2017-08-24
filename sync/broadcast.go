/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package sync

import gosync "sync"

type Broadcast struct {
	C  <-chan struct{}
	ch chan struct{}
	// This used to be implemented with a goroutine instead of a mutex.
	// However, that risked leaking the goroutine if no Notify() ever happened.
	// The mutex avoid the leak and is simpler, though perhaps less elegant.
	mutex  gosync.Mutex
	closed bool
}

func NewBroadcast() *Broadcast {
	ch := make(chan struct{})
	return &Broadcast{
		C:  ch,
		ch: ch,
	}
}

func (b *Broadcast) Notify() {
	b.mutex.Lock()
	if !b.closed {
		close(b.ch)
		b.closed = true
	}
	b.mutex.Unlock()
}
