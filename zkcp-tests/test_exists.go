/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package main

import (
	"strings"
	"time"

	"github.com/ongardie/ark/proto"
)

func (t *Test) TestZKCP_Exists_watchPath() {
	client := makeClient(t)
	defer client.Close()

	notified := 0
	_, err := client.Exists("/x/y", func(proto.EventType) {
		notified++
	})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected does not exist error, got: %v", err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}
	_, err = client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}
	_, err = client.Create("/x/y", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified != 1 {
		t.Errorf("Expected 1 notification before create response, got %v",
			notified)
	}
}

func (t *Test) TestZKCP_Exists_watchRestore() {
	client := makeClient(t)
	defer client.Close()

	notified := 0
	_, err := client.Exists("/x/y", func(proto.EventType) {
		notified++
	})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected does not exist error, got: %v", err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}

	// Cause the server to close the connection
	client.Conn().RequestSync(proto.OpError, nil)
	time.Sleep(time.Second) // TODO: eliminate timing

	_, err = client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}
	_, err = client.Create("/x/y", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified != 1 {
		t.Errorf("Expected 1 notification before create response, got %v",
			notified)
	}
}
