/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package intframe

import (
	"encoding/binary"
	"io"
	"net"
)

func Send(conn net.Conn, msg []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	_, err := conn.Write(buf)
	return err
}

func Receive(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 4)
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	bytes := binary.BigEndian.Uint32(buf[:n])
	buf = make([]byte, bytes)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
