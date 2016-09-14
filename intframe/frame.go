/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
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
