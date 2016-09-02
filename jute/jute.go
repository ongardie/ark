/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package jute

import (
	"fmt"
	"log"
)

func Encode(msg interface{}) ([]byte, error) {
	buf, err := encode(msg)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func DecodeSome(buf []byte, msg interface{}) ([]byte, error) {
	n, err := decodePacket(buf, msg)
	if err != nil {
		return buf, err
	}
	buf = buf[n:]
	log.Printf("DecodePacket returned %v, so %v bytes left", n, len(buf))
	return buf, nil
}

func Decode(buf []byte, msg interface{}) error {
	more, err := DecodeSome(buf, msg)
	if err != nil {
		return err
	}
	if len(more) > 0 {
		return fmt.Errorf("Unexpected bytes after reading %T", msg)
	}
	return nil
}

func encode(msg interface{}) ([]byte, error) {
	bufSize := 1024
	for {
		buf := make([]byte, bufSize)
		n, err := encodePacket(buf, msg)
		if err == nil {
			return buf[:n], nil
		}
		if err == errShortBuffer {
			log.Printf("buffer size of %v too small, doubling", bufSize)
			bufSize *= 2
			continue
		}
		return nil, err
	}
}
