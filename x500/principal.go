/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package x500

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"strings"
)

// Principal attempts to match the behavior of Java's
// javax.security.auth.x500.X500Principal.getName(RFC2253).
func Principal(names *pkix.Name) (string, error) {
	values := make([]string, 0, len(names.Names))
	for _, name := range names.Names {
		value, ok := name.Value.(string)
		if !ok {
			return "", fmt.Errorf("x500.Principal encountered unknown value type %T", name.Value)
		}
		kv := fmt.Sprintf("%v=%v", rfc2253Label(name.Type), escape(value))
		values = append(values, kv)
	}
	values = reversed(values)
	return strings.Join(values, ","), nil
}

func rfc2253Label(oid asn1.ObjectIdentifier) string {
	dotted := oid.String()
	switch dotted {
	case "2.5.4.3":
		return "CN" // common name
	case "2.5.4.6":
		return "C" // country name
	case "2.5.4.7":
		return "L" // locality name
	case "2.5.4.8":
		return "ST" // state or province name
	case "2.5.4.9":
		return "STREET" // street address
	case "2.5.4.10":
		return "O" // organization name
	case "2.5.4.11":
		return "OU" // organizational unit name
	case "0.9.2342.19200300.100.1.25":
		return "DC" // domain component
	case "0.9.2342.19200300.100.1.1":
		return "UID" // userid
	default:
		return dotted
	}
}

func escape(value string) string {
	valueBytes := []byte(value)
	buf := make([]byte, 0, len(valueBytes))
	beginning := true
	beginSpaces := len(valueBytes)
	if len(valueBytes) > 0 {
		i := len(valueBytes) - 1
		for {
			if valueBytes[i] == ' ' {
				beginSpaces = i
			} else {
				break
			}
			if i == 0 {
				break
			}
			i -= 1
		}
	}
	for i, b := range valueBytes {
		if b == ' ' && i >= beginSpaces {
			buf = append(buf, '\\', b)
			continue
		}
		if b == ' ' || b == '#' {
			if beginning {
				buf = append(buf, '\\', b)
				continue
			}
		} else {
			beginning = false
		}
		if shouldEscape(b) {
			buf = append(buf, '\\', b)
		} else {
			buf = append(buf, b)
		}
	}
	return string(buf)
}

func shouldEscape(char byte) bool {
	switch char {
	case ',':
		return true
	case '+':
		return true
	case '"':
		return true
	case '\\':
		return true
	case '<':
		return true
	case '>':
		return true
	case ';':
		return true
	default:
		return false
	}
}

func reversed(list []string) []string {
	if len(list) == 0 {
		return nil
	}
	rev := make([]string, 0, len(list))
	i := len(list) - 1
	for {
		rev = append(rev, list[i])
		if i == 0 {
			break
		}
		i -= 1
	}
	return rev
}
