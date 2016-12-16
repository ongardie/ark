/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import (
	"net"

	"salesforce.com/ark/proto"
)

func ipMatches(ident proto.Identity, pattern proto.Identity) bool {
	if ident.Scheme != "ip" || pattern.Scheme != "ip" {
		return false
	}
	_, network, err := net.ParseCIDR(pattern.ID)
	if err != nil {
		return false
	}
	ip := net.ParseIP(ident.ID)
	if ip == nil {
		return false
	}
	return network.Contains(ip)
}

func identityMatches(ident proto.Identity, pattern proto.Identity) bool {
	return ident == pattern || ipMatches(ident, pattern)
}

func checkACL(identities []proto.Identity, perm proto.Permission, acl []proto.ACL) proto.ErrCode {
	for _, entry := range acl {
		if (int32(entry.Perms) & int32(perm)) == 0 {
			continue
		}
		if identityMatches(proto.Identity{Scheme: "world", ID: "anyone"}, entry.Identity) {
			return proto.ErrOk
		}
		for _, ident := range identities {
			if identityMatches(ident, entry.Identity) {
				return proto.ErrOk
			}
		}
	}
	if len(identities) == 0 {
		return proto.ErrNoAuth
	}
	return proto.ErrAuthFailed
}
