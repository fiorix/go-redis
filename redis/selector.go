// Copyright 2013 Alexandre Fiori
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// This is a modified version of gomemcache adapted to redis.
// Original code and license at https://github.com/bradfitz/gomemcache/

package redis

import (
	"hash/crc32"
	"net"
	"strings"
	"sync"
)

// ServerSelector is the interface that selects a redis server as a function
// of the item's key.
//
// All ServerSelector implementations must be threadsafe.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto, or the first listed server if an
	// empty key is given.
	PickServer(key string) (net.Addr, error)

	// Sharding returns true if the client can connect to multiple
	// different servers. e.g.: 10.0.0.1:6379, 10.0.0.1:6380, 10.0.0.2:6379
	Sharding() bool
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	lk       sync.RWMutex
	addrs    []net.Addr
	sharding bool
}

// SetServers changes a ServerList's set of servers at runtime and is
// threadsafe.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) (err error) {
	var fs, addr net.Addr
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		if strings.Contains(server, "/") {
			addr, err = net.ResolveUnixAddr("unix", server)
		} else {
			addr, err = net.ResolveTCPAddr("tcp", server)
		}
		if err != nil {
			return err
		} else {
			naddr[i] = addr
		}
		if i == 0 {
			fs = addr
		} else if fs != addr && !ss.sharding {
			ss.sharding = true
		}
	}

	ss.lk.Lock()
	defer ss.lk.Unlock()
	ss.addrs = naddr
	return nil
}

func (ss *ServerList) Sharding() bool {
	return ss.sharding
}

func (ss *ServerList) PickServer(key string) (addr net.Addr, err error) {
	ss.lk.RLock()
	defer ss.lk.RUnlock()
	if len(ss.addrs) == 0 {
		err = ErrNoServers
		return
	}
	if key == "" {
		addr = ss.addrs[0]
	} else {
		addr = ss.addrs[crc32.ChecksumIEEE([]byte(key))%uint32(len(ss.addrs))]
	}
	return
}
