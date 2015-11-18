// Copyright 2013-2015 go-redis authors.  All rights reserved.
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

package redis

import "testing"

func TestServerList(t *testing.T) {
	sl := &ServerList{}
	sl.SetServers("10.0.0.1:6379", "10.0.0.2:6379")
	if !sl.Sharding() {
		t.Fatal("server list of 2 hosts should support sharding")
	}
	si1, _ := sl.PickServer("alice")
	si2, _ := sl.PickServer("bob")
	if si1.Addr == si2.Addr {
		t.Fatal("same server for keys a and b, not sharding")
	}
}
