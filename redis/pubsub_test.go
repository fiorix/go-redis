// Copyright 2013-2014 go-redis authors.  All rights reserved.
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

import (
	"math/rand"
	"testing"
	"time"
)

// TODO: sort tests by dependency (set first, etc)

// rc is the redis client handler used for all tests.
// Make sure redis-server is running before starting the tests.
var rcPubSub *Client

func init() {
	rcPubSub = New("127.0.0.1:6379")
	rand.Seed(time.Now().UTC().UnixNano())
}

// Tests

func TestPubSub(t *testing.T) {
	k := randomString(16)
	v := "pubsubis"

	ch := make(chan PubSubMessage)
	stop := make(chan bool)

	err := rcPubSub.Subscribe(k, ch, stop)
	if err != nil {
		t.Error(err)
		return
	}

	kids := make(chan bool)
	counter := 100
	go func() {
		run := true
		for run {
			select {
			case vv := <-ch:
				//fmt.Println(vv.Channel, vv.Value, vv.Error)
				if vv.Error != nil {
					run = false
				}
			}
		}
		kids <- true
	}()

	go func() {
		for i := counter; i > 0; i-- {
			err := rcPubSub.Publish(k, v)
			if err != nil {
				t.Error(err)
				return
			}
			counter--
		}
		stop <- true
	}()

	<-kids

	if counter > 0 {
		t.Error("Failed to parse PubSub messages ", counter)
		return
	}
}
