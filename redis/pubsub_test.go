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

func TestPubSub(t *testing.T) {
	k := randomString(16)
	v := "pubsubis"

	ch := make(chan PubSubMessage)
	stop := make(chan bool)
	err := rc.Subscribe(k, ch, stop)
	if err != nil {
		t.Fatal(err)
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
			err := rc.Publish(k, v)
			if err != nil {
				t.Error(err)
				return
			}
			counter--
		}
		close(stop)
	}()

	<-kids

	if counter > 0 {
		t.Fatal("Failed to parse PubSub messages ", counter)
	}
}
