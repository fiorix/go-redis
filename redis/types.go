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
	"errors"
	"fmt"
	"strconv"
)

var ErrInvalidType = errors.New("Invalid type for convertion")

// vstr2iface converts an array of strings to an array of empty interfaces
func vstr2iface(a []string) (r []interface{}) {
	r = make([]interface{}, len(a))
	for n, item := range a {
		r[n] = item
	}
	return
}

// iface2vstr converts an interface to an array of strings
func iface2vstr(a interface{}) []string {
	r := []string{}
	switch a.(type) {
	case []interface{}:
		for _, item := range a.([]interface{}) {
			switch item.(type) {
			case string:
				r = append(r, item.(string))
			}
		}
	}
	return r
}

// iface2strmap converts an interface to map of strings
func iface2strmap(a interface{}) map[string]string {
	tmp := iface2vstr(a)
	m := make(map[string]string)
	for n := 0; n < len(tmp)/2; n++ {
		m[tmp[n*2]] = tmp[(n*2)+1]
	}
	return m
}

// iface2bool validates and converts interface (int) to bool
func iface2bool(a interface{}) (bool, error) {
	switch a.(type) {
	case int:
		if a.(int) == 1 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, ErrInvalidType
}

// iface2int validates and converts interface to int
func iface2int(a interface{}) (int, error) {
	switch a.(type) {
	case int:
		return a.(int), nil
	}
	return 0, ErrInvalidType
}

// iface2str validates and converts interface to string
func iface2str(a interface{}) (string, error) {
	switch a.(type) {
	case string:
		return a.(string), nil
	}
	return "", ErrInvalidType
}

// autoconv_args converts commands' arguments from multiple types to string,
// so they can be sent to the server. e.g. rc.IncrBy("k", 1) -> "k", "1"
func autoconv_args(a []interface{}) []string {
	s := make([]string, len(a))
	for n, item := range a {
		switch item.(type) {
		case int:
			s[n] = strconv.Itoa(item.(int))
		case string:
			s[n] = item.(string)
		default:
			// TODO: use iface2n, maybe
			panic(fmt.Sprintf("Unsupported argument type for convertion: %#v", item))
		}
	}
	return s
}
