go-redis
========

[![Build Status](https://secure.travis-ci.org/fiorix/go-redis.png)](http://travis-ci.org/fiorix/go-redis)

*For the latest source code, see <https://github.com/fiorix/go-redis>*


_go-redis_ is a [Redis](http://redis.io) client library for the
[Go](http://golang.org) programming language. It's built on the skeleton of
[gomemcache](http://github.com/bradfitz/gomemcache).

Licensed under the Apache License, Version 2.0.


*THIS IS A WORK IN PROGRESS, USE AT YOUR OWN RISK*


## Installing

Use _go get_ to install:

	$ go get github.com/fiorix/go-redis/redis


## Usage

Hello world:

	import "github.com/fiorix/go-redis"

	func main() {
		rc := redis.New("10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379")
		rc.Set("foo", "bar")

		v, err := rc.Get("foo")
		...
	}

When connected to multiple servers, commands such as PING, INFO and
similar are only executed on the first server. GET, SET and others are
distributed by their key.

See [commands.go](https://github.com/fiorix/go-redis/blob/master/redis/commands.go)
for a list of current supported commands. (myself, @gleicon and others are
currently working on it; contributors are welcome)

New connections are created on demand, and stay available in the connection
pool until they time out.


### Unix socket, dbid and password support

The client supports ip:port or unix socket for connecting to redis.

	rc := redis.New("/tmp/redis.sock db=5 passwd=foobared")

Database ID and password can only be set by ``New()`` and can't be
changed later. If that is required, make a new connection.


## Credits

Thanks to (in no particular order):

- [gomemcache](https://github.com/bradfitz/gomemcache): for the skeleton of
this client library.
- [txredisapi](https://github.com/fiorix/txredisapi): for the experience in
writing redis client libraries over the years.
