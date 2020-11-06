package redislock

import (
	"bytes"
	"github.com/gomodule/redigo/redis"
	"log"
	"runtime"
	"strconv"
)

func returnConn(conn redis.Conn) error {
	if err := conn.Close(); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

// 获取携程号
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
