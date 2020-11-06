package redislock

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"sync"
	"testing"
	"time"
)

var pool *redis.Pool
var pool2 *redis.Pool
var started bool

func start() {
	if started {
		return
	}
	pool = &redis.Pool{
		MaxIdle:     3,
		MaxActive:   5,
		IdleTimeout: 120 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	pool2 = &redis.Pool{
		MaxIdle:     3,
		MaxActive:   5,
		IdleTimeout: 120 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379")
		},
	}
	started = true
}

func TestConcurrent(t *testing.T) {
	var wg = &sync.WaitGroup{}
	start()

	lock := NewReentrantLock(pool, "test", 6000)

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			doSomething(lock, wg)
		}()
	}
	wg.Wait()
}

func doSomething(lock *ReentrantLock, wg *sync.WaitGroup) {
	gid := GetGID()
	for i := 0; i < 5; i++ {
		err := lock.Lock()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(strconv.FormatUint(gid, 10) + " 获取到锁")
	}

	for i := 0; i < 5; i++ {
		if err := lock.Unlock(); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(strconv.FormatUint(gid, 10) + " 释放锁")
		}
	}
	fmt.Println(pool.IdleCount())
	wg.Done()
}

func TestPubsub(t *testing.T) {
	start()

	conn := pool.Get()
	t.Log(&conn)

	subConn := redis.PubSubConn{conn}
	if err := subConn.Subscribe("tt"); err != nil {
		t.Log(err)
		return
	}

	if err := subConn.Subscribe("xx"); err != nil {
		t.Log(err)
		return
	}

	go func() {
		for i := 0; i < 5; i++ {
			switch res := subConn.Receive().(type) {
			case redis.Message:
				fmt.Println(res.Channel, string(res.Data))
			case redis.Subscription:
				fmt.Printf("%s: %s %d\n", res.Channel, res.Kind, res.Count)
			case error:
				fmt.Println(res)
			}
			fmt.Println(i)
		}
		//if err := subConn.Unsubscribe("tt"); err != nil {
		//	t.Log(err)
		//	return
		//}

		subConn.Close()

		//conn = pool.Get()
		//t.Log(&conn)
		//
		//conn.Send("SET", "name", "red")
		//conn.Send("SET", "t2", "red")
		//conn.Send("SET", "name", "red")
		//conn.Send("SET", "name", "red")
		//conn.Flush()

		fmt.Println(1111)
	}()

	if err := subConn.Subscribe("tt"); err != nil {
		t.Log(err)
		return
	}

	fmt.Println("do")

	<-make(chan bool)
}

func TestBaseLock(t *testing.T) {
	var wg = &sync.WaitGroup{}
	start()

	lock := NewBaseLock(pool, "test", 3000)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			doSomethingBase(lock, wg)
		}()
	}
	wg.Wait()
}

// 不可重入
func doSomethingBase(lock *BaseLock, wg *sync.WaitGroup) {
	gid := GetGID()

	err := lock.Lock()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(strconv.FormatUint(gid, 10) + " 获取到锁")
	if err := lock.Unlock(); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(strconv.FormatUint(gid, 10) + " 释放锁")
	}
	fmt.Println(pool.IdleCount())
	wg.Done()

}
