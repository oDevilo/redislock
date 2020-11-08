package redislock

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"sync"
	"sync/atomic"
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

	lock := NewReentrantLock(pool, "AAAAA", 6000)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			doSomething(lock, wg)
		}()
	}

	lock2 := NewReentrantLock(pool, "BBBBB", 3000)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			doSomething(lock2, wg)
		}()
	}

	lock3 := NewReentrantLock(pool, "CCCCC", 3000)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			doSomething(lock3, wg)
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
		fmt.Println(strconv.FormatUint(gid, 10) + " 获取到锁 " + lock.name)
	}

	for i := 0; i < 5; i++ {
		if err := lock.Unlock(); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(strconv.FormatUint(gid, 10) + " 释放锁 " + lock.name)
		}
	}
	wg.Done()
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

func TestChannel(t *testing.T) {
	ch := make(chan bool)
	go func() {
		ch <- true
		fmt.Println("do", time.Now())
	}()

	go func() {
		fmt.Println("1 ", <-ch)
	}()

	go func() {
		fmt.Println("2 ", <-ch)
	}()

	go func() {
		time.Sleep(time.Second)
		ch <- true
		fmt.Println("do", time.Now())
	}()

	<-make(chan bool)
}

var wg sync.WaitGroup

func TestWaitGroup(t *testing.T) { // 可以重复使用
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
		}()
	}
	wg.Wait()

	fmt.Println("first wait")

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
		}()
	}
	wg.Wait()

	fmt.Println("second wait")
}

func TestWaitGroup2(t *testing.T) { //
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 2; i++ {
		go func(a int) {
			wg.Wait()
			t.Log(a)
		}(i)
	}
	<-make(chan bool)
}

func TestAtomic(t *testing.T) {
	renwe := int32(0)
	fmt.Println(atomic.CompareAndSwapInt32(&renwe, 0, 0))
	fmt.Println(atomic.LoadInt32(&renwe))
	fmt.Println(atomic.CompareAndSwapInt32(&renwe, 0, 1))
	fmt.Println(atomic.LoadInt32(&renwe))
	fmt.Println(atomic.CompareAndSwapInt32(&renwe, 1, 0))
	fmt.Println(atomic.LoadInt32(&renwe))
}

var mutex sync.Mutex

func TestMutex(t *testing.T) {
	mutex.Lock()

	time.Sleep(time.Second)

	go func() {
		mutex.Unlock()
		fmt.Println(111)
		fmt.Println(111)
		fmt.Println(111)
		fmt.Println(111)
	}()

	go func() {
		mutex.Lock()
		fmt.Println(222)
		fmt.Println(222)
		fmt.Println(222)
		fmt.Println(222)
	}()

	<-make(chan bool)
}
