package redislock

import (
	"github.com/gomodule/redigo/redis"
	"github.com/satori/go.uuid"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var subConn *redis.PubSubConn
var subConnMutex sync.Mutex
var lockId = uuid.NewV4().String() // 每个机器一个就行
var renewMap sync.Map              // 进行renwe的锁 key 锁名 value gid
var lockEntryMap sync.Map

type Entry struct {
	waitCount int32
	channel   chan bool
}

// 可重入锁 起码需要3个连接 一个用于请求 一个用于renew 一个用于订阅
type ReentrantLock struct {
	name      string // 锁定的key
	pool      *redis.Pool
	leaseTime int64
}

func NewReentrantLock(pool *redis.Pool, name string, leaseTime int64) *ReentrantLock {
	// 为每个name 创建其监听
	lock := &ReentrantLock{
		name:      name,
		pool:      pool,
		leaseTime: leaseTime,
	}
	// 获取订阅连接
	if subConn == nil {
		subConnMutex.Lock()
		if subConn == nil { // 双重检测
			subConn = &redis.PubSubConn{Conn: pool.Get()}
			go func() {
				for {
					switch res := subConn.Receive().(type) {
					case redis.Message:
						keyName := string(res.Data)
						if load, ok := lockEntryMap.Load(keyName); ok {
							entry := load.(*Entry)
							entry.channel <- true
						}
					}
				}
			}()
		}
		subConnMutex.Unlock()
	}
	return lock
}

// 获取不到锁会一直等待
func (l *ReentrantLock) Lock() error {
	if locked, err := l.TryLock(); err != nil {
		return err
	} else if locked {
		return nil
	}

	// 失败的 去订阅通道 等释放锁的通知
	if err := l.subscribe(); err != nil {
		return err
	}

	if err := l.waitLock(); err != nil {
		return err
	} else {
		l.unsubscribe()
		return nil
	}
}

func (l *ReentrantLock) waitLock() error {
	for {
		if locked, err := l.TryLock(); err != nil {
			return err
		} else if locked {
			return nil
		}
		load, _ := lockEntryMap.Load(l.name)
		entry := load.(*Entry)
		<-entry.channel
	}
}

func (l *ReentrantLock) subscribe() error {
	if load, ok := lockEntryMap.Load(l.name); ok {
		entry := load.(*Entry)
		atomic.AddInt32(&entry.waitCount, 1)
		entry.channel <- true
		return nil
	}

	entry := Entry{waitCount: 0, channel: make(chan bool, 10)}

	if load, ok := lockEntryMap.LoadOrStore(l.name, &entry); ok {
		entry := load.(*Entry)
		atomic.AddInt32(&entry.waitCount, 1)
		entry.channel <- true
		return nil
	} else {
		if err := subConn.Subscribe(l.getChannelName()); err != nil {
			log.Println("订阅失败 ", err)
			return err
		}
		return nil
	}
}

func (l *ReentrantLock) unsubscribe() {
	load, _ := lockEntryMap.Load(l.name)
	entry := load.(*Entry)
	if atomic.LoadInt32(&entry.waitCount) == 0 {
		lockEntryMap.Delete(l.name)
		if err := subConn.Unsubscribe(l.getChannelName()); err != nil {
			log.Println("取消订阅失败 ", err)
		}
	} else {
		entry.channel <- true
	}
}

// 直接返回成功失败 leaseTime 单位 毫秒
func (l *ReentrantLock) TryLock() (bool, error) {
	if ttl, err := l.tryAcquire(); err != nil {
		return false, err
	} else if ttl == 0 {
		return true, nil
	}
	return false, nil
}

// 定时任务 只有获取到分布式锁的才会进入这个方法 刷新 name和gid对应的过期时间
func (l *ReentrantLock) renewLockStart(gid string) {
	if _, ok := renewMap.Load(l.name); ok {
		return
	}

	timer := time.NewTimer(time.Duration(l.leaseTime/3) * time.Millisecond)

	go func() {
		select {
		case <-timer.C:
			conn := l.pool.Get()
			renewMap.LoadAndDelete(l.name)
			result, err := reentrantRenewScript.Do(conn, l.name, l.leaseTime, gid)
			if err != nil {
				log.Println(err)
				return // 怕日志太多 直接返回
			}
			if err = returnConn(conn); err != nil {
				log.Println(err)
				return // 怕日志太多 直接返回
			}
			if result.(int64) == 1 { // 更新成功 重新执行
				l.renewLockStart(gid)
			}
		}
	}()

	if _, loaded := renewMap.LoadOrStore(l.name, gid); loaded {
		timer.Stop()
	}

}

// k1 = name arg1 过期时间 arg2 = gid
var reentrantRenewScript = redis.NewScript(1, `
	if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
		redis.call('pexpire', KEYS[1], ARGV[1])
		return 1
	end
	return 0
`)

// ttl < 0 key 不存在  ttl = 0 获取锁成功 ttl > 0 key存在
func (l *ReentrantLock) tryAcquire() (int, error) {
	conn := l.pool.Get()
	defer func() {
		returnConn(conn)
	}()

	ttl, err := reentrantLockScript.Do(conn, l.name, l.leaseTime, l.getGLockId())
	if err != nil {
		return 1, err
	} else {
		if ttl != nil {
			return int(ttl.(int64)), err
		} else {
			l.renewLockStart(l.getGLockId())
			return 0, err
		}
	}
}

// keys[1] name argv[1] leaseTime argv[2] id
var reentrantLockScript = redis.NewScript(1, `
	if (redis.call('exists', KEYS[1]) == 0) then 
		redis.call('hset', KEYS[1], ARGV[2], 1)
		redis.call('pexpire', KEYS[1], ARGV[1])
		return nil
	end
	if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
		redis.call('hincrby', KEYS[1], ARGV[2], 1)
		redis.call('pexpire', KEYS[1], ARGV[1])
		return nil
	end
	return redis.call('pttl', KEYS[1])
`)

func (l *ReentrantLock) getGLockId() string {
	return lockId + ":" + strconv.FormatUint(GetGID(), 10)
}

func (l *ReentrantLock) Unlock() error {
	conn := l.pool.Get()
	defer func() {
		returnConn(conn)
	}()

	_, err := reentrantUnlockScript.Do(conn, l.name, l.getChannelName(), l.leaseTime, l.getGLockId())
	if err != nil {
		return err
	}
	return nil
}

// redislock 还做了订阅发布 这里就省略了
// nil 表示没获取到锁的线程调用的 0 表示线程还持有锁 1 表示解锁成功
// k1=lock_name k2=channel_name arg1=expire arg2=gid
var reentrantUnlockScript = redis.NewScript(2, `
	if (redis.call('exists', KEYS[1]) == 0) then
		redis.call('publish', KEYS[2], ARGV[3])
		return 1
	end
	if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then
		return nil
	end
	local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)
	if (counter > 0) then
		redis.call('pexpire', KEYS[1], ARGV[1])
		return 0
	else
		redis.call('del', KEYS[1])
		redis.call('publish', KEYS[2], KEYS[1])
		return 1
	end
`)

func (l *ReentrantLock) getChannelName() string {
	return "count_down_latch_channel_" + l.name
}
