package redislock

import (
	"github.com/gomodule/redigo/redis"
	"github.com/satori/go.uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

var subConn *redis.PubSubConn
var subConnMutex sync.Mutex
var lockId = uuid.NewV4().String()
var nameChannel sync.Map

// 可重入锁 起码需要3个连接 一个用于请求 一个用于renew 一个用于订阅
type ReentrantLock struct {
	name        string // 锁定的key
	lockId      string // 生成的uuid 相当于机器号
	pool        *redis.Pool
	renew       bool
	waitChannel chan bool // 等待数
	leaseTime   int64
}

func NewReentrantLock(pool *redis.Pool, name string, leaseTime int64) *ReentrantLock {
	// 为每个name 创建其监听
	lock := &ReentrantLock{
		name:        name,
		pool:        pool,
		lockId:      lockId,
		renew:       false,
		waitChannel: make(chan bool),
		leaseTime:   leaseTime,
	}
	// 获取订阅连接
	if subConn == nil {
		subConnMutex.Lock()
		if subConn == nil {
			subConn = &redis.PubSubConn{Conn: pool.Get()}
			go func() {
				for {
					switch res := subConn.Receive().(type) {
					case redis.Message:
						keyName := string(res.Data)
						channel, _ := nameChannel.Load(keyName)
						if channel == nil {
							log.Println(keyName + " 不存在")
							continue
						}
						channel.(chan bool) <- true
					}
				}
			}()
		}
		subConnMutex.Unlock()
	}
	if err := subConn.Subscribe(lock.getChannelName()); err != nil { // todo 已经不用的锁 需要取消订阅 否则订阅越来越多会对redis造成影响
		log.Println("订阅失败 ", err)
	}
	nameChannel.Store(name, make(chan bool))
	return lock
}

// 获取不到锁会一直等待
func (l *ReentrantLock) Lock() error {
	ttl, err := l.tryAcquire()
	if err != nil {
		return err
	}
	if ttl == 0 {
		l.renewLock(l.getGLockId())
		return nil
	}

	for {
		// 等待者 + 1
		l.waitChannel <- true

		channel, _ := nameChannel.Load(l.name)
		<-channel.(chan bool)
		ttl, err := l.tryAcquire()
		if err != nil {
			return err
		}

		if ttl == 0 {
			//l.waitCount--
			l.renewLock(l.getGLockId())
			return nil
		}
	}
}

// 直接返回成功失败 leaseTime 单位 毫秒
func (l *ReentrantLock) TryLock() (bool, error) {
	ttl, err := l.tryAcquire()
	if err != nil {
		return false, err
	}
	if ttl == 0 {
		l.renewLock(l.getGLockId())
		return true, err
	} else {
		return false, err
	}
}

// 定时任务 刷新 如果等待为空
func (l *ReentrantLock) renewLock(gid string) {
	if l.renew { // 已经有更新任务启动了
		return
	}
	l.renew = true // 可以进入这步的表示获取到锁了 而且只有当前协程主动unlock才会改为false 所以没有并发问题

	go func() {
		conn := l.pool.Get()
		defer func() {
			returnConn(conn)
		}()
		for {
			// 等待的时候 有别的获取到锁了
			time.Sleep(time.Duration(l.leaseTime/3) * time.Millisecond)
			result, err := reentrantRenewScript.Do(conn, l.name, l.leaseTime, gid)
			if err != nil {
				log.Println(err)
				return // 怕日志太多 直接返回
			}
			if result.(int64) != 1 { // todo 如果还有等待的 则继续跑 如果没有等待了 更新状态
				l.renew = false
				return
			}
		}
	}()

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
			return 0, err
		}
	}
}

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
	return l.lockId + ":" + strconv.FormatUint(GetGID(), 10)
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
