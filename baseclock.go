package redislock

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/satori/go.uuid"
	"strconv"
	"time"
)

// 不可重入
type BaseLock struct {
	name      string // 锁定的key
	pool      *redis.Pool
	leaseTime int
	lockId    string // 生成的uuid 用于区分机器
}

func NewBaseLock(pool *redis.Pool, name string, leaseTime int) *BaseLock {
	lockId := uuid.NewV4().String()
	return &BaseLock{
		name:      name,
		pool:      pool,
		lockId:    lockId,
		leaseTime: leaseTime,
	}
}

// 获取不到锁会一直等待
func (l *BaseLock) Lock() error {
	// 循环等待方案 直到获取锁 todo 等待超时
	for {
		locked, err := l.tryAcquire()
		if err != nil {
			return err
		}
		if locked {
			return nil
		}
		time.Sleep(time.Duration(l.leaseTime/4) * time.Millisecond)
	}
}

// 直接返回成功失败 leaseTime 单位 毫秒
func (l *BaseLock) TryLock() (bool, error) {
	locked, err := l.tryAcquire()
	if err != nil {
		return false, err
	}
	if locked {
		return true, err
	} else {
		return false, err
	}
}

// ttl < 0 key 不存在  ttl = 0 获取锁成功 ttl > 0 key存在
func (l *BaseLock) tryAcquire() (bool, error) {
	//t := time.Now()
	//fmt.Println(util.GetGID(), " 闲置连接:", l.pool.IdleCount())
	conn := l.pool.Get()
	//fmt.Println(util.GetGID(), " 获取连接:", time.Since(t), " 闲置连接:", l.pool.IdleCount())
	defer func() {
		returnConn(conn)
		//fmt.Println(util.GetGID(), " 释放后 闲置连接:", l.pool.IdleCount())
	}()

	result, err := baseLockScript.Do(conn, l.name, l.getGLockId(), l.leaseTime)
	if err != nil {
		return false, err
	}
	switch result.(type) {
	case int64:
		if int(result.(int64)) > 0 {
			return true, err
		} else {
			return false, err
		}
	case int32:
		if int(result.(int64)) > 0 {
			return true, err
		} else {
			return false, err
		}
	default:
		return false, err
	}
}

// 判断 key 是否存在 如果存在 则返回 0 如果不存在 则上锁
var baseLockScript = redis.NewScript(1, `
	if (redis.call('exists', KEYS[1]) == 0) then 
		redis.call('set', KEYS[1], ARGV[1])
		redis.call('pexpire', KEYS[1], ARGV[2])
		return 1
	end
	return 0
`)

func (l *BaseLock) Unlock() error {
	conn := l.pool.Get()
	defer func() {
		returnConn(conn)
	}()

	unlock, err := baseUnlockScript.Do(conn, l.name, l.getGLockId())
	if err != nil {
		return err
	}
	switch unlock.(type) {
	case int64:
		if int(unlock.(int64)) == 1 {
			return nil
		} else {
			return nil
		}
	case int32:
		if int(unlock.(int32)) == 1 {
			return nil
		} else {
			return nil
		}
	default:
		return errors.New(fmt.Sprintf("未知返回值: %+v", unlock))
	}
}

// 成功返回 1 失败 返回 0
// 判断 key 是否存在 不存在 返回 0 存在 判断 v 是否相等 不想等返回 0 相等 删除 key 返回 1
var baseUnlockScript = redis.NewScript(1, `
	if (redis.call('exists', KEYS[1]) == 0) then
		return 0
	end
	if (redis.call('get', KEYS[1]) == ARGV[1]) then
		redis.call('del', KEYS[1])
		return 1
	end
	return 0
`)

func (l *BaseLock) getGLockId() string {
	return l.lockId + ":" + strconv.FormatUint(GetGID(), 10)
}
