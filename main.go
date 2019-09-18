package main

import (
	"sync"
	"fmt"
	"time"
	"math/rand"
	"github.com/garyburd/redigo/redis"
)

const (
	ID_TYPE_CIRCLE int32 = 1
)

type IdSetting struct {
	start uint64
	end   uint64
	cache uint32
	rand  bool
}

type IdManager struct {
	sync       sync.Mutex
	mapIdSetting map[int32]*IdSetting
	mapIds       map[int32][]uint64
}

func NewIdManager() *IdManager {
	idm := &IdManager{
		mapIdSetting: make(map[int32]*IdSetting),
		mapIds:       make(map[int32][]uint64),
	}
	return idm
}

func (idm *IdManager) Init() {
	set := &IdSetting{
		start: 10000,
		end:   0x7fffffff,
		cache: 100,
		rand:  true,
	}
	idm.mapIdSetting[ID_TYPE_CIRCLE] = set
}

func (idm *IdManager) GetUUId(idType int32) uint64 {
	var ids []uint64
	idm.sync.Lock()
	if ids, _ = idm.mapIds[idType]; len(ids) == 0 {
		ids = generateUUIds(idType, idm.mapIdSetting[idType])
		if len(ids) == 0 {
			return 0
		}
		idm.mapIds[idType] = ids
	}
	index := 0
	if idm.mapIdSetting[idType].rand {
		index = GetRandom(0, len(ids)-1)
	}
	id := ids[index]
	if index < len(ids)-1 {
		idm.mapIds[idType] = append(ids[:index], ids[index+1:]...)
	} else {
		idm.mapIds[idType] = ids[:index]
	}
	idm.sync.Unlock()
	return id
}

func generateUUIds(idType int32, settings *IdSetting) []uint64 {
	defer func() {
		msg := recover()
		if msg != nil {
			fmt.Errorf("generateUUIds panic: %s", msg)
		}
	}()
	if settings == nil || settings.cache <= 0 {
		return nil
	}
	conn := g_redisPool.Get()
	defer conn.Close()

	reply, err := conn.Do("hincrby", "universal_unique_id", idType, settings.cache)
	if err != nil {
		fmt.Errorf("generateUUIds do hincrby error: %s", err)
		panic(err)
	}
	ret, err := redis.Uint64(reply, err)
	if err != nil {
		fmt.Errorf("generateUUIds get result error: %s", err)
		panic(err)
	}
	ids := make([]uint64, settings.cache)
	ids[len(ids)-1] = (ret-1)%(settings.end-settings.start+1) + settings.start
	for i := len(ids) - 2; i >= 0; i-- {
		ids[i] = ids[i+1] - 1
		if ids[i] < settings.start {
			ids[i] = settings.end
		}
	}
	fmt.Println("generateUUIds", settings, ret, ids)
	return ids
}

func NewRedisPool() *redis.Pool {
	return &redis.Pool{
        MaxIdle:     3,
        IdleTimeout: 300 * time.Second,
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", ":6379")
            if err != nil {
                return nil, fmt.Errorf("redis connection error: %s", err)
            }
            if _, err := c.Do("PING"); err != nil {
				if _, err := c.Do("AUTH", ""); err != nil {
					c.Close()
					return nil, err
				}
			}
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            if err != nil {
                return fmt.Errorf("ping redis error: %s", err)
            }
            return nil
        },
    }
}

func GetRandom(start int, end int) int {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	if start > end {
		return 0
	}
	return start + r.Intn(end-start+1)
}

func main() {
	g_redisPool = NewRedisPool()
	g_idMgr = NewIdManager()
	g_idMgr.Init()
	fmt.Println(g_idMgr.GetUUId(ID_TYPE_CIRCLE))
}

var g_redisPool *redis.Pool
var g_idMgr *IdManager