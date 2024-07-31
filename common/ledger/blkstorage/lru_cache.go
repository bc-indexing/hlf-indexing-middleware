package blkstorage

import (
	"container/list"
	"sync"
)

const CACHE_SIZE = 100000

type LRUCache struct {
	capacity int
	cache    map[IntPair]*list.Element
	list     *list.List
	mu       sync.Mutex
}

type Entry struct {
	Key   IntPair
	Value *fileLocPointer
}

type IntPair struct {
	First, Second uint64
}

func NewLRUCache() *LRUCache {
	return &LRUCache{
		capacity: CACHE_SIZE,
		cache:    make(map[IntPair]*list.Element),
		list:     list.New(),
	}
}

func (c *LRUCache) Get(blockNum uint64, tranNum uint64) (*fileLocPointer, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	blockTran := IntPair{blockNum, tranNum}
	if ele, found := c.cache[blockTran]; found {
		c.list.MoveToFront(ele)
		return ele.Value.(*Entry).Value, true
	}

	return nil, false
}

func (c *LRUCache) Put(blockNum uint64, tranNum uint64, value *fileLocPointer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	blockTran := IntPair{blockNum, tranNum}
	if ele, found := c.cache[blockTran]; found {
		c.list.MoveToFront(ele)
		ele.Value.(*Entry).Value = value
		return
	}

	if c.list.Len() >= c.capacity {
		back := c.list.Back()
		if back != nil {
			c.list.Remove(back)
			entry := back.Value.(*Entry)
			delete(c.cache, entry.Key)
		}
	}

	entry := &Entry{Key: blockTran, Value: value}
	ele := c.list.PushFront(entry)
	c.cache[blockTran] = ele
}
