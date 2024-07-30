package blkstorage

import (
	"container/list"
)

const CACHE_SIZE = 10000

type LRUCache struct {
	capacity int
	cache    map[fileLocPointer]*list.Element
	list     *list.List
}

type Entry struct {
	Key   fileLocPointer
	Value []byte
}

func NewLRUCache() *LRUCache {
	return &LRUCache{
		capacity: CACHE_SIZE,
		cache:    make(map[fileLocPointer]*list.Element),
		list:     list.New(),
	}
}

func (c *LRUCache) Get(lp fileLocPointer) ([]byte, bool) {
	if ele, found := c.cache[lp]; found {
		c.list.MoveToFront(ele)
		return ele.Value.(*Entry).Value, true
	}

	return nil, false
}

func (c *LRUCache) Put(lp fileLocPointer, value []byte) {
	if ele, found := c.cache[lp]; found {
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

	entry := &Entry{Key: lp, Value: value}
	ele := c.list.PushFront(entry)
	c.cache[lp] = ele
}
