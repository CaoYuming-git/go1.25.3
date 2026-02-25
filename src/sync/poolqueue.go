// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync

import (
	"sync/atomic"
	"unsafe"
)

// poolDequeue is a lock-free fixed-size single-producer,
// multi-consumer queue. The single producer can both push and pop
// from the head, and consumers can pop from the tail.
//
// It has the added feature that it nils out unused slots to avoid
// unnecessary retention of objects. This is important for sync.Pool,
// but not typically a property considered in the literature.
// 双端队列，poolChain链表节点中的底层存储结构
type poolDequeue struct {
	// headTail packs together a 32-bit head index and a 32-bit
	// tail index. Both are indexes into vals modulo len(vals)-1.
	//
	// tail = index of oldest data in queue
	// head = index of next slot to fill
	//
	// Slots in the range [tail, head) are owned by consumers.
	// A consumer continues to own a slot outside this range until
	// it nils the slot, at which point ownership passes to the
	// producer.
	//
	// The head index is stored in the most-significant bits so
	// that we can atomically add to it and the overflow is
	// harmless.
	//队头队尾位置，头部只会被生产者访问(本地P从对头存取)，尾部只会被消费者访问(其他P从队尾取)
	headTail atomic.Uint64

	// vals is a ring buffer of interface{} values stored in this
	// dequeue. The size of this must be a power of 2.
	//
	// vals[i].typ is nil if the slot is empty and non-nil
	// otherwise. A slot is still in use until *both* the tail
	// index has moved beyond it and typ has been set to nil. This
	// is set to nil atomically by the consumer and read
	// atomically by the producer.
	vals []eface
}

type eface struct {
	typ, val unsafe.Pointer
}

const dequeueBits = 32

// dequeueLimit is the maximum size of a poolDequeue.
//
// This must be at most (1<<dequeueBits)/2 because detecting fullness
// depends on wrapping around the ring buffer without wrapping around
// the index. We divide by 4 so this fits in an int on 32-bit.
const dequeueLimit = (1 << dequeueBits) / 4

// dequeueNil is used in poolDequeue to represent interface{}(nil).
// Since we use nil to represent empty slots, we need a sentinel value
// to represent nil.
type dequeueNil *struct{}

func (d *poolDequeue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (d *poolDequeue) pack(head, tail uint32) uint64 {
	const mask = 1<<dequeueBits - 1
	return (uint64(head) << dequeueBits) |
		uint64(tail&mask)
}

// pushHead adds val at the head of the queue. It returns false if the
// queue is full. It must only be called by a single producer.
// 添加对象到poolDequeue双端队列的队头，如果队列满了则返回false，只会被单个生产者调用(所属的p)
func (d *poolDequeue) pushHead(val any) bool {
	ptrs := d.headTail.Load()
	//解析head和tail的值，高32位是head，低32位是tail
	head, tail := d.unpack(ptrs)
	//队列满了
	if (tail+uint32(len(d.vals)))&(1<<dequeueBits-1) == head {
		// Queue is full.
		return false
	}
	//找到head对应的数组下标位置：head&uint32(len(d.vals)-1)等价于head%len(d.vals)
	slot := &d.vals[head&uint32(len(d.vals)-1)]

	// Check if the head slot has been released by popTail.
	//检查是否和popTail冲突，操作同一个位置
	typ := atomic.LoadPointer(&slot.typ)
	if typ != nil {
		// Another goroutine is still cleaning up the tail, so
		// the queue is actually still full.
		return false
	}

	// The head slot is free, so we own it.
	if val == nil {
		val = dequeueNil(nil)
	}
	*(*any)(unsafe.Pointer(slot)) = val

	// Increment head. This passes ownership of slot to popTail
	// and acts as a store barrier for writing the slot.
	//head加1
	d.headTail.Add(1 << dequeueBits)
	return true
}

// popHead removes and returns the element at the head of the queue.
// It returns false if the queue is empty. It must only be called by a
// single producer.
// 从poolDequeue双端队列的队头取对象，这个操作只会被单个生产者调用(所属的p)
func (d *poolDequeue) popHead() (any, bool) {
	var slot *eface
	//循环CAS获取一个对象
	for {
		ptrs := d.headTail.Load()
		head, tail := d.unpack(ptrs)
		if tail == head {
			// Queue is empty.
			return nil, false
		}

		// Confirm tail and decrement head. We do this before
		// reading the value to take back ownership of this
		// slot.
		//head减1，并更新到headTail中去
		head--
		ptrs2 := d.pack(head, tail)
		if d.headTail.CompareAndSwap(ptrs, ptrs2) {
			// We successfully took back slot.
			//找到head对应的数组下标位置：head&uint32(len(d.vals)-1)等价于head%len(d.vals)
			slot = &d.vals[head&uint32(len(d.vals)-1)]
			break
		}
	}

	val := *(*any)(unsafe.Pointer(slot))
	if val == dequeueNil(nil) {
		val = nil
	}
	// Zero the slot. Unlike popTail, this isn't racing with
	// pushHead, so we don't need to be careful here.
	*slot = eface{}
	return val, true
}

// popTail removes and returns the element at the tail of the queue.
// It returns false if the queue is empty. It may be called by any
// number of consumers.
// 从poolDequeue双端队列的队尾取对象，这个操作会被多个生产者调用(其他p)
func (d *poolDequeue) popTail() (any, bool) {
	var slot *eface
	//循环CAS获取一个对象
	for {
		ptrs := d.headTail.Load()
		head, tail := d.unpack(ptrs)
		if tail == head {
			// Queue is empty.
			return nil, false
		}

		// Confirm head and tail (for our speculative check
		// above) and increment tail. If this succeeds, then
		// we own the slot at tail.
		//tail加1，并更新到headTail中去
		ptrs2 := d.pack(head, tail+1)
		if d.headTail.CompareAndSwap(ptrs, ptrs2) {
			// Success.
			//找到tail对应的数组下标位置：tail&uint32(len(d.vals)-1)等价于tail%len(d.vals)
			slot = &d.vals[tail&uint32(len(d.vals)-1)]
			break
		}
	}

	// We now own slot.
	val := *(*any)(unsafe.Pointer(slot))
	if val == dequeueNil(nil) {
		val = nil
	}

	// Tell pushHead that we're done with this slot. Zeroing the
	// slot is also important so we don't leave behind references
	// that could keep this object live longer than necessary.
	//
	// We write to val first and then publish that we're done with
	// this slot by atomically writing to typ.
	slot.val = nil
	atomic.StorePointer(&slot.typ, nil)
	// At this point pushHead owns the slot.

	return val, true
}

// poolChain is a dynamically-sized version of poolDequeue.
//
// This is implemented as a doubly-linked list queue of poolDequeues
// where each dequeue is double the size of the previous one. Once a
// dequeue fills up, this allocates a new one and only ever pushes to
// the latest dequeue. Pops happen from the other end of the list and
// once a dequeue is exhausted, it gets removed from the list.
// 双向链表，每个节点都有一个双端队列存储缓存的对象，链头节点表示最新的节点，链尾节点表示最旧的节点
// 每个节点的next指向更新节点的方向(偏链头方向)，prev指向更旧节点的方向(偏链尾方向)，这个和普通的双向链表中的next,prev含义相反
type poolChain struct {
	// head is the poolDequeue to push to. This is only accessed
	// by the producer, so doesn't need to be synchronized.
	// 链表头节点，表示最新的节点，它的前驱节点是更旧一点的节点，并不是nil，后驱节点是更新一点的节点，和双向链表有点区别。且只被生产者访问(所属的p)
	head *poolChainElt

	// tail is the poolDequeue to popTail from. This is accessed
	// by consumers, so reads and writes must be atomic.
	// 链表的尾节点，表示最旧的节点。只被消费者访问(就是其他P，不是这个链表所属的P，会在偷取的时候访问)
	tail atomic.Pointer[poolChainElt]
}

// poolChain链表中的节点，底层是一个poolDequeue，是一个双端队列，视为一个环形缓冲区
type poolChainElt struct {
	//底层数据存储的双端队列
	poolDequeue

	// next and prev link to the adjacent poolChainElts in this
	// poolChain.
	//
	// next is written atomically by the producer and read
	// atomically by the consumer. It only transitions from nil to
	// non-nil.
	//
	// prev is written atomically by the consumer and read
	// atomically by the producer. It only transitions from
	// non-nil to nil.
	// next:更新的节点(偏链头方向)，prev:更旧的节点(偏链尾方向) 和正常的双向链表的next/prev含义相反
	next, prev atomic.Pointer[poolChainElt]
}

// 存储对象到指定p的localPool.shared链表的head节点的双端队列的队头去，如果当前head节点的双端队列满了，则新增一个节点，添加进去，并作为链表的新头节点
func (c *poolChain) pushHead(val any) {
	d := c.head
	//如果head节点不存在，则创建节点作为head节点，并设置双端队列的长度为8
	if d == nil {
		// Initialize the chain.
		const initSize = 8 // Must be a power of 2
		d = new(poolChainElt)
		d.vals = make([]eface, initSize)
		c.head = d
		c.tail.Store(d)
	}

	//将对象添加到最新节点的双端队列的队头中去
	if d.pushHead(val) {
		return
	}

	// The current dequeue is full. Allocate a new one of twice
	// the size.
	//添加到head节点失败，说明节点的队列满了，则创建一个新节点，大小为当前节点的两倍(最大不超过dequeueLimit)
	newSize := len(d.vals) * 2
	if newSize >= dequeueLimit {
		// Can't make it any bigger.
		newSize = dequeueLimit
	}

	//将新节点作为head节点(最新的节点)
	d2 := &poolChainElt{}
	d2.prev.Store(d)
	d2.vals = make([]eface, newSize)
	c.head = d2
	d.next.Store(d2)
	d2.pushHead(val)
}

// 从指定p的localPool.shared(p中可被其他p共享的对象链表)链表的head(最新节点)->tail(最旧节点)方向遍历节点，从节点中的双端队列的队头获取一个对象
// 这个方法只会被生产者调用(就是所属的p自己)，但是不表示对节点的访问只有这一个方法，所以需要考虑数据竞争
func (c *poolChain) popHead() (any, bool) {
	d := c.head
	//从最新的节点向最旧节点遍历节点
	for d != nil {
		//从每个节点的双端队列的队头取
		if val, ok := d.popHead(); ok {
			return val, ok
		}
		// There may still be unconsumed elements in the
		// previous dequeue, so try backing up.
		d = d.prev.Load()
	}
	return nil, false
}

// 从指定p的localPool.shared(p中可被其他p共享的对象链表)链表的tail(最旧节点)->head(最新节点)方向遍历节点，从节点中的双端队列的队尾获取一个对象，遍历的时候如果发现节点的对象为空(即没有取到对象)，则将节点从链表中剔除
// 会被消费者调用(就是除自己意外的其他p偷取)，会被多个消费者调用，需要考虑数据竞争
func (c *poolChain) popTail() (any, bool) {
	d := c.tail.Load()
	if d == nil {
		return nil, false
	}

	//从最旧的节点向最新节点遍历节点
	for {
		// It's important that we load the next pointer
		// *before* popping the tail. In general, d may be
		// transiently empty, but if next is non-nil before
		// the pop and the pop fails, then d is permanently
		// empty, which is the only condition under which it's
		// safe to drop d from the chain.
		//获取更新的一个节点
		d2 := d.next.Load()

		//从节点的双端队列的尾部获取对象，获取到则直接返回
		if val, ok := d.popTail(); ok {
			return val, ok
		}
		//表示没有节点了，全部遍历完了还没有获取到，直接返回
		if d2 == nil {
			// This is the only dequeue. It's empty right
			// now, but could be pushed to in the future.
			return nil, false
		}

		// The tail of the chain has been drained, so move on
		// to the next dequeue. Try to drop it from the chain
		// so the next pop doesn't have to look at the empty
		// dequeue again.
		//表示当前节点没有对象可供获取，将节点从链表剔除
		if c.tail.CompareAndSwap(d, d2) {
			// We won the race. Clear the prev pointer so
			// the garbage collector can collect the empty
			// dequeue and so popHead doesn't back up
			// further than necessary.
			d2.prev.Store(nil)
		}
		d = d2
	}
}
