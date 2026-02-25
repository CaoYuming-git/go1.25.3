// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync

import (
	"internal/race"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// A Pool is a set of temporary objects that may be individually saved and
// retrieved.
//
// Any item stored in the Pool may be removed automatically at any time without
// notification. If the Pool holds the only reference when this happens, the
// item might be deallocated.
//
// A Pool is safe for use by multiple goroutines simultaneously.
//
// Pool's purpose is to cache allocated but unused items for later reuse,
// relieving pressure on the garbage collector. That is, it makes it easy to
// build efficient, thread-safe free lists. However, it is not suitable for all
// free lists.
//
// An appropriate use of a Pool is to manage a group of temporary items
// silently shared among and potentially reused by concurrent independent
// clients of a package. Pool provides a way to amortize allocation overhead
// across many clients.
//
// An example of good use of a Pool is in the fmt package, which maintains a
// dynamically-sized store of temporary output buffers. The store scales under
// load (when many goroutines are actively printing) and shrinks when
// quiescent.
//
// On the other hand, a free list maintained as part of a short-lived object is
// not a suitable use for a Pool, since the overhead does not amortize well in
// that scenario. It is more efficient to have such objects implement their own
// free list.
//
// A Pool must not be copied after first use.
//
// In the terminology of [the Go memory model], a call to Put(x) “synchronizes before”
// a call to [Pool.Get] returning that same value x.
// Similarly, a call to New returning x “synchronizes before”
// a call to Get returning that same value x.
//
// [the Go memory model]: https://go.dev/ref/mem
type Pool struct {
	// 防止pool被拷贝的一个声明类型(供go vet检查)
	noCopy noCopy
	// 本地缓存对象池：按P划分的本地固定大小的对象池，实际类型是[P]poolLocal
	local unsafe.Pointer // local fixed-size per-P pool, actual type is [P]poolLocal
	// 本地缓存对象池的大小，即元素数量，即P的数量(GOMAXPROCS)
	localSize uintptr // size of the local array
	// 受害者对象池：上一次GC后的本地缓存对象池(local)
	victim unsafe.Pointer // local from previous cycle
	// 受害者对象池的大小，即元素数量，也是P的数量(GOMAXPROCS)
	victimSize uintptr // size of victims array

	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	// It may not be changed concurrently with calls to Get.
	// 生成新对象时使用的方法
	New func() any
}

// Local per-P Pool appendix.
// P本地池底层数据的结构，每个p一个实例
type poolLocalInternal struct {
	//P私有的【1个】缓存对象，仅供P使用
	private any // Can be used only by the respective P.
	//可被其他p共享的【多个】缓存对象，可被其他P偷。
	//是一个双向链表
	//P自己从head(链头节点，最新节点)->tail(链尾节点，最旧节点)方向进行取，向head(链头节点，最新节点)存
	//其他P从tail(链尾节点，最旧节点)->head(链头节点，最新节点)方向进行取
	shared poolChain // Local P can pushHead/popHead; any P can popTail.
}

// 每个P自己的本地池
type poolLocal struct {
	//真正存储数据的部分
	poolLocalInternal

	// Prevents false sharing on widespread platforms with
	// 128 mod (cache line size) = 0 .
	//不同cpu访问不同变量，如果这些变量落在同一个cache line中，会导致缓存不停失效，性能严重下降
	//一般cpu cache line通常是64字节，go这里使用128字节对齐，为了让每个poolLocal的起始地址都落在不同的cache line上。
	pad [128 - unsafe.Sizeof(poolLocalInternal{})%128]byte
}

// from runtime
//
//go:linkname runtime_randn runtime.randn
func runtime_randn(n uint32) uint32

var poolRaceHash [128]uint64

// poolRaceAddr returns an address to use as the synchronization point
// for race detector logic. We don't use the actual pointer stored in x
// directly, for fear of conflicting with other synchronization on that address.
// Instead, we hash the pointer to get an index into poolRaceHash.
// See discussion on golang.org/cl/31589.
func poolRaceAddr(x any) unsafe.Pointer {
	ptr := uintptr((*[2]unsafe.Pointer)(unsafe.Pointer(&x))[1])
	h := uint32((uint64(uint32(ptr)) * 0x85ebca6b) >> 16)
	return unsafe.Pointer(&poolRaceHash[h%uint32(len(poolRaceHash))])
}

// Put adds x to the pool.
// 放回一个对象到pool中
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}
	//如果是以-race构建的，开启了数据竞争检测
	if race.Enabled {
		//以25%的概率丢弃对象(强化非确定性缓存的特性，即对象可能会被丢弃的特性),通过随机丢弃更早暴露错误用法
		if runtime_randn(4) == 0 {
			// Randomly drop x on floor.
			return
		}
		//告诉数据竞争检测器，x被同步了，避免数据竞争检测器的误报。因为数据竞争检测器只能识别几种同步原语，比如：sync.Mutex、channel、atomic等等，而sync.Pool并不是同步原语，所以这里手动标记为同步
		race.ReleaseMerge(poolRaceAddr(x))
		//关闭竞争检测器
		race.Disable()
	}
	//将g固定到p上，并返回该p的缓冲池poolLocal(如果没有则创建poolLocal返回)
	l, _ := p.pin()
	if l.private == nil {
		//如果poolLocal.private空，则存到private中
		l.private = x
	} else {
		//如果poolLocal.private有值了，则放到poolLocal.shared中去
		l.shared.pushHead(x)
	}
	runtime_procUnpin()
	if race.Enabled {
		//重新打开竞争检测器
		race.Enable()
	}
}

// Get selects an arbitrary item from the [Pool], removes it from the
// Pool, and returns it to the caller.
// Get may choose to ignore the pool and treat it as empty.
// Callers should not assume any relation between values passed to [Pool.Put] and
// the values returned by Get.
//
// If Get would otherwise return nil and p.New is non-nil, Get returns
// the result of calling p.New.
// 从Pool获取一个对象，获取顺序是：
// 1、将g固定到p,并返回p自己的localPool(如果没有则会初始化创建)，称为l
// 2、先从l.private(p私有的对象)中获取对象(无锁，只有自己所属的p访问，无并发)
// 3、如果l.private没有，则从l.shared(p中可被其他p共享的对象链表)中的head(最新节点)->tail(最旧节点)遍历节点获取
// 4、如果l.shared中没有，则从其他p的localPool.shared中的tail(最旧节点)->head(最新节点)遍历节点偷取
// 5、如果其他p的shared没有，则从Pool.victim中获取
// (5.1)、先从victim中p自己的private获取
// (5.2)、再从victim中的所有p的shared链表中去获取
// 6、如果victim中没有，则生成一个(调用New函数)返回
func (p *Pool) Get() any {
	//如果是以-race构建的，临时关闭race检测，自己来处理数据竞争
	if race.Enabled {
		race.Disable()
	}
	//将g固定到p上，并返回该p的缓冲池poolLocal(如果没有则创建poolLocal返回)
	l, pid := p.pin()
	//从localPool.private取，p私有的，不共享(无锁，因为无并发)
	x := l.private
	l.private = nil
	if x == nil {
		// Try to pop the head of the local shard. We prefer
		// the head over the tail for temporal locality of
		// reuse.
		//如果l.private没有，则从l.shared共享对象链表的最新节点->最旧节点方向遍历节点的双端队列的队头取对象
		x, _ = l.shared.popHead()
		if x == nil {
			//如果l.shared没有取到对象，则调用缓慢获取对象方法，获取的优先级是： 从其他p的shared链表偷->victim尝试获取(本p->其他p)
			x = p.getSlow(pid)
		}
	}
	runtime_procUnpin()
	if race.Enabled {
		race.Enable()
		if x != nil {
			race.Acquire(poolRaceAddr(x))
		}
	}
	//如果都没有获取到，则创建一个对象返回
	if x == nil && p.New != nil {
		x = p.New()
	}
	return x
}

// 缓慢方式获取对象，流程如下：
// 1、先尝试从其他p的poolLocal.shared中偷对象，同时会把链表中的空对象节点剔除掉。
// 2、如果没有偷到，再去Pool中的victim中尝试获取(先从victim自己p的poolLocal.private获取，再尝试从victim所有p的poolLocal.shared获取)
func (p *Pool) getSlow(pid int) any {
	// See the comment in pin regarding ordering of the loads.
	//local的元素个数，即p的数量
	size := runtime_LoadAcquintptr(&p.localSize) // load-acquire
	locals := p.local                            // load-consume
	// Try to steal one element from other procs.
	//尝试从其他p的poolLocal.shared中去偷对象(从最旧节点向最新节点遍历，期间如果发现节点对象空了，还会把节点从链表中剔除掉)
	for i := 0; i < int(size); i++ {
		l := indexLocal(locals, (pid+i+1)%int(size))
		if x, _ := l.shared.popTail(); x != nil {
			return x
		}
	}

	// Try the victim cache. We do this after attempting to steal
	// from all primary caches because we want objects in the
	// victim cache to age out if at all possible.

	//判断victim是否有这个p的poolLocal，没有直接返回(可能出现P数量变化的情况导致)
	size = atomic.LoadUintptr(&p.victimSize)
	if uintptr(pid) >= size {
		return nil
	}
	//尝试从victim中获取对象
	locals = p.victim
	l := indexLocal(locals, pid)
	//先从victim中p的private获取
	if x := l.private; x != nil {
		l.private = nil
		return x
	}
	//如果没有再从victim中的所有p的shared链表中去获取
	for i := 0; i < int(size); i++ {
		l := indexLocal(locals, (pid+i)%int(size))
		if x, _ := l.shared.popTail(); x != nil {
			return x
		}
	}

	// Mark the victim cache as empty for future gets don't bother
	// with it.
	//如果victim中没有获取到，则把victimSize标记为0，后续再次调用时，会在前面判断直接跳过victim获取的步骤了
	atomic.StoreUintptr(&p.victimSize, 0)

	return nil
}

// pin pins the current goroutine to P, disables preemption and
// returns poolLocal pool for the P and the P's id.
// Caller must call runtime_procUnpin() when done with the pool.
/*
功能：将g固定到p上，并返回该p的缓冲池poolLocal，注意：如果没有则还会为所有p创建poolLocal(pool实例中的所有p的localPool是一次性同时创建的)，调用pinSlow实现
【关键点】将g固定到p上，会禁止g被迁移到其他p上：
1、这样能保证g访问所属p的poolLocal.private时，几乎是不需要加锁，因为p只会有自己这1个g在访问poolLocal.private，不会出现并行导致的数据竞争。
   否则，如果没有把g固定到p上，g访问所属p的poolLocal.private时，从p1迁移到了p2，会出现数据竞争的情况，比如以下场景时：
	(1)goroutine 开始在 P0
	(2)读了 local[0].private
	(3)goroutine 被抢占
	(4)在 P1 上恢复
	(5)写的还是 local[0].private，此时P0上有另一个g也正在写local[0].private
	就会出现数据竞争，要保证正常运行地加锁才行
2、但是g访问所属p的poolLocal.shared时，因为同时可能有多个g访问，还是需要加锁(不是使用mutex，使用的是CAS和轮询)
*/
func (p *Pool) pin() (*poolLocal, int) {
	// Check whether p is nil to get a panic.
	// Otherwise the nil dereference happens while the m is pinned,
	// causing a fatal error rather than a panic.
	if p == nil {
		panic("nil Pool")
	}

	//将当前goroutine固定到p上，在pin期间保证goroutine不会迁移到其他P上执行，并返回p的id
	pid := runtime_procPin()
	// In pinSlow we store to local and then to localSize, here we load in opposite order.
	// Since we've disabled preemption, GC cannot happen in between.
	// Thus here we must observe local at least as large localSize.
	// We can observe a newer/larger local, it is fine (we must observe its zero-initialized-ness).
	//获取localSize的值，即poolLocal的数量
	s := runtime_LoadAcquintptr(&p.localSize) // load-acquire
	//获取Pool的local缓冲池数组
	l := p.local // load-consume
	//如果当前p缓冲池存在(即当前Pool为这个P分配了poolLocal)，则取出来返回
	if uintptr(pid) < s {
		return indexLocal(l, pid), pid
	}
	//如果当前p缓冲池不存在(即当前Pool还没有为这个P分配poolLocal)，则为p创建localPool(实际是为所有p重新创建分配localPool)
	return p.pinSlow()
}

// 慢固定：将g固定到p上，并返回该p的缓冲池poolLocal(如果没有则还会为所有p创建poolLocal返回)。
func (p *Pool) pinSlow() (*poolLocal, int) {
	// Retry under the mutex.
	// Can not lock the mutex while pinned.
	//取消将g固定到p中
	runtime_procUnpin()
	//对allPool加锁
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()
	//重新将g固定到p中
	pid := runtime_procPin()
	// poolCleanup won't be called while we are pinned.
	s := p.localSize
	l := p.local
	//再尝试从p的poolLocal数组中取一次
	if uintptr(pid) < s {
		return indexLocal(l, pid), pid
	}
	//如果此时该g所属的p还没有分配poolLocal，则将这个pool实例加入allPools中，然后为所有的p重新创建poolLocal(所有的p的poolLocal是一起创建的)
	if p.local == nil {
		allPools = append(allPools, p)
	}
	// If GOMAXPROCS changes between GCs, we re-allocate the array and lose the old one.
	//获取当前可用的p数量
	size := runtime.GOMAXPROCS(0)
	//创建p个poolLocal
	local := make([]poolLocal, size)
	atomic.StorePointer(&p.local, unsafe.Pointer(&local[0])) // store-release
	runtime_StoreReluintptr(&p.localSize, uintptr(size))     // store-release
	return &local[pid], pid
}

// poolCleanup should be an internal detail,
// but widely used packages access it using linkname.
// Notable members of the hall of shame include:
//   - github.com/bytedance/gopkg
//   - github.com/songzhibin97/gkit
//
// Do not remove or change the type signature.
// See go.dev/issue/67401.
//
// pool中的对象清理函数，会在gc开始时调用
// 1、将上一轮gc留下的oldPools中的受害者对象池(victim)置空，即去除引用关系，才能让后面的gc回收掉
// 2、将本轮gc中的allPools中的本地缓存对象池(local)转换为受害者对象池(victim)，即不直接将本地缓存对象池清空回收，而是给一个缓冲时间，在下一轮gc回收掉
// 3、更新allPools和oldPools集合
//
//go:linkname poolCleanup
func poolCleanup() {
	// This function is called with the world stopped, at the beginning of a garbage collection.
	// It must not allocate and probably should not call any runtime functions.

	// Because the world is stopped, no pool user can be in a
	// pinned section (in effect, this has all Ps pinned).

	// Drop victim caches from all pools.
	//对旧池子中的受害者对象池(victim)清空，即缓存的对象此时才可能被gc回收，也就是说缓存的对象至少会存在两个gc周期，
	//第一个gc周期会从local变为victim，第二个gc周期才会通过victim=nil，去掉引用关系，才能让缓存对象被gc回收
	for _, p := range oldPools {
		p.victim = nil
		p.victimSize = 0
	}

	// Move primary cache to victim cache.
	//将新pool实例中的本地缓存对象池(local)转换为受害者对象池(victim)，即第一次gc周期不会直接把缓存对象回收，而是在第二个周期，即上面的代码去掉引用关系后被gc回收
	for _, p := range allPools {
		p.victim = p.local
		p.victimSize = p.localSize
		p.local = nil
		p.localSize = 0
	}

	// The pools with non-empty primary caches now have non-empty
	// victim caches and no pools have primary caches.
	//1、上面已经清空了oldPools中的受害者对象池(victim)，所以原来的oldPools中的pool实例已经不算是oldPool了，更不算是allPool对象了，因为既没有本地缓存对象池(local)，又没有受害者对象池(victim)
	//2、上面把allPools中的本地缓存对象池(local)转换为了受害者对象池(victim)，所以现在的allPools中的pool实例已经不算是allPool了，已经变成了oldPool
	//所以这里更新oldPool、allPools集合
	oldPools, allPools = allPools, nil
}

var (
	//锁，保护allPools
	allPoolsMu Mutex

	// allPools is the set of pools that have non-empty primary
	// caches. Protected by either 1) allPoolsMu and pinning or 2)
	// STW.
	// 是已经初始化poolLocal的Pool实例集合，会在pinSlow的时候初始化poolLocal时加入到这个全局变量中去，gc的时候会使用到
	allPools []*Pool

	// oldPools is the set of pools that may have non-empty victim
	// caches. Protected by STW.
	// 是一组具有受害者对象池(victim)的pool实例集合，每次gc，allPools会变为oldPools
	oldPools []*Pool
)

// 初始化、先向runtime注册一个poolCleanup清理函数，注册函数实际在runtime包中的mgc.go文件中使用了linkname方式来实现的
func init() {
	runtime_registerPoolCleanup(poolCleanup)
}

func indexLocal(l unsafe.Pointer, i int) *poolLocal {
	lp := unsafe.Pointer(uintptr(l) + uintptr(i)*unsafe.Sizeof(poolLocal{}))
	return (*poolLocal)(lp)
}

// Implemented in runtime. 实际实现在runtime包中的mgc.go文件中使用了linkname方式来实现的
func runtime_registerPoolCleanup(cleanup func())
func runtime_procPin() int
func runtime_procUnpin()

// The below are implemented in internal/runtime/atomic and the
// compiler also knows to intrinsify the symbol we linkname into this
// package.

//go:linkname runtime_LoadAcquintptr internal/runtime/atomic.LoadAcquintptr
func runtime_LoadAcquintptr(ptr *uintptr) uintptr

//go:linkname runtime_StoreReluintptr internal/runtime/atomic.StoreReluintptr
func runtime_StoreReluintptr(ptr *uintptr, val uintptr) uintptr
