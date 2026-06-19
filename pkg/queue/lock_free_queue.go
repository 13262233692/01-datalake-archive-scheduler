package queue

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type LockFreeQueue struct {
	buffer     []interface{}
	capacity   int64
	mask       int64
	writePos   int64
	readPos    int64
	itemCount  int64
}

func NewLockFreeQueue(capacity int) *LockFreeQueue {
	c := int64(1)
	for c < int64(capacity) {
		c <<= 1
	}

	return &LockFreeQueue{
		buffer:    make([]interface{}, c),
		capacity:  c,
		mask:      c - 1,
		writePos:  0,
		readPos:   0,
		itemCount: 0,
	}
}

func (q *LockFreeQueue) Enqueue(item interface{}) bool {
	if atomic.LoadInt64(&q.itemCount) >= q.capacity {
		return false
	}

	pos := atomic.AddInt64(&q.writePos, 1) - 1
	idx := pos & q.mask

	for atomic.LoadInt64(&q.itemCount) >= q.capacity {
		runtime.Gosched()
	}

	q.buffer[idx] = item
	atomic.AddInt64(&q.itemCount, 1)

	return true
}

func (q *LockFreeQueue) Dequeue() (interface{}, bool) {
	if atomic.LoadInt64(&q.itemCount) <= 0 {
		return nil, false
	}

	pos := atomic.AddInt64(&q.readPos, 1) - 1
	idx := pos & q.mask

	for atomic.LoadInt64(&q.itemCount) <= 0 {
		runtime.Gosched()
	}

	item := q.buffer[idx]
	q.buffer[idx] = nil
	atomic.AddInt64(&q.itemCount, -1)

	return item, true
}

func (q *LockFreeQueue) DequeueWithTimeout(timeout time.Duration) (interface{}, bool) {
	deadline := time.Now().Add(timeout)

	for {
		item, ok := q.Dequeue()
		if ok {
			return item, true
		}

		if time.Now().After(deadline) {
			return nil, false
		}

		runtime.Gosched()
	}
}

func (q *LockFreeQueue) Len() int {
	return int(atomic.LoadInt64(&q.itemCount))
}

func (q *LockFreeQueue) Capacity() int {
	return int(q.capacity)
}

func (q *LockFreeQueue) IsEmpty() bool {
	return atomic.LoadInt64(&q.itemCount) == 0
}

func (q *LockFreeQueue) IsFull() bool {
	return atomic.LoadInt64(&q.itemCount) >= q.capacity
}

type BatchingQueue struct {
	queue       *LockFreeQueue
	batchSize    int
	flusherCh   chan struct{}
	stopCh      chan struct{}
	stopped     int32
	batchMu     sync.Mutex
	batches     [][]interface{}
}

func NewBatchingQueue(capacity, batchSize int) *BatchingQueue {
	return &BatchingQueue{
		queue:     NewLockFreeQueue(capacity),
		batchSize: batchSize,
		flusherCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
}

func (bq *BatchingQueue) Enqueue(item interface{}) bool {
	if !bq.queue.Enqueue(item) {
		return false
	}

	if bq.queue.Len() >= bq.batchSize {
		select {
		case bq.flusherCh <- struct{}{}:
		default:
		}
	}

	return true
}

func (bq *BatchingQueue) DequeueBatch(maxBatchSize int) []interface{} {
	count := bq.queue.Len()
	if count <= 0 {
		return nil
	}

	if count > maxBatchSize {
		count = maxBatchSize
	}

	batch := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		item, ok := bq.queue.Dequeue()
		if !ok {
			break
		}
		batch = append(batch, item)
	}

	return batch
}

func (bq *BatchingQueue) StartFlushTicker(interval time.Duration) {
	ticker := time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				select {
				case bq.flusherCh <- struct{}{}:
				default:
				}
			case <-bq.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func (bq *BatchingQueue) Stop() {
	if atomic.CompareAndSwapInt32(&bq.stopped, 0, 1) {
		close(bq.stopCh)
	}
}

func (bq *BatchingQueue) Len() int {
	return bq.queue.Len()
}

type QueueConsumer struct {
	queue       *BatchingQueue
	processFunc func(batch []interface{}) error
	running     int32
	stopCh      chan struct{}
	wg          sync.WaitGroup
	workerCount int
}

func NewQueueConsumer(queue *BatchingQueue, workerCount int, processFunc func(batch []interface{}) error) *QueueConsumer {
	return &QueueConsumer{
		queue:       queue,
		processFunc: processFunc,
		stopCh:      make(chan struct{}),
		workerCount: workerCount,
	}
}

func (qc *QueueConsumer) Start() {
	if !atomic.CompareAndSwapInt32(&qc.running, 0, 1) {
		return
	}

	for i := 0; i < qc.workerCount; i++ {
		qc.wg.Add(1)
		go qc.worker()
	}
}

func (qc *QueueConsumer) worker() {
	defer qc.wg.Done()

	for {
		select {
		case <-qc.stopCh:
			return
		default:
		}

		batch := qc.queue.DequeueBatch(100)
		if len(batch) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err := qc.processFunc(batch); err != nil {
			continue
		}
	}
}

func (qc *QueueConsumer) Stop() {
	if !atomic.CompareAndSwapInt32(&qc.running, 1, 0) {
		return
	}

	close(qc.stopCh)
	qc.wg.Wait()
}

func (qc *QueueConsumer) IsRunning() bool {
	return atomic.LoadInt32(&qc.running) == 1
}
