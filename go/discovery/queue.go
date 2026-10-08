/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*

package discovery manages a queue of discovery requests: an ordered
queue with no duplicates.

push() operation never blocks while pop() blocks on an empty queue.

*/

package discovery

import (
	"sync"
	"time"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
)

// QueueMetric contains the queue's active and queued sizes
type QueueMetric struct {
	Active int
	Queued int
}

// Queue contains information for managing discovery requests
type Queue struct {
	sync.Mutex

	name         string
	cfg          *config.Configuration // read for statistics size and poll warning threshold
	done         chan struct{}
	stopOnce     sync.Once
	queue        chan inst.InstanceKey
	queuedKeys   map[inst.InstanceKey]time.Time
	consumedKeys map[inst.InstanceKey]time.Time
	metrics      []QueueMetric
}

// discoveryQueue is the legacy process-wide registry of named queues, which can
// be accessed via an API call for monitoring. Per-tenant code should own its
// queues via NewQueue()/NewQueueFromConfig() instead of registering them here.
var discoveryQueue map[string](*Queue)
var dcLock sync.Mutex

func init() {
	discoveryQueue = make(map[string](*Queue))
}

// StopMonitoring stops monitoring all the registered (named) queues
func StopMonitoring() {
	dcLock.Lock()
	queues := make([]*Queue, 0, len(discoveryQueue))
	for _, q := range discoveryQueue {
		queues = append(queues, q)
	}
	dcLock.Unlock()

	for _, q := range queues {
		q.stopMonitoring()
	}
}

func ReturnQueue(name string) *Queue {
	dcLock.Lock()
	defer dcLock.Unlock()
	if q, found := discoveryQueue[name]; found {
		return q
	}
	return nil
}

// CreateOrReturnQueue allows for creation of a new discovery queue or
// returning a pointer to an existing one given the name. It is the legacy
// (single tenant) shim: queues are registered in a process-wide map and are
// sized from config.Config.
func CreateOrReturnQueue(name string) *Queue {
	dcLock.Lock()
	defer dcLock.Unlock()
	if q, found := discoveryQueue[name]; found {
		return q
	}

	q := NewQueueFromConfig(name, config.Config)
	discoveryQueue[name] = q

	return q
}

// NewQueue creates a queue that is NOT registered in the process-wide map, with
// the given channel capacity. Statistics size and the slow-key warning threshold
// are read from config.Config; use NewQueueFromConfig for a tenant config.
// The caller owns the queue and must Close() it.
func NewQueue(name string, capacity int) *Queue {
	return newQueue(name, capacity, config.Config)
}

// NewQueueFromConfig creates an unregistered queue sized and tuned by cfg
// (DiscoveryQueueCapacity, DiscoveryQueueMaxStatisticsSize, InstancePollSeconds).
// The caller owns the queue and must Close() it.
func NewQueueFromConfig(name string, cfg *config.Configuration) *Queue {
	return newQueue(name, int(cfg.DiscoveryQueueCapacity), cfg)
}

func newQueue(name string, capacity int, cfg *config.Configuration) *Queue {
	q := &Queue{
		name:         name,
		cfg:          cfg,
		done:         make(chan struct{}),
		queuedKeys:   make(map[inst.InstanceKey]time.Time),
		consumedKeys: make(map[inst.InstanceKey]time.Time),
		queue:        make(chan inst.InstanceKey, capacity),
	}
	go q.startMonitoring()
	return q
}

// Name returns the name the queue was created with
func (q *Queue) Name() string {
	return q.name
}

// Close stops the queue's statistics collection. It is safe to call more than
// once. Keys already queued can still be consumed.
func (q *Queue) Close() {
	q.stopMonitoring()
}

// monitoring queue sizes until we are told to stop
func (q *Queue) startMonitoring() {
	log.Debugf("Queue.startMonitoring(%s)", q.name)
	ticker := time.NewTicker(time.Second) // hard-coded at every second
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // do the periodic expiry
			q.collectStatistics()
		case <-q.done:
			return
		}
	}
}

// Stop monitoring the queue
func (q *Queue) stopMonitoring() {
	q.stopOnce.Do(func() { close(q.done) })
}

// do a check of the entries in the queue, both those active and queued
func (q *Queue) collectStatistics() {
	q.Lock()
	defer q.Unlock()

	q.metrics = append(q.metrics, QueueMetric{Queued: len(q.queuedKeys), Active: len(q.consumedKeys)})

	// remove old entries if we get too big
	if len(q.metrics) > q.cfg.DiscoveryQueueMaxStatisticsSize {
		q.metrics = q.metrics[len(q.metrics)-q.cfg.DiscoveryQueueMaxStatisticsSize:]
	}
}

// QueueLen returns the length of the queue (channel size + queued size)
func (q *Queue) QueueLen() int {
	q.Lock()
	defer q.Unlock()

	return len(q.queue) + len(q.queuedKeys)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key inst.InstanceKey) {
	q.Lock()
	defer q.Unlock()

	// is it enqueued already?
	if _, found := q.queuedKeys[key]; found {
		return
	}

	// is it being processed now?
	if _, found := q.consumedKeys[key]; found {
		return
	}

	q.queuedKeys[key] = time.Now()
	q.queue <- key
}

// Consume fetches a key to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() inst.InstanceKey {
	// q.queue is never reassigned, so it can be read without the lock
	key := <-q.queue

	q.Lock()
	defer q.Unlock()

	// alarm if have been waiting for too long
	timeOnQueue := time.Since(q.queuedKeys[key])
	if timeOnQueue > time.Duration(q.cfg.InstancePollSeconds)*time.Second {
		_ = log.Warningf("key %v spent %.4fs waiting on a discoveryQueue", key, timeOnQueue.Seconds())
	}

	q.consumedKeys[key] = q.queuedKeys[key]

	delete(q.queuedKeys, key)

	return key
}

// Release removes a key from a list of being processed keys
// which allows that key to be pushed into the queue again.
func (q *Queue) Release(key inst.InstanceKey) {
	q.Lock()
	defer q.Unlock()

	delete(q.consumedKeys, key)
}
