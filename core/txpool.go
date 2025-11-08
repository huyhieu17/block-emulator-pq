// the define and some operation of txpool

package core

import (
	"blockEmulator/utils"
	"container/heap"
	"sync"
	"time"
	"unsafe"
)

// Priority queue for transactions based on price
type txHeap []*Transaction

func (h txHeap) Len() int           { return len(h) }
func (h txHeap) Less(i, j int) bool { return h[i].Value.Cmp(h[j].Value) > 0 } // Max heap
func (h txHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *txHeap) Push(x interface{}) {
	*h = append(*h, x.(*Transaction))
}

func (h *txHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TxPool struct {
	TxQueue   *txHeap                   // transaction Queue (priority queue)
	RelayPool map[uint64][]*Transaction //designed for sharded blockchain, from Monoxide
	lock      sync.Mutex
	// The pending list is ignored
}

func NewTxPool() *TxPool {
	h := make(txHeap, 0)
	heap.Init(&h)
	return &TxPool{
		TxQueue:   &h,
		RelayPool: make(map[uint64][]*Transaction),
	}
}

// Add a transaction to the pool (consider the queue only)
func (txpool *TxPool) AddTx2Pool(tx *Transaction) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if tx.Time.IsZero() {
		tx.Time = time.Now()
	}
	heap.Push(txpool.TxQueue, tx)
}

// Add a list of transactions to the pool
func (txpool *TxPool) AddTxs2Pool(txs []*Transaction) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	for _, tx := range txs {
		if tx.Time.IsZero() {
			tx.Time = time.Now()
		}
		heap.Push(txpool.TxQueue, tx)
	}
}

// add transactions into the pool head
func (txpool *TxPool) AddTxs2Pool_Head(tx []*Transaction) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	for _, t := range tx {
		if t.Time.IsZero() {
			t.Time = time.Now()
		}
		heap.Push(txpool.TxQueue, t)
	}
}

// Pack transactions for a proposal
func (txpool *TxPool) PackTxs(max_txs uint64) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	txs_Packed := make([]*Transaction, 0)
	queueLen := txpool.TxQueue.Len()
	txNum := max_txs
	if uint64(queueLen) < txNum {
		txNum = uint64(queueLen)
	}

	for i := uint64(0); i < txNum; i++ {
		if txpool.TxQueue.Len() > 0 {
			txs_Packed = append(txs_Packed, heap.Pop(txpool.TxQueue).(*Transaction))
		}
	}

	return txs_Packed
}

// Pack transaction for a proposal (use 'BlocksizeInBytes' to control)
func (txpool *TxPool) PackTxsWithBytes(max_bytes int) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	txs_Packed := make([]*Transaction, 0)
	currentSize := 0

	for txpool.TxQueue.Len() > 0 {
		tx := heap.Pop(txpool.TxQueue).(*Transaction)
		currentSize += int(unsafe.Sizeof(*tx))
		txs_Packed = append(txs_Packed, tx)
		if currentSize > max_bytes {
			break
		}
	}

	return txs_Packed
}

// Relay transactions
func (txpool *TxPool) AddRelayTx(tx *Transaction, shardID uint64) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	_, ok := txpool.RelayPool[shardID]
	if !ok {
		txpool.RelayPool[shardID] = make([]*Transaction, 0)
	}
	txpool.RelayPool[shardID] = append(txpool.RelayPool[shardID], tx)
}

// txpool get locked
func (txpool *TxPool) GetLocked() {
	txpool.lock.Lock()
}

// txpool get unlocked
func (txpool *TxPool) GetUnlocked() {
	txpool.lock.Unlock()
}

// get the length of tx queue
func (txpool *TxPool) GetTxQueueLen() int {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	return txpool.TxQueue.Len()
}

// get the length of ClearRelayPool
func (txpool *TxPool) ClearRelayPool() {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	txpool.RelayPool = nil
}

// abort ! Pack relay transactions from relay pool
func (txpool *TxPool) PackRelayTxs(shardID, minRelaySize, maxRelaySize uint64) ([]*Transaction, bool) {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()
	if _, ok := txpool.RelayPool[shardID]; !ok {
		return nil, false
	}
	if len(txpool.RelayPool[shardID]) < int(minRelaySize) {
		return nil, false
	}
	txNum := maxRelaySize
	if uint64(len(txpool.RelayPool[shardID])) < txNum {
		txNum = uint64(len(txpool.RelayPool[shardID]))
	}
	relayTxPacked := txpool.RelayPool[shardID][:txNum]
	txpool.RelayPool[shardID] = txpool.RelayPool[shardID][txNum:]
	return relayTxPacked, true
}

// abort ! Transfer transactions when re-sharding
func (txpool *TxPool) TransferTxs(addr utils.Address) []*Transaction {
	txpool.lock.Lock()
	defer txpool.lock.Unlock()

	txTransfered := make([]*Transaction, 0)
	newTxHeap := make(txHeap, 0)

	// Pop all transactions from the heap
	tempTxs := make([]*Transaction, 0)
	for txpool.TxQueue.Len() > 0 {
		tempTxs = append(tempTxs, heap.Pop(txpool.TxQueue).(*Transaction))
	}

	// Separate transactions by sender
	for _, tx := range tempTxs {
		if tx.Sender == addr {
			txTransfered = append(txTransfered, tx)
		} else {
			newTxHeap = append(newTxHeap, tx)
		}
	}

	// Rebuild the heap with remaining transactions
	heap.Init(&newTxHeap)
	txpool.TxQueue = &newTxHeap

	// Process relay pool
	newRelayPool := make(map[uint64][]*Transaction)
	for shardID, shardPool := range txpool.RelayPool {
		for _, tx := range shardPool {
			if tx.Sender == addr {
				txTransfered = append(txTransfered, tx)
			} else {
				if _, ok := newRelayPool[shardID]; !ok {
					newRelayPool[shardID] = make([]*Transaction, 0)
				}
				newRelayPool[shardID] = append(newRelayPool[shardID], tx)
			}
		}
	}
	txpool.RelayPool = newRelayPool
	return txTransfered
}
