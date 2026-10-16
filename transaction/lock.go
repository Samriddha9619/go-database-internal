package transaction

import (
	"fmt"
	"sync"
	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

// LockManager manages the granting, releasing, and waiting of locks on database resources.
type LockManager struct {
	// Add fields here
	mu sync.Mutex
	cond *sync.Cond
	// locks: Global granted lock table.
	// Key 1 (DBLockTag): The resource being locked (Table or Tuple).
	// Key 2 (TransactionID): The transaction holding the lock.
	// Value (DBLockMode): The permission level (S, X, IS, IX, etc.).
	locks map[DBLockTag]map[common.TransactionID]DBLockMode
	waitqueue map[DBLockTag][]common.TransactionID
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	lm:= &LockManager{
		locks: make(map[DBLockTag]map[common.TransactionID]DBLockMode),
		waitqueue: make(map[DBLockTag][]common.TransactionID),
	}
	lm.cond=sync.NewCond(&lm.mu)
	return lm
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if holders, exists := lm.locks[tag]; exists {
		if currentMode, held := holders[tid]; held { // held is just the boolean value indicating if the transaction holds a lock on the resource
			if currentMode == mode {
				return nil
			}
			delete(holders,tid)
			if len(holders) == 0 {
				delete(lm.locks, tag)
			}
		}
	}

	conflict:= false
	if holders, exists := lm.locks[tag]; exists {
		for otherTid, otherMode:= range holders {
			if otherTid ==tid{
				continue
			}
			if !isCompatible(mode, otherMode) {
				conflict = true
				break
			}
		}
	}
	if conflict{
		lm.waitqueue[tag] = append(lm.waitqueue[tag], tid)
		
		if lm.hasDeadLock(tid, tag) {
			lm.removeFromWaitQueue(tid,tag)
			return common.GoDBError{Code: common.DeadlockError, ErrString: "deadlock detected"}
		}

		for {
			conflict=false
			if holders, exists := lm.locks[tag]; exists {
				for otherTid, otherMode := range holders {
					if otherTid == tid {
						continue
					}
					if !isCompatible(mode, otherMode) {
						conflict = true
						break
					}
				}
			}

			if !conflict {
				break
			}

			lm.cond.Wait()
		}

		lm.removeFromWaitQueue(tid, tag)
	}

	if _, exists := lm.locks[tag]; !exists {
		lm.locks[tag] = make(map[common.TransactionID]DBLockMode)
	}
	lm.locks[tag][tid] = mode

	return nil
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	holders,exists := lm.locks[tag]
	if !exists{
		return common.GoDBError{Code: common.LockNotFoundError, ErrString: "lock not found"}
	}

	if _, held := holders[tid]; !held {
		return common.GoDBError{Code: common.LockNotFoundError, ErrString: "lock not found"}
	}
	delete(holders, tid)

	if len(holders)==0{
		delete(lm.locks, tag)
	}
	
	if waiters,waiting :=lm.waitqueue[tag]; waiting && len(waiters)>0{
		lm.cond.Broadcast()
	}
	return nil
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
		lm.mu.Lock()
		defer lm.mu.Unlock()

		holders:=lm.locks[tag]

		if holders==nil || len(holders)==0{
			return false
		}
		return true
}


// isCompatible checks if requestedMode can be granted while existingMode is held.
func isCompatible(requestedMode DBLockMode, existingMode DBLockMode) bool {
    
    // 1. Exclusive (X) clashes with absolutely everything.
    if requestedMode == LockModeX || existingMode == LockModeX {
        return false
    }

    // 2. Shared (S) only gets along with S, IS, and SIX.
    if requestedMode == LockModeS {
        if existingMode == LockModeS || existingMode == LockModeIS || existingMode == LockModeSIX {
            return true
        }
        return false
    }
    if existingMode == LockModeS {
        if requestedMode == LockModeS || requestedMode == LockModeIS || requestedMode == LockModeSIX {
            return true
        }
        return false
    }

    // 3. Intent Exclusive (IX) gets along with IS and IX.
    if requestedMode == LockModeIX {
        if existingMode == LockModeIS || existingMode == LockModeIX {
            return true
        }
        return false
    }
    if existingMode == LockModeIX {
        if requestedMode == LockModeIS || requestedMode == LockModeIX {
            return true
        }
        return false
    }

    // 4. Shared Intent Exclusive (SIX) gets along with IS and S.
    if requestedMode == LockModeSIX {
        if existingMode == LockModeIS || existingMode == LockModeS {
            return true
        }
        return false
    }
    if existingMode == LockModeSIX {
        if requestedMode == LockModeIS || requestedMode == LockModeS {
            return true
        }
        return false
    }

    // 5. Intent Shared (IS) gets along with everything except X (which we already checked in step 1).
    // So if we made it here, it's safe.
    return true
}


// imp things this will help in deadlock detection also when a broadcast is called it wakes up everyone and the first person who wakes up or is more compatiable gets the lock 
func (lm *LockManager) removeFromWaitQueue(tid common.TransactionID, tag DBLockTag) {
	waiters:= lm.waitqueue[tag]
	for i, waitingTid := range waiters {
		if waitingTid == tid {
//removing in a slice by adjusting the whole slice is slower O(N) than hashmap O(1) but becase of how CPU;s work a smaller slice like 10 locks it will be faster with slice 			
			lm.waitqueue[tag]=append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(lm.waitqueue[tag])==0{
		delete(lm.waitqueue, tag)
	}
}



func (lm *LockManager) hasDeadLock(startID common.TransactionID, tag DBLockTag) bool{
	visited := make(map[common.TransactionID]bool)
	return lm.dfs(startID,startID,visited)
}

func(lm *LockManager) dfs(currentId common.TransactionID, startId common.TransactionID, visited map[common.TransactionID]bool) bool{
	if visited[currentId] {	
		return currentId == startId
	}

	visited[currentId] = true

	for waitTag, waiters := range lm.waitqueue {
		for _, waitingTid := range waiters {
			if waitingTid == currentId {

				if holders, exists := lm.locks[waitTag]; exists {
					for holderTid := range holders {
						if holderTid != currentId {
							if lm.dfs(holderTid, startId, visited) {
								return true
							}
						}
					}
				}
			}
		}
	}
	return false
}