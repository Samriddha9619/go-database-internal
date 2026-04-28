package storage

import (
	"runtime"
	"sync"
	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

const maxRefCount int32 = 2


// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).

type BufferPool struct {
	storageManager DBFileManager
	frames         []*PageFrame
	pageTable      *xsync.MapOf[common.PageID, *PageFrame]
	clockHand      int
	poolLatch      sync.Mutex
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	bp := &BufferPool{
		storageManager: storageManager,
		frames:         make([]*PageFrame, numPages),
		pageTable:      xsync.NewMapOf[common.PageID, *PageFrame](),
		clockHand:      0,
	}
	for i := 0; i < numPages; i++ {
		bp.frames[i] = &PageFrame{}
	}
	return bp
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	for {
		if frame, ok := bp.pageTable.Load(pageID); ok {
			frame.metaLatch.Lock()
			if frame.pageID == pageID {
				frame.pinCount++
				if frame.refCount < maxRefCount {
					frame.refCount++
				}
				frame.metaLatch.Unlock()
				return frame, nil
			}
			frame.metaLatch.Unlock()
			continue
		}

		bp.poolLatch.Lock()

		if frame, ok := bp.pageTable.Load(pageID); ok {
			bp.poolLatch.Unlock()
			frame.metaLatch.Lock()
			if frame.pageID == pageID {
				frame.pinCount++
				if frame.refCount < maxRefCount {
					frame.refCount++
				}
				frame.metaLatch.Unlock()
				return frame, nil
			}
			frame.metaLatch.Unlock()
			continue
		}

		var victim *PageFrame
		poolSize := len(bp.frames)
		passesAllowed := 2
		if poolSize > 1000 {
			passesAllowed = 1
		}

		for pass := 0; pass < passesAllowed && victim == nil; pass++ {
			for i := 0; i < poolSize; i++ {
				frame := bp.frames[bp.clockHand]
				if frame.pinCount == 0 {
					frame.metaLatch.Lock()
					if frame.pinCount == 0 {
						if frame.refCount == 0 {
							victim = frame
							break
						} else {
							if pass == 0 && poolSize <= 1000 {
								frame.refCount--
							} else {
								frame.refCount = 0
							}
							if frame.refCount == 0 {
								victim = frame
								break
							}
						}
					}
					if victim == nil {
						frame.metaLatch.Unlock()
					}
				}
				if victim == nil {
					bp.clockHand = (bp.clockHand + 1) % poolSize
				}
			}
		}

		if victim == nil {
			bp.poolLatch.Unlock()
			runtime.Gosched()
			continue
		}

		oldPageID := victim.pageID
		victim.pinCount = 1
		victim.pageID = pageID
		bp.pageTable.Store(pageID, victim)
		bp.clockHand = (bp.clockHand + 1) % poolSize

		bp.poolLatch.Unlock()

		if victim.isDirty {
			dbFile, err := bp.storageManager.GetDBFile(oldPageID.Oid)
			if err != nil {
				bp.pageTable.Delete(pageID)
				bp.pageTable.Delete(oldPageID)
				victim.pinCount = 0
				victim.pageID = common.PageID{}
				victim.metaLatch.Unlock()
				return nil, err
			}
			err = dbFile.WritePage(int(oldPageID.PageNum), victim.Bytes[:])
			if err != nil {
				bp.pageTable.Delete(pageID)
				bp.pageTable.Delete(oldPageID)
				victim.pinCount = 0
				victim.pageID = common.PageID{}
				victim.metaLatch.Unlock()
				return nil, err
			}
			victim.isDirty = false
		}

		bp.pageTable.Delete(oldPageID)

		victim.refCount = 1
		dbFile, err := bp.storageManager.GetDBFile(pageID.Oid)
		if err != nil {
			bp.pageTable.Delete(pageID)
			victim.pinCount = 0
			victim.pageID = common.PageID{}
			victim.metaLatch.Unlock()
			return nil, err
		}
		err = dbFile.ReadPage(int(pageID.PageNum), victim.Bytes[:])
		if err != nil {
			bp.pageTable.Delete(pageID)
			victim.pinCount = 0
			victim.pageID = common.PageID{}
			victim.metaLatch.Unlock()
			return nil, err
		}

		victim.metaLatch.Unlock()
		return victim, nil
	}
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	frame.metaLatch.Lock()
	if frame.pinCount != 0 {
		frame.pinCount--
	}
	if setDirty {
		frame.isDirty = true
	}
	frame.metaLatch.Unlock()
}

func (bp *BufferPool) flushFrame(frame *PageFrame) error {
	if !frame.isDirty {
		return nil
	}
	dbFile, err := bp.storageManager.GetDBFile(frame.pageID.Oid)
	if err != nil {
		return err
	}
	err = dbFile.WritePage(int(frame.pageID.PageNum), frame.Bytes[:])
	if err != nil {
		return err
	}
	frame.isDirty = false
	return nil
}


// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	for i := 0; i < len(bp.frames); i++ {
		frame := bp.frames[i]
		frame.metaLatch.Lock()
		err := bp.flushFrame(frame)
		if err != nil {
			frame.metaLatch.Unlock()
			return err
		}
		frame.metaLatch.Unlock()
	}
	return nil
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is called during checkpoint to snapshot the current DPT into the log.
//
// Hint: You do not need to worry about this function until lab 4

func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}
