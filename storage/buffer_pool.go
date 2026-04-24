package storage

import (
	"sync"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type BufferPool struct {
	// add more fields here...
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
	frame, ok := bp.pageTable.Load(pageID)
	if ok {
		frame.metaLatch.Lock()
		frame.pinCount++
		frame.recentlyUsed = true
		frame.metaLatch.Unlock()
		return frame, nil
	}
	bp.poolLatch.Lock()
	defer bp.poolLatch.Unlock()
	for {
		frame := bp.frames[bp.clockHand]
		frame.metaLatch.Lock()
		if frame.pinCount > 0 {
			frame.metaLatch.Unlock()
			bp.clockHand = (bp.clockHand + 1) % len(bp.frames)
		}
		if frame.pinCount == 0 && frame.recentlyUsed == true {
			frame.recentlyUsed = false
		}
		if frame.pinCount == 0 && frame.recentlyUsed == false {
			break
		}
	}
	err := bp.flushFrame(frame)
	if err!= nil{
		frame.metaLatch.Unlock()
		return nil,err
	}
	bp.pageTable.Delete(frame.pageID)
	dbFile,err:= bp.storageManager.GetDBFile(pageID.Oid)
	if err!=nil{
		frame.metaLatch.Unlock()
		return nil,err
	}
	err = dbFile.ReadPage(int(pageID.PageNum),frame.Bytes[:])
	if err!=nil{
		frame.metaLatch.Unlock()
		return nil,err
	}
	frame.pageID=pageID
	frame.pinCount=1
	frame.isDirty=false
	frame.recentlyUsed=true

	bp.pageTable.Store(pageID,frame)
	frame.metaLatch.Unlock()
	return frame,nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	frame.metaLatch.Lock()
	if frame.pinCount!=0{
	frame.pinCount=frame.pinCount-1
	}
	if setDirty==true{
		frame.isDirty=true
	}
	frame.metaLatch.Unlock()
}

func (bp *BufferPool) flushFrame(frame *PageFrame) error {
	if frame.isDirty == false {
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
	panic("unimplemented")
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is called during checkpoint to snapshot the current DPT into the log.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}
