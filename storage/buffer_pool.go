package storage

import (
	"runtime"
	"sync"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

const maxRefCount int32 = 2

type BufferPool struct {
	storageManager DBFileManager
	frames         []*PageFrame
	pageTable      *xsync.MapOf[common.PageID, *PageFrame]
	clockHand      int
	poolLatch      sync.Mutex
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages.
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

// GetPage retrieves a page from the buffer pool.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	for {
		frame, ok := bp.pageTable.Load(pageID)
		if !ok {
			break
		}
		frame.metaLatch.Lock()
		if frame.pageID != pageID {
			frame.metaLatch.Unlock()
			continue
		}
		frame.pinCount++
		if frame.refCount < maxRefCount {
			frame.refCount++
		}
		frame.metaLatch.Unlock()
		return frame, nil
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
		return bp.GetPage(pageID)
	}

	// CLOCK-Sweep victim selection.
	// First pass: decrement refCount by 1 (preserves scan resistance for hot pages).
	// Second pass: aggressively zero refCount (guarantees finding a victim quickly).
	var frame *PageFrame
	poolSize := len(bp.frames)

sweepRestart:
	passesAllowed := 2
	if poolSize > 1000 {
		passesAllowed = 1
	}
	for pass := 0; pass < passesAllowed; pass++ {
		for i := 0; i < poolSize; i++ {
			frame = bp.frames[bp.clockHand]

			if frame.pinCount == 0 {
				if frame.refCount == 0 {
					frame.metaLatch.Lock()
					if frame.pinCount == 0 && frame.refCount == 0 {
						goto foundVictim
					}
					frame.metaLatch.Unlock()
				} else {
					if pass == 0 && poolSize <= 1000 {
						frame.refCount--
					} else {
						frame.refCount = 0
					}
					if frame.refCount == 0 {
						frame.metaLatch.Lock()
						if frame.pinCount == 0 && frame.refCount == 0 {
							goto foundVictim
						}
						frame.metaLatch.Unlock()
					}
				}
			}
			bp.clockHand++
			if bp.clockHand >= poolSize {
				bp.clockHand = 0
			}
		}
	}

	bp.poolLatch.Unlock()
	runtime.Gosched()
	bp.poolLatch.Lock()

	if rframe, rok := bp.pageTable.Load(pageID); rok {
		bp.poolLatch.Unlock()
		rframe.metaLatch.Lock()
		if rframe.pageID == pageID {
			rframe.pinCount++
			if rframe.refCount < maxRefCount {
				rframe.refCount++
			}
			rframe.metaLatch.Unlock()
			return rframe, nil
		}
		rframe.metaLatch.Unlock()
		return bp.GetPage(pageID)
	}

	goto sweepRestart

foundVictim:

	oldPageID := frame.pageID
	frame.pinCount = 1
	frame.pageID = pageID
	bp.pageTable.Store(pageID, frame)
	bp.clockHand = (bp.clockHand + 1) % len(bp.frames)

	bp.poolLatch.Unlock()

	if frame.isDirty {
		dbFile, err := bp.storageManager.GetDBFile(oldPageID.Oid)
		if err != nil {
			bp.pageTable.Delete(pageID)
			bp.pageTable.Delete(oldPageID)
			frame.pinCount = 0
			frame.pageID = common.PageID{}
			frame.metaLatch.Unlock()
			return nil, err
		}
		err = dbFile.WritePage(int(oldPageID.PageNum), frame.Bytes[:])
		if err != nil {
			bp.pageTable.Delete(pageID)
			bp.pageTable.Delete(oldPageID)
			frame.pinCount = 0
			frame.pageID = common.PageID{}
			frame.metaLatch.Unlock()
			return nil, err
		}
		frame.isDirty = false
	}

	bp.pageTable.Delete(oldPageID)

	frame.refCount = 1
	dbFile, err := bp.storageManager.GetDBFile(pageID.Oid)
	if err != nil {
		bp.pageTable.Delete(pageID)
		frame.pinCount = 0
		frame.pageID = common.PageID{}
		frame.metaLatch.Unlock()
		return nil, err
	}
	err = dbFile.ReadPage(int(pageID.PageNum), frame.Bytes[:])
	if err != nil {
		bp.pageTable.Delete(pageID)
		frame.pinCount = 0
		frame.pageID = common.PageID{}
		frame.metaLatch.Unlock()
		return nil, err
	}

	frame.metaLatch.Unlock()
	return frame, nil
}

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

func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	panic("unimplemented")
}
