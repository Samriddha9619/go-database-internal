package execution

import (
	"errors"
	"sync"

	//"golang.org/x/tools/go/analysis/passes/nilfunc"
	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager
	mu *sync.Mutex
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	types:= make([]common.Type,len(table.Columns))
	for i,col:=range table.Columns{
		types[i]=col.Type
	}
	return &TableHeap{
		oid:         table.Oid,
		desc:        storage.NewRawTupleDesc(types),
		bufferPool:  bufferPool,
		logManager:  logManager,
		lockManager: lockManager,
		mu : &sync.Mutex{},
	}, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	
	file,err:= tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
	if err !=nil{
		return  common.RecordID{},err
	}
	tableHeap.mu.Lock()
	defer tableHeap.mu.Unlock()
	numPages,err:=file.NumPages()
	if err!=nil{
		return common.RecordID{},err
	}

	if numPages>0{
		lastPage:= numPages-1
		pid:= common.PageID{Oid: tableHeap.oid,PageNum: int32(lastPage)}
		frame,err:= tableHeap.bufferPool.GetPage(pid)
		if err!=nil{
			return common.RecordID{},err
		}
		heapPage:=frame.AsHeapPage()
		frame.PageLatch.Lock()
		slot := heapPage.FindFreeSlot()
		if slot !=-1{
			rid:=common.RecordID{PageID:pid,Slot: int32(slot)}
			heapPage.MarkAllocated(rid,true)
			rawTuple:=heapPage.AccessTuple(rid)
			copy(rawTuple,row)
			frame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(frame,true)
			return rid,nil
		}
		frame.PageLatch.Unlock()
		if slot ==-1{
			tableHeap.bufferPool.UnpinPage(frame, false)
		}

	}
	newPage,err:= file.AllocatePage(1)
	if err!=nil{
		return common.RecordID{},err
	}
	pid := common.PageID{Oid: tableHeap.oid, PageNum: int32(newPage)}
	frame,err:= tableHeap.bufferPool.GetPage(pid)
	if err!=nil{
		return common.RecordID{},err
	}
	defer tableHeap.bufferPool.UnpinPage(frame,true)
	storage.InitializeHeapPage(tableHeap.desc,frame)
	heapPage:=frame.AsHeapPage()
	slot:=0
	rid:=common.RecordID{PageID:pid,Slot: int32(slot)}
	heapPage.MarkAllocated(rid,true)
	rawTuple:=heapPage.AccessTuple(rid)
	copy(rawTuple,row)
	return rid,nil
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	frame,err:= tableHeap.bufferPool.GetPage(rid.PageID)
	if err!=nil{
		return err
	}
	defer tableHeap.bufferPool.UnpinPage(frame,true)
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()
	heapPage:= frame.AsHeapPage()
	if !heapPage.IsAllocated(rid) || heapPage.IsDeleted(rid){
		return ErrTupleDeleted
	}
	heapPage.MarkDeleted(rid,true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	frame,err:= tableHeap.bufferPool.GetPage(rid.PageID)
	if err!=nil{
		return err
	}
	defer tableHeap.bufferPool.UnpinPage(frame,false)
	frame.PageLatch.RLock()
	defer frame.PageLatch.RUnlock()
	heapPage:= frame.AsHeapPage()
	if !heapPage.IsAllocated(rid) || heapPage.IsDeleted(rid){
		return ErrTupleDeleted
	}
	rawTuple := heapPage.AccessTuple(rid)
	copy(buffer,rawTuple)
	return nil

}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	frame,err:= tableHeap.bufferPool.GetPage(rid.PageID)
	if err!=nil{
		return err
	}
	defer tableHeap.bufferPool.UnpinPage(frame,true)
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()
	heapPage:= frame.AsHeapPage()
	if !heapPage.IsAllocated(rid) || heapPage.IsDeleted(rid){
		return ErrTupleDeleted
	}
	rawTuple:= heapPage.AccessTuple(rid)
	copy(rawTuple,updatedTuple)
	return  nil


}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	frame,err:= tableHeap.bufferPool.GetPage(pageID)
	if err!=nil{
		return err
	}
	defer tableHeap.bufferPool.UnpinPage(frame,true)
	heapPage:=frame.AsHeapPage()
	for i:=0;i<int(heapPage.NumSlots());i++{
		rid:= common.RecordID{PageID: pageID, Slot: int32(i)}
		if heapPage.IsDeleted(rid) || !heapPage.IsAllocated(rid){
			heapPage.MarkDeleted(rid,false)
			heapPage.MarkAllocated(rid,false)
		}
	}
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	file,err:= tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
	if err!=nil{
		return TableHeapIterator{},err
	}
	numPages,err:=file.NumPages()
	if err!=nil{
		return TableHeapIterator{},err
	}
	return TableHeapIterator{
	tableHeap:      tableHeap,
    buffer:         buffer,
    numPages:       numPages,
    currentPageNum: 0,
    currentSlot:    -1,
	},nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	tableHeap *TableHeap 
	buffer []byte
	numPages int
	currentPageNum int
	currentSlot int
	currentFrame *storage.PageFrame
	currentHeapPage storage.HeapPage
	err error
	hasLock bool
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it.tableHeap==nil
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	if it.hasLock{
		it.currentFrame.PageLatch.RUnlock()
		it.hasLock=false
	}
	for{
		it.currentSlot++
		if it.currentPageNum>=it.numPages{
			return false
		}
		if it.currentFrame==nil{
			pid:= common.PageID{Oid: it.tableHeap.oid,PageNum: int32(it.currentPageNum)}
			frame,err:= it.tableHeap.bufferPool.GetPage(pid)
			if err!=nil{
				it.err=err
				return false
			}
			heapPage:=frame.AsHeapPage()
			it.currentFrame=frame
			it.currentHeapPage=heapPage
		}
		if it.currentSlot>= int(it.currentHeapPage.NumSlots()){
			if it.hasLock{
				it.currentFrame.PageLatch.RUnlock()
				it.hasLock=false
			}
			it.tableHeap.bufferPool.UnpinPage(it.currentFrame,false)
			it.currentFrame=nil
			it.currentPageNum++
			it.currentSlot=-1
			continue
		}
		it.currentFrame.PageLatch.RLock()
		rid:= common.RecordID{PageID: common.PageID{Oid: it.tableHeap.oid,PageNum: int32(it.currentPageNum)},Slot: int32(it.currentSlot)}
		if it.currentHeapPage.IsAllocated(rid) && !it.currentHeapPage.IsDeleted(rid){
			it.hasLock=true
			return true
		}
		it.currentFrame.PageLatch.RUnlock()
	}
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	if it.currentFrame ==nil {
		return nil
	}
	rid := common.RecordID{PageID: common.PageID{Oid: it.tableHeap.oid,PageNum: int32(it.currentPageNum)},Slot: int32(it.currentSlot)}
	return it.currentHeapPage.AccessTuple(rid)
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	if it.currentFrame ==nil {
		return common.RecordID{}
	}
	return common.RecordID{PageID: common.PageID{Oid: it.tableHeap.oid,PageNum: int32(it.currentPageNum)},Slot: int32(it.currentSlot)}
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return it.err
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.hasLock{
		it.currentFrame.PageLatch.RUnlock()
		it.hasLock=false
	}
	if it.currentFrame != nil {
		it.tableHeap.bufferPool.UnpinPage(it.currentFrame,false)
		it.currentFrame=nil
	}
	return nil 
}
