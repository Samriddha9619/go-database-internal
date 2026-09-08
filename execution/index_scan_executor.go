package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	planNode  *planner.IndexScanNode
	index     indexing.Index
	tableHeap *TableHeap
	iterator  indexing.ScanIterator
	current   storage.Tuple
	buffer    []byte
	ctx       *ExecutorContext
	lastErr   error
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	return &IndexScanExecutor{
		planNode:  plan,
		index:     index,
		tableHeap: tableHeap,
		buffer:    make([]byte, tableHeap.StorageSchema().BytesPerTuple()),
	}
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	iter, err := e.index.Scan(
		e.planNode.StartKey,
		e.planNode.Direction,
		ctx.GetTransaction(),
	)
	e.iterator = iter
	return err
}

func (e *IndexScanExecutor) Next() bool {
	if !e.iterator.Next() {
		return false
	}
	
	rid := e.iterator.Value()
	
	err := e.tableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.buffer, false)
	if err != nil {
		e.lastErr = err
		return false
	}
	
	e.current = storage.FromRawTuple(e.buffer, e.tableHeap.StorageSchema(), rid)
	return true
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return e.current
}

func (e *IndexScanExecutor) Close() error {
	if e.iterator != nil {
		return e.iterator.Close()
	}
	return nil
}

func (e *IndexScanExecutor) Error() error {
	if e.lastErr != nil {
		return e.lastErr
	}
	if e.iterator != nil {
		return e.iterator.Error()
	}
	return nil
}
