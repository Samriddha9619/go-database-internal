package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	planNode  *planner.IndexLookupNode
	index     indexing.Index
	tableHeap *TableHeap
	current   storage.Tuple
	buffer    []byte
	rids      []common.RecordID
	ridIdx    int
	ctx       *ExecutorContext
	lastErr   error
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	return &IndexLookupExecutor{
		planNode:  plan,
		index:     index,
		tableHeap: tableHeap,
		buffer:    make([]byte, tableHeap.StorageSchema().BytesPerTuple()),
	}
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	rids, err := e.index.ScanKey(e.planNode.EqualityKey, []common.RecordID{}, e.ctx.GetTransaction())
	if err != nil {
		return err
	}
	e.rids = rids
	e.ridIdx = 0
	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	if e.ridIdx < len(e.rids) {
		rid := e.rids[e.ridIdx]
		e.ridIdx++
		
		err := e.tableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.buffer, false)
		if err != nil {
			e.lastErr = err
			return false
		}
		e.current = storage.FromRawTuple(e.buffer, e.tableHeap.StorageSchema(), rid)
		return true
	}
	return false
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	return e.current
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.lastErr
}
