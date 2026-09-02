package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	planNode  *planner.SeqScanNode
	tableHeap *TableHeap
	iterator  TableHeapIterator
	current   storage.Tuple
	buffer    []byte
	ctx       *ExecutorContext
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	return &SeqScanExecutor{
		planNode:  plan,
		tableHeap: tableHeap,
		buffer:    make([]byte, tableHeap.StorageSchema().BytesPerTuple()),
	}
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *SeqScanExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	iter, err := e.tableHeap.Iterator(ctx.GetTransaction(), e.planNode.Mode, e.buffer)
	e.iterator = iter
	return err
}

func (e *SeqScanExecutor) Next() bool {
	if !e.iterator.Next() {
		return false
	}
	e.current = storage.FromRawTuple(
		e.iterator.CurrentTuple(),
		e.tableHeap.StorageSchema(),
		e.iterator.CurrentRID(),
	)
	return true
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	return e.current
}

func (e *SeqScanExecutor) Error() error {
	return e.iterator.Error()
}

func (e *SeqScanExecutor) Close() error {
	return e.iterator.Close()
}
