package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type InsertExecutor struct {
	planNode  *planner.InsertNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	current   storage.Tuple
	done      bool
	lastErr   error
	ctx       *ExecutorContext
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	return &InsertExecutor{
		planNode:  plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
	}
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.done = false
	return e.child.Init(ctx)
}

func (e *InsertExecutor) Next() bool {
	if e.done {
		return false
	}
	
	e.done = true
	count := int64(0)
	
	for e.child.Next() {
		childTuple := e.child.Current()
		
		rawBuf := make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
		childTuple.WriteToBuffer(rawBuf, e.tableHeap.StorageSchema())
		
		rid, err := e.tableHeap.InsertTuple(e.ctx.GetTransaction(), storage.RawTuple(rawBuf))
		if err != nil {
			e.lastErr = err
			return false
		}
		
		for _, idx := range e.indexes {
			md := idx.Metadata()
			keyValues := make([]common.Value, len(md.ProjectionList))
			for i, colIdx := range md.ProjectionList {
				keyValues[i] = childTuple.GetValue(colIdx)
			}
			
			key := storage.FromValues(keyValues...)
			keyBuf := make([]byte, md.KeySchema.BytesPerTuple())
			key.WriteToBuffer(keyBuf, md.KeySchema)
			
			if err := idx.InsertEntry(md.AsKey(keyBuf), rid, e.ctx.GetTransaction()); err != nil {
				e.lastErr = err
				return false
			}
		}
		count++
	}
	
	e.current = storage.FromValues(common.NewIntValue(count))
	return true
}

func (e *InsertExecutor) Current() storage.Tuple {
	return e.current
}

func (e *InsertExecutor) Close() error {
	return e.child.Close()
}

func (e *InsertExecutor) Error() error {
	if e.lastErr != nil {
		return e.lastErr
	}
	return e.child.Error()
}
