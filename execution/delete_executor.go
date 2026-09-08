package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	planNode  *planner.DeleteNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	current   storage.Tuple
	done      bool
	lastErr   error
	ctx       *ExecutorContext
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	return &DeleteExecutor{
		planNode:  plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
	}
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.done = false
	return e.child.Init(ctx)
}

func (e *DeleteExecutor) Next() bool {
	if e.done {
		return false
	}
	
	e.done = true
	count := int64(0)
	
	var tuplesToDelete []storage.Tuple
	for e.child.Next() {
		tuplesToDelete = append(tuplesToDelete, e.child.Current().DeepCopy(e.tableHeap.StorageSchema()))
	}
	
	for _, tuple := range tuplesToDelete {
		rid := tuple.RID()
		
		for _, idx := range e.indexes {
			md := idx.Metadata()
			
			keyValues := make([]common.Value, len(md.ProjectionList))
			for i, colIdx := range md.ProjectionList {
				keyValues[i] = tuple.GetValue(colIdx)
			}
			
			key := storage.FromValues(keyValues...)
			keyBuf := make([]byte, md.KeySchema.BytesPerTuple())
			key.WriteToBuffer(keyBuf, md.KeySchema)
			
			if err := idx.DeleteEntry(md.AsKey(keyBuf), rid, e.ctx.GetTransaction()); err != nil {
				e.lastErr = err
				return false
			}
		}
		
		if err := e.tableHeap.DeleteTuple(e.ctx.GetTransaction(), rid); err != nil {
			e.lastErr = err
			return false
		}
		
		count++
	}
	
	e.current = storage.FromValues(common.NewIntValue(count))
	return true
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return e.current
}

func (e *DeleteExecutor) Close() error {
	return e.child.Close()
}

func (e *DeleteExecutor) Error() error {
	if e.lastErr != nil {
		return e.lastErr
	}
	return e.child.Error()
}
