package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	planNode  *planner.UpdateNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	current   storage.Tuple
	done      bool
	lastErr   error
	ctx       *ExecutorContext
}

func NewUpdateExecutor(plan *planner.UpdateNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *UpdateExecutor {
	return &UpdateExecutor{
		planNode:  plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
	}
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.done = false
	return e.child.Init(ctx)
}

func (e *UpdateExecutor) Next() bool {
	if e.done {
		return false
	}
	
	e.done = true
	count := int64(0)
	
	var tuplesToUpdate []storage.Tuple
	for e.child.Next() {
		tuplesToUpdate = append(tuplesToUpdate, e.child.Current().DeepCopy(e.tableHeap.StorageSchema()))
	}
	
	for _, oldTuple := range tuplesToUpdate {
		rid := oldTuple.RID()
		
		newValues := make([]common.Value, oldTuple.NumColumns())
		for i := 0; i < oldTuple.NumColumns(); i++ {
			if e.planNode.Expressions[i] != nil {
				newValues[i] = e.planNode.Expressions[i].Eval(oldTuple)
			} else {
				newValues[i] = oldTuple.GetValue(i)
			}
		}
		
		newTuple := storage.FromValues(newValues...)
		rawBuf := make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
		newTuple.WriteToBuffer(rawBuf, e.tableHeap.StorageSchema())
		
		if err := e.tableHeap.UpdateTuple(e.ctx.GetTransaction(), rid, storage.RawTuple(rawBuf)); err != nil {
			e.lastErr = err
			return false
		}
		
		for _, idx := range e.indexes {
			md := idx.Metadata()
			
			oldKeyValues := make([]common.Value, len(md.ProjectionList))
			newKeyValues := make([]common.Value, len(md.ProjectionList))
			
			keyChanged := false
			for i, colIdx := range md.ProjectionList {
				oldKeyValues[i] = oldTuple.GetValue(colIdx)
				newKeyValues[i] = newValues[colIdx]
				
				if oldKeyValues[i].Compare(newKeyValues[i]) != 0 {
					keyChanged = true
				}
			}
			
			if keyChanged {
				oldKey := storage.FromValues(oldKeyValues...)
				oldKeyBuf := make([]byte, md.KeySchema.BytesPerTuple())
				oldKey.WriteToBuffer(oldKeyBuf, md.KeySchema)
				if err := idx.DeleteEntry(md.AsKey(oldKeyBuf), rid, e.ctx.GetTransaction()); err != nil {
					e.lastErr = err
					return false
				}
				
				newKey := storage.FromValues(newKeyValues...)
				newKeyBuf := make([]byte, md.KeySchema.BytesPerTuple())
				newKey.WriteToBuffer(newKeyBuf, md.KeySchema)
				if err := idx.InsertEntry(md.AsKey(newKeyBuf), rid, e.ctx.GetTransaction()); err != nil {
					e.lastErr = err
					return false
				}
			}
		}
		
		count++
	}
	
	e.current = storage.FromValues(common.NewIntValue(count))
	return true
}

func (e *UpdateExecutor) OutputSchema() []common.Type {
	return e.planNode.OutputSchema()
}

func (e *UpdateExecutor) Current() storage.Tuple {
	return e.current
}

func (e *UpdateExecutor) Close() error {
	return e.child.Close()
}

func (e *UpdateExecutor) Error() error {
	if e.lastErr != nil {
		return e.lastErr
	}
	return e.child.Error()
}
