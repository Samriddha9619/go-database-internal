package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	planNode *planner.LimitNode
	child    Executor
	limit    int
	count    int
	current  storage.Tuple
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	return &LimitExecutor{
		planNode: plan,
		child:    child,
		limit:    plan.Limit,
	}
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	e.count = 0
	return e.child.Init(ctx)
}

func (e *LimitExecutor) Next() bool {
	if e.count >= e.limit {
		return false
	}
	
	if !e.child.Next() {
		return false
	}
	
	e.current = e.child.Current()
	e.count++
	return true
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.current
}

func (e *LimitExecutor) Error() error {
	return e.child.Error()
}

func (e *LimitExecutor) Close() error {
	return e.child.Close()
}
