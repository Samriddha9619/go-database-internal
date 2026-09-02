package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// FilterExecutor filters tuples from its child executor based on a predicate.
type FilterExecutor struct {
	planNode *planner.FilterNode
	child    Executor
	current  storage.Tuple
}

// NewFilter creates a new FilterExecutor executor.
func NewFilter(plan *planner.FilterNode, child Executor) *FilterExecutor {
	return &FilterExecutor{
		planNode: plan,
		child:    child,
	}
}

func (e *FilterExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

// Init initializes the child.
func (e *FilterExecutor) Init(ctx *ExecutorContext) error {
	return e.child.Init(ctx)
}

func (e *FilterExecutor) Next() bool {
	for e.child.Next() {
		tuple := e.child.Current()
		predicate := e.planNode.Predicate
		val := predicate.Eval(tuple)
		if planner.ExprIsTrue(val) {
			e.current = tuple
			return true
		}
	}
	return false
}

func (e *FilterExecutor) Current() storage.Tuple {
	return e.current
}

func (e *FilterExecutor) Error() error {
	return e.child.Error()
}

func (e *FilterExecutor) Close() error {
	return e.child.Close()
}
