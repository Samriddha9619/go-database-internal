package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	planNode *planner.ProjectionNode
	child    Executor
	current  storage.Tuple
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	return &ProjectionExecutor{
		planNode: plan,
		child:    child,
	}
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	return e.child.Init(ctx)
}

func (e *ProjectionExecutor) Next() bool {
	if !e.child.Next() {
		return false
	}
	
	inputTuple := e.child.Current()
	exprs := e.planNode.Expressions
	projectedValues := make([]common.Value, len(exprs))
	
	for i, expr := range exprs {
		projectedValues[i] = expr.Eval(inputTuple)
	}
	
	e.current = storage.FromValues(projectedValues...)
	return true
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	return e.current
}

func (e *ProjectionExecutor) Error() error {
	return e.child.Error()
}

func (e *ProjectionExecutor) Close() error {
	return e.child.Close()
}
