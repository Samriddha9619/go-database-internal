package execution

import (
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	plan   *planner.SortNode
	child  Executor
	desc   *storage.RawTupleDesc 
	buffer []storage.Tuple       
	pos    int                   
	loaded bool                
	err    error
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	return &SortExecutor{plan: plan, child: child, pos: -1}
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	e.buffer = nil
	e.pos = -1
	e.loaded = false
	e.err = nil

	if err := e.child.Init(ctx); err != nil {
		return err
	}
	e.desc = storage.NewRawTupleDesc(e.child.PlanNode().OutputSchema())
	return nil
}

func (e *SortExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	if !e.loaded {
		for e.child.Next() {
			e.buffer = append(e.buffer, e.child.Current().DeepCopy(e.desc))
		}
		if err := e.child.Error(); err != nil {
			e.err = err
			return false
		}

		sort.SliceStable(e.buffer, func(i, j int) bool {
			return e.less(e.buffer[i], e.buffer[j])
		})
		e.loaded = true
	}

	if e.pos+1 >= len(e.buffer) {
		return false
	}
	e.pos++
	return true
}

func (e *SortExecutor) less(a, b storage.Tuple) bool {
	for _, ob := range e.plan.OrderBy {
		va := ob.Expr.Eval(a)
		vb := ob.Expr.Eval(b)

		c := va.Compare(vb) 
		if c == 0 {
			continue 
		}
		if ob.Direction == planner.SortOrderDescending {
			return c > 0
		}
		return c < 0
	}
	return false
}

func (e *SortExecutor) Current() storage.Tuple {
	return e.buffer[e.pos]
}

func (e *SortExecutor) Error() error {
	return e.err
}

func (e *SortExecutor) Close() error {
	e.buffer = nil
	return e.child.Close()
}

