package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type AggregateExecutor struct {
	plan    *planner.AggregateNode
	child   Executor
	results []storage.Tuple
	pos     int
	err     error
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	return &AggregateExecutor{plan: plan, child: child, pos: -1}
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	e.results = nil
	e.pos = -1
	e.err = nil

	if err := e.child.Init(ctx); err != nil {
		return err
	}

	groupTypes := make([]common.Type, len(e.plan.GroupByClause))
	for i, expr := range e.plan.GroupByClause {
		groupTypes[i] = expr.OutputType()
	}
	groupDesc := storage.NewRawTupleDesc(groupTypes)
	groupBuffer := make([]byte, groupDesc.BytesPerTuple())

	type aggregateState struct {
		values []common.Value
		seen   []bool
	}

	groups := make(map[string]int)
	groupValues := make([][]common.Value, 0)
	states := make([]*aggregateState, 0)

	for e.child.Next() {
		tuple := e.child.Current()
		keyValues := make([]common.Value, len(e.plan.GroupByClause))
		for i, expr := range e.plan.GroupByClause {
			keyValues[i] = expr.Eval(tuple).Copy()
		}

		keyTuple := storage.FromValues(keyValues...)
		keyTuple.WriteToBuffer(groupBuffer, groupDesc)
		key := string(groupBuffer)

		stateIdx, exists := groups[key]
		if !exists {
			stateIdx = len(states)
			groups[key] = stateIdx
			states = append(states, &aggregateState{
				values: make([]common.Value, len(e.plan.AggClauses)),
				seen:   make([]bool, len(e.plan.AggClauses)),
			})
			groupCopy := append([]common.Value(nil), keyValues...)
			groupValues = append(groupValues, groupCopy)
		}

		state := states[stateIdx]
		for i, clause := range e.plan.AggClauses {
			value := clause.Expr.Eval(tuple)
			switch clause.Type {
			case planner.AggCount:
				if !value.IsNull() {
					if !state.seen[i] {
						state.values[i] = common.NewIntValue(1)
						state.seen[i] = true
					} else {
						state.values[i] = common.NewIntValue(state.values[i].IntValue() + 1)
					}
				}
			case planner.AggSum:
				if !value.IsNull() {
					if !state.seen[i] {
						state.values[i] = value.Copy()
						state.seen[i] = true
					} else {
						state.values[i] = common.NewIntValue(state.values[i].IntValue() + value.IntValue())
					}
				}
			case planner.AggMin:
				if !value.IsNull() {
					if !state.seen[i] || value.Compare(state.values[i]) < 0 {
						state.values[i] = value.Copy()
						state.seen[i] = true
					}
				}
			case planner.AggMax:
				if !value.IsNull() {
					if !state.seen[i] || value.Compare(state.values[i]) > 0 {
						state.values[i] = value.Copy()
						state.seen[i] = true
					}
				}
			}
		}
	}

	if err := e.child.Error(); err != nil {
		e.err = err
		return err
	}

	if len(e.plan.GroupByClause) == 0 && len(states) == 0 {
		return nil
	}

	for i, state := range states {
		output := make([]common.Value, 0, len(e.plan.GroupByClause)+len(e.plan.AggClauses))
		output = append(output, groupValues[i]...)
		for j, clause := range e.plan.AggClauses {
			if state.seen[j] {
				output = append(output, state.values[j].Copy())
				continue
			}
			if clause.Type == planner.AggCount {
				output = append(output, common.NewIntValue(0))
			} else {
				output = append(output, common.NewNullInt())
			}
		}
		e.results = append(e.results, storage.FromValues(output...))
	}

	if len(e.plan.GroupByClause) == 0 && len(e.results) == 0 {
		e.results = nil
	}

	return nil
}

func (e *AggregateExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	if e.pos+1 >= len(e.results) {
		return false
	}
	e.pos++
	return true
}

func (e *AggregateExecutor) Current() storage.Tuple {
	return e.results[e.pos]
}

func (e *AggregateExecutor) Error() error {
	return e.err
}

func (e *AggregateExecutor) Close() error {
	e.results = nil
	return e.child.Close()
}
