package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	plan *planner.NestedLoopJoinNode
	left Executor
	right Executor
	ctx *ExecutorContext

	leftDesc *storage.RawTupleDesc // descriptor for left tuple 
	joinedDesc *storage.RawTupleDesc // descriptor for joined tuple descriptor for right tuple is not needed
	joinedBuf []byte // buffer for joined tuples

	current storage.Tuple
	err  error
	leftblock []storage.Tuple
	blockidx int
	right_has_cur bool
	// Fill me in!
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	return &BlockNestedLoopJoinExecutor{
		plan :plan,
		right : right,
		left : left,
	}
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx =ctx
	if err := e.left.Init(ctx); err != nil {
		return err
	}
	if err := e.right.Init(ctx); err != nil {
		return err
	}
	left_types:=e.left.PlanNode().OutputSchema()
	e.leftDesc = storage.NewRawTupleDesc(left_types)
	right_types:=e.right.PlanNode().OutputSchema()
	joined_types := append(left_types,right_types...)
	e.joinedDesc = storage.NewRawTupleDesc(joined_types)
	e.joinedBuf = make([]byte,e.joinedDesc.BytesPerTuple())

	e.leftblock = make([]storage.Tuple, 0)
	e.blockidx = 0
	e.right_has_cur = false
	
	return e.loadLeftBlock()
}

func (e *BlockNestedLoopJoinExecutor) loadLeftBlock() error {
	e.leftblock = e.leftblock[:0] 
	usedSize := 0
	for usedSize < blockSize && e.left.Next() {
		if e.left.Error() != nil {
			return e.left.Error()
		}
		tuple :=e.left.Current().DeepCopy(e.leftDesc)
		e.leftblock = append(e.leftblock,tuple)
		usedSize+= tuplesize(tuple)
	}

	return nil
}

func tuplesize(t storage.Tuple) int {
	size:=0
	for i:=0;i<t.NumColumns();i++{
		size+=t.GetValue(i).SizeInBytes()
	}
	return size
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	for{
		if len(e.leftblock) == 0 {
			return false
		}
		if e.right_has_cur == false {
			if !e.right.Next() {
				if e.right.Error() != nil {
					e.err =e.right.Error()
					return false
				}
				if err := e.loadLeftBlock(); err != nil {
					e.err = err
					return false
				}
				if len(e.leftblock)==0{
					return false
				}
				if err:= e.right.Init(e.ctx);err!=nil{
					e.err=err
					return false
				}
				continue

			}
			e.right_has_cur = true
			e.blockidx =0
		}

		for e.blockidx < len(e.leftblock){
			lefttuple := e.leftblock[e.blockidx]
			e.blockidx++
			righttuple := e.right.Current()
			joined:= storage.MergeTuples(e.joinedBuf,e.joinedDesc,lefttuple,righttuple)
			val:=e.plan.Predicate.Eval(joined)
			if planner.ExprIsTrue(val){
				e.current = joined
				return true
			}
		}
		e.right_has_cur=false
	}
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return  e.current
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	return e.err
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	if err:=e.left.Close();err !=nil{
		return err
	}

	return e.right.Close() //return is error so if right right.close has error it will be returned 
}
