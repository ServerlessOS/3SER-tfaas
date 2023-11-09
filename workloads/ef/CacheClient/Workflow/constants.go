package Workflow

type StateType int
type ActionType int

const (
	SequentialState StateType  = 0
	SwitchState     StateType  = 1
	LoopState       StateType  = 2
	ParallelState   StateType  = 3
	ExecTransaction ActionType = 0
	ExecSubflow     ActionType = 1
)
