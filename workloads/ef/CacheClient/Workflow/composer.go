package Workflow

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"tfaas/golang/types"

	"github.com/lechou-0/Client/CacheClient"
	"github.com/lechou-0/common/util"
	uuid "github.com/nu7hatch/gouuid"
)

var connectionCache *CacheClient.ConnectionCache
var clientLabel string
var cts int64
var sessionLock = &sync.Mutex{}
var env types.Environment

// var client = &http.Client{
// 	Transport: &http.Transport{
// 		MaxIdleConns:        100,
// 		MaxConnsPerHost:     50,
// 		MaxIdleConnsPerHost: 50,
// 	},
// }

type Input struct {
	In   map[string]any
	deps []string
}

type Workflow struct {
	states     map[string]*State
	workflowId string
	begin      string
	iscritical bool
}

func NewWorkflow(label string, connectioncache *CacheClient.ConnectionCache, new_env types.Environment) *Workflow {
	cts = -1
	clientLabel = label
	connectionCache = connectioncache
	env = new_env
	go func() {
		dataClient := CacheClient.Register("", nil, connectionCache)
		cts = dataClient.GetS(clientLabel)
		dataClient.CloseClient()
	}()

	uid, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("Unexpected error while generating UUID: %v", err)
	}
	return &Workflow{
		workflowId: uid.String(),
		states:     map[string]*State{},
	}
}

func CloseWorkflow(output *Input, err error) ([]byte, error) {
	if err != nil {
		return json.Marshal(map[string]any{"error": err.Error()})
	} else {
		return json.Marshal(output.In)
	}
}

func (w *Workflow) NewSequentialState(name string, actions ...*Action) {
	for i, ac := range actions {
		ac.seqnum = i
	}
	ss := &State{
		actions: actions,
		stype:   SequentialState,
	}

	w.states[name] = ss
}

func (w *Workflow) NewSwitchState(name string, condition func(map[string]any) int, actions ...*Action) {
	for i, ac := range actions {
		ac.seqnum = i
	}
	ss := &State{
		actions:   actions,
		stype:     SwitchState,
		condition: condition,
	}

	w.states[name] = ss
}

func (w *Workflow) NewLoopState(name string, condition func(map[string]any) int, actions ...*Action) {
	for i, ac := range actions {
		ac.seqnum = i
	}
	ss := &State{
		actions:   actions,
		stype:     LoopState,
		condition: condition,
	}

	w.states[name] = ss
}

func (w *Workflow) NewParallelState(name string, actions ...*Action) {
	ss := &State{
		actions: actions,
		stype:   ParallelState,
	}
	sumt := 0
	for i, ac := range actions {
		ac.seqnum = i
		if ac.atype == ExecTransaction {
			sumt++
		}
	}
	if sumt < 2 {
		ss.parallelFlag = true
	}
	w.states[name] = ss
}

func (w *Workflow) Transition(state1 string, state2 string) {
	if state1 == "" {
		w.begin = state2
	} else {
		w.states[state1].transition = state2
	}
}

func InitTransaction(createT func(input map[string]any, invoker *TInvoker) (map[string]any, error)) *Action {
	ac := &Action{
		atype: ExecTransaction,
		execTransaction: func(i *Input, b bool, tid string, iscritical bool) (*Input, error) {
			output := &Input{}
			invoker := &TInvoker{
				metaData: util.Metadata{},
			}
			invoker.env = env
			invoker.metaData.Tid = tid
			invoker.metaData.RwSet = map[string][]any{}
			var err error
			output.In, err = createT(i.In, invoker)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			dataClient := CacheClient.Register(tid, invoker.metaData.RwSet, connectionCache)
			for {
				if cts != -1 {
					break
				}
				time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
			}
			bout, err := json.Marshal(output.In)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			commit, t := CacheClient.Commit(dataClient, cts, clientLabel, iscritical, bout, i.deps)
			dataClient.CloseClient()
			sessionLock.Lock()
			if t > cts {
				cts = t
			}
			sessionLock.Unlock()
			if !commit {
				return nil, errors.New("abort")
			}
			output.deps = []string{tid}
			return output, nil
		},
	}
	return ac
}

func (w *Workflow) ExecFlow(input *Input, iscritical bool) (*Input, error) {
	w.iscritical = iscritical
	var err error
	begin := w.begin
	state := w.states[begin]
	state.stateId = w.workflowId + "-" + begin
	for {
		state.iscritical = w.iscritical
		input, err = state.execState(input)
		if err != nil {
			break
		}
		if state.transition == "" {
			break
		} else {
			next := state.transition
			state = w.states[next]
			state.stateId = w.workflowId + "-" + next
		}
	}

	return input, err
}

type State struct {
	stateId      string
	actions      []*Action
	condition    func(map[string]any) int
	stype        StateType
	iscritical   bool
	parallelFlag bool
	transition   string
}

func (s *State) execState(input *Input) (*Input, error) {
	var err error
	if s.stype == SequentialState {
		for _, ac := range s.actions {
			ac.iscritical = s.iscritical
			ac.actionId = s.stateId + "-" + strconv.Itoa(ac.seqnum)
			input, err = ac.execAction(input)
			if err != nil {
				log.Println(err)
				break
			}
		}
	} else if s.stype == SwitchState {
		acSeqnum := s.condition(input.In)
		if acSeqnum != -1 {
			s.actions[acSeqnum].iscritical = s.iscritical
			s.actions[acSeqnum].actionId = s.stateId + "-" + strconv.Itoa(s.actions[acSeqnum].seqnum)
			input, err = s.actions[acSeqnum].execAction(input)
			if err != nil {
				log.Println(err)
			}
		}
	} else if s.stype == LoopState {
		for {
			acSeqnum := s.condition(input.In)
			if acSeqnum != -1 {
				s.actions[acSeqnum].iscritical = s.iscritical
				s.actions[acSeqnum].actionId = s.stateId + "-" + strconv.Itoa(s.actions[acSeqnum].seqnum)
				input, err = s.actions[acSeqnum].execAction(input)
				if err != nil {
					log.Println(err)
				}
			} else {
				break
			}
		}
	} else if s.stype == ParallelState {
		if !s.parallelFlag {
			s.iscritical = false
		} else {
			branches := 0
			for _, ac := range s.actions {
				if testAction(ac, input) {
					branches++
				}
			}
			if branches > 1 {
				s.iscritical = false
			}
		}
		output := &Input{}
		outchan := make(chan *Input)
		errs := []error{}
		errchan := make(chan error)

		for _, ac := range s.actions {
			go func(input *Input, ac *Action) {
				ac.iscritical = s.iscritical
				ac.actionId = s.stateId + "-" + strconv.Itoa(ac.seqnum)
				out, err := ac.execAction(input)
				outchan <- out
				errchan <- err
			}(input, ac)
		}
		for i := 0; i < len(s.actions); i++ {
			out := <-outchan
			cerr := <-errchan
			if cerr != nil {
				if cerr.Error() == "abort" {
					err = cerr
				}
				errs = append(errs, err)
			} else {
				for k, v := range out.In {
					output.In[k] = v
				}
				output.deps = append(output.deps, out.deps...)
			}
		}
		if len(errs) != 0 && err == nil {
			err_string := ""
			for i := 0; i < len(errs); i++ {
				err_string = errs[i].Error() + "\n"
			}
			err = errors.New(err_string)
		}
		input = output
	}

	return input, err
}

type Action struct {
	actionId        string
	seqnum          int
	atype           ActionType
	execTransaction func(*Input, bool, string, bool) (*Input, error)
	subflow         *Workflow
	iscritical      bool
}

func NewActionByFlow(subflow *Workflow) *Action {
	return &Action{
		atype:   ExecSubflow,
		subflow: subflow,
	}
}

func (a *Action) execAction(input *Input) (*Input, error) {
	if a.atype == ExecTransaction {
		tid := a.actionId + ",0"

		return a.execTransaction(input, a.iscritical, tid, a.iscritical)
	} else if a.atype == ExecSubflow {
		a.subflow.workflowId = a.actionId
		return a.subflow.ExecFlow(input, a.iscritical)
	}
	return &Input{}, nil
}

func testWorkflow(w *Workflow, input *Input) bool {
	state := w.states[w.begin]
	for {
		if testState(state, input) {
			return true
		}
		if state.transition == "" {
			break
		} else {
			state = w.states[state.transition]
		}
	}

	return false
}

func testState(s *State, input *Input) bool {
	if s.stype == SequentialState {
		for _, ac := range s.actions {
			if testAction(ac, input) {
				return true
			}
		}
	} else if s.stype == SwitchState {
		acSeqnum := s.condition(input.In)
		if acSeqnum != -1 {
			return true
		}
	} else if s.stype == LoopState {
		acSeqnum := s.condition(input.In)
		if acSeqnum != -1 {
			return true
		}
	} else if s.stype == ParallelState {
		if !s.parallelFlag {
			return true
		} else {
			branches := 0
			for _, ac := range s.actions {
				if testAction(ac, input) {
					branches++
				}
			}
			if branches > 0 {
				return true
			}
		}
	}
	return false
}

func testAction(a *Action, input *Input) bool {
	if a.atype == ExecTransaction {
		return true
	} else if a.atype == ExecSubflow {
		return testWorkflow(a.subflow, input)
	}
	return false
}

type TInvoker struct {
	env      types.Environment
	metaData util.Metadata
}

// func (invoker *TInvoker) Invoke(url string, params map[string]any) (map[string]any, error) {
// 	in := &interData{
// 		MetaData: invoker.metaData,
// 		UserData: params,
// 	}
// 	reqBody, err := json.Marshal(in)
// 	if err != nil {
// 		log.Println("fail to marshal params")
// 		return nil, err
// 	}
// 	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
// 	if err != nil {
// 		log.Println(err)
// 	}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
// 	respBody, bodyErr := io.ReadAll(resp.Body)
// 	resp.Body.Close()
// 	if bodyErr != nil {
// 		log.Printf("Error reading body from request.")
// 		return nil, err
// 	}
// 	err = json.Unmarshal(respBody, in)
// 	if err != nil {
// 		log.Printf("fail to unmarshal resp1: %s", err)
// 		return nil, err
// 	}
// 	for k, v := range in.MetaData.RwSet {
// 		invoker.metaData.RwSet[k] = v
// 	}

// 	return in.UserData, nil
// }

func (invoker *TInvoker) Invoke(funcName string, params map[string]any) (map[string]any, error) {
	in := &util.InterData{
		MetaData: invoker.metaData,
		UserData: params,
	}
	reqBody, err := json.Marshal(in)
	if err != nil {
		log.Printf("fail to marshal params: %s", err)
		return nil, err
	}

	respBody, invokeErr := invoker.env.InvokeFunc(context.TODO(), funcName, reqBody)

	if invokeErr != nil {
		log.Printf("Error reading body from request.: %s", invokeErr)
		return nil, invokeErr
	}
	err = json.Unmarshal(respBody, in)
	if err != nil {
		log.Printf("fail to unmarshal resp1: %s", err)
		return nil, err
	}
	errString, ok := in.UserData["error"]
	if ok {
		return nil, errors.New(errString.(string))
	}
	for k, v := range in.MetaData.RwSet {
		invoker.metaData.RwSet[k] = v

	}
	return in.UserData, nil
}
