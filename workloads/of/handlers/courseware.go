package handlers

import (
	"context"
	"encoding/json"
	"log"

	"tfaas/golang/types"

	"github.com/lechou-0/Client/CacheClient"
	"github.com/lechou-0/Client/Workflow"
)

type DeleteWorkflowHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewDeleteWorkflowHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &DeleteWorkflowHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *DeleteWorkflowHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	delete := map[string]any{}
	err := json.Unmarshal(input, &delete)
	if err != nil {
		log.Printf("fail to unmarshal DeleteWorkflow: %s", err)
		return nil, err
	}
	w := Workflow.NewWorkflow(delete["client"].(string), h.connectionCache, h.env)
	t1 := Workflow.InitTransaction(createDeleteWorkflowT)
	w.NewSequentialState("1", t1)
	w.Transition("", "1")
	w.Transition("1", "")
	out, err := w.ExecFlow(&Workflow.Input{In: delete}, true)
	return Workflow.CloseWorkflow(out, err)
}

func createDeleteWorkflowT(input map[string]any, invoker *Workflow.TInvoker) (map[string]any, error) {
	out, err := invoker.Invoke("isDelete", input)
	if err != nil {
		return nil, err
	}

	if out["is"].(bool) {
		out, err := invoker.Invoke("delete", out)
		if err != nil {
			return nil, err
		}

		if out["success"].(bool) {
			return map[string]any{"success": true}, nil
		} else {
			return map[string]any{"success": false, "message": "failed to delete"}, nil
		}
	} else {
		return map[string]any{"success": true, "message": "can't be deleted"}, nil
	}
}

type RegisterWorkflowHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewRegisterWorkflowHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &RegisterWorkflowHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *RegisterWorkflowHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	register := map[string]any{}
	err := json.Unmarshal(input, &register)
	if err != nil {
		log.Printf("fail to unmarshal RegisterWorkflow: %s", err)
		return nil, err
	}
	w := Workflow.NewWorkflow(register["client"].(string), h.connectionCache, h.env)
	t1 := Workflow.InitTransaction(createRegisterWorkflowT)
	w.NewSequentialState("1", t1)
	w.Transition("", "1")
	w.Transition("1", "")
	out, err := w.ExecFlow(&Workflow.Input{In: register}, true)
	return Workflow.CloseWorkflow(out, err)
}

func createRegisterWorkflowT(input map[string]any, invoker *Workflow.TInvoker) (map[string]any, error) {
	out, err := invoker.Invoke("isRegister", input)
	if err != nil {
		return nil, err
	}

	if out["is"].(bool) {
		out, err := invoker.Invoke("register", out)
		if err != nil {
			return nil, err
		}

		if out["success"].(bool) {
			return map[string]any{"success": true}, nil
		} else {
			return map[string]any{"success": false, "message": "failed to register"}, nil
		}
	} else {
		return map[string]any{"success": true, "message": "can't be registered"}, nil
	}
}

type ResetCourseWorkflowHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewResetCourseWorkflowHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &ResetCourseWorkflowHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *ResetCourseWorkflowHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	reset := map[string]any{}
	err := json.Unmarshal(input, &reset)
	if err != nil {
		log.Printf("fail to unmarshal ResetCourseWorkflow: %s", err)
		return nil, err
	}
	w := Workflow.NewWorkflow(reset["client"].(string), h.connectionCache, h.env)
	t1 := Workflow.InitTransaction(createResetCourseWorkflowT)
	w.NewSequentialState("1", t1)
	w.Transition("", "1")
	w.Transition("1", "")
	out, err := w.ExecFlow(&Workflow.Input{In: reset}, true)
	return Workflow.CloseWorkflow(out, err)
}

func createResetCourseWorkflowT(input map[string]any, invoker *Workflow.TInvoker) (map[string]any, error) {
	out, err := invoker.Invoke("resetCourse", input)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type ListCoursesWorkflowHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewListCoursesWorkflowHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &ListCoursesWorkflowHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *ListCoursesWorkflowHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	listcourses := map[string]any{}
	err := json.Unmarshal(input, &listcourses)
	if err != nil {
		log.Printf("fail to unmarshal ListCoursesWorkflow: %s", err)
		return nil, err
	}
	w := Workflow.NewWorkflow(listcourses["client"].(string), h.connectionCache, h.env)
	t1 := Workflow.InitTransaction(createListCoursesWorkflowT)
	w.NewSequentialState("1", t1)
	w.Transition("", "1")
	w.Transition("1", "")
	out, err := w.ExecFlow(&Workflow.Input{In: listcourses}, true)
	return Workflow.CloseWorkflow(out, err)
}

func createListCoursesWorkflowT(input map[string]any, invoker *Workflow.TInvoker) (map[string]any, error) {
	out, err := invoker.Invoke("listCourses", input)
	if err != nil {
		return nil, err
	}

	return out, nil
}
