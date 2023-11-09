package main

import (
	"errors"
	"log"

	"tfaas/workflow/handlers"

	"tfaas/golang"
	"tfaas/golang/types"
	"github.com/lechou-0/Client/CacheClient"
)

var connectionCache *CacheClient.ConnectionCache

type funcHandlerFactory struct {
	connectionCache *CacheClient.ConnectionCache
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	if funcName == "deleteworkflow" {
		return handlers.NewDeleteWorkflowHandler(env, f.connectionCache), nil
	} else if funcName == "registerworkflow" {
		return handlers.NewRegisterWorkflowHandler(env, f.connectionCache), nil
	} else if funcName == "resetworkflow" {
		return handlers.NewResetCourseWorkflowHandler(env, f.connectionCache), nil
	} else if funcName == "listCoursesworkflow" {
		return handlers.NewListCoursesWorkflowHandler(env, f.connectionCache), nil
	} else if funcName == "test" {
		return handlers.NewTestHandler(env, f.connectionCache), nil
	} else {
		return nil, nil
	}
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, errors.New("not implemented")
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	connectionCache = CacheClient.CreateCC()
	defer connectionCache.CloseConnection()
	faas.Serve(&funcHandlerFactory{connectionCache: connectionCache})
}
