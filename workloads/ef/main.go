package main

import (
	"errors"
	"log"

	"tfaas/executor/handlers"

	"tfaas/golang"
	"tfaas/golang/types"
	"github.com/lechou-0/Client/CacheClient"
)

var connectionCache *CacheClient.ConnectionCache

type funcHandlerFactory struct {
	connectionCache *CacheClient.ConnectionCache
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	if funcName == "delete" {
		return handlers.NewDeleteHandler(env, f.connectionCache), nil
	} else if funcName == "register" {
		return handlers.NewRegisterHandler(env, f.connectionCache), nil
	} else if funcName == "isDelete" {
		return handlers.NewIsDeleteHandler(env, f.connectionCache), nil
	} else if funcName == "isRegister" {
		return handlers.NewIsRegisterHandler(env, f.connectionCache), nil
	} else if funcName == "listCourses" {
		return handlers.NewListCoursesHandler(env, f.connectionCache), nil
	} else if funcName == "resetCourse" {
		return handlers.NewResetCourseHandler(env, f.connectionCache), nil
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
