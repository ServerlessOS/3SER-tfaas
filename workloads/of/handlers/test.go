package handlers

import (
	"context"
	"log"

	"tfaas/golang/types"
	"github.com/lechou-0/Client/CacheClient"
)

type testHandler struct {
	env types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewTestHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &testHandler{
		env: env,
		connectionCache: connectionCache,
	}
}

func (h *testHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	log.Println(string(input))
	return input, nil
}
