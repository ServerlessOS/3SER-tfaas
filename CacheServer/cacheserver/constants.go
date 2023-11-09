package cacheserver

type ctxString string

const (
	policy string    = "consistent_hash_policy"
	ctxKey ctxString = "key"
	scheme string    = "transactionmanager"
)
