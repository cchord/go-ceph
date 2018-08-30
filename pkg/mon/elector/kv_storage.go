package elector

type KVStorage interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error

	PutInt64(key []byte, value int64) error
	GetInt64(key []byte) (int64, error)
}
