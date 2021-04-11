package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

type MyReader struct {
	txn *badger.Txn
}

func (r *MyReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		return []byte(nil), nil
	}
	return val, nil
}

func (r *MyReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *MyReader) Close() {
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)

	if err != nil {
		log.Fatal(err)
	}

	return &StandAloneStorage{
		db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &MyReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, sub := range batch {
		switch sub.Data.(type) {
		case storage.Put:
			req := sub.Data.(storage.Put)
			engine_util.PutCF(s.db, req.Cf, req.Key, req.Value)
		case storage.Delete:
			req := sub.Data.(storage.Delete)
			engine_util.DeleteCF(s.db, req.Cf, req.Key)
		}
	}

	return nil
}
