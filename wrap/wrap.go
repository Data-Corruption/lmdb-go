// Package wrap provides a thin, opinionated abstraction for the most common LMDB operations.
package wrap

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Data-Corruption/lmdb-go/lmdb"
)

const MapSize = 10 * 1 << 30 // 10 GB

var (
	ErrDuplicateDbName = errors.New("duplicate database name")
	ErrDbNameNotFound  = errors.New("database name not found")
	ErrDBClosed        = errors.New("database is closed")
	ErrEmptyKey        = errors.New("empty key")
)

// updateOp is a struct used to pass LMDB write operations to an OS thread-locked goroutine.
//
// see https://pkg.go.dev/github.com/bmatsuo/lmdb-go/lmdb?utm_source=godoc#hdr-Caveats
type updateOp struct {
	op  lmdb.TxnOp
	res chan<- error
}

// DB represents a simple LMDB database wrapper.
type DB struct {
	env       *lmdb.Env
	dbs       map[string]lmdb.DBI // handle is just a uint, safe to cache for the lifetime of the DB
	uOps      chan *updateOp
	wg        sync.WaitGroup // for closing the update goroutine cleanly
	closeOnce sync.Once
	closed    uint32
}

// New creates (or opens) an LMDB environment at the specified directory path and initializes the given databases.
// If the directory does not exist, it will be created. Remember to call Close() on the returned DB
// to cleanly shut down the environment. Returns the DB pointer, the number of stale readers cleared, and any error.
func New(dirPath string, dbNames []string) (*DB, int, error) {

	// Ensure the database names are unique
	seen := make(map[string]struct{})
	for _, n := range dbNames {
		if _, ok := seen[n]; ok {
			return nil, 0, ErrDuplicateDbName
		}
		seen[n] = struct{}{}
	}

	// Ensure the directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, 0, err
	}

	// Create DB struct and open the environment
	newDB := &DB{dbs: make(map[string]lmdb.DBI), uOps: make(chan *updateOp, 1000)}

	var err error
	newDB.env, err = lmdb.NewEnv()
	if err != nil {
		return nil, 0, err
	}
	if err = newDB.env.SetMaxDBs(len(dbNames)); err != nil {
		return nil, 0, err
	}
	if err = newDB.env.SetMapSize(MapSize); err != nil {
		return nil, 0, err
	}
	if err = newDB.env.Open(dirPath, 0, 0644); err != nil {
		return nil, 0, err
	}

	// Check for stale readers and clear them
	staleReaders, err := newDB.env.ReaderCheck()
	if err != nil {
		newDB.env.Close()
		return nil, 0, err
	}

	// Open each database handle
	for _, name := range dbNames {
		err = newDB.env.Update(func(txn *lmdb.Txn) (err error) {
			newDB.dbs[name], err = txn.CreateDBI(name)
			return err
		})
		if err != nil {
			newDB.env.Close()
			return nil, staleReaders, err
		}
	}

	// Start issuing update operations in an OS thread-locked goroutine
	newDB.wg.Add(1)
	go func() {
		runtime.LockOSThread()
		defer func() {
			runtime.UnlockOSThread()
			newDB.wg.Done()
		}()
		for op := range newDB.uOps {
			op.res <- newDB.env.UpdateLocked(op.op)
		}
	}()

	return newDB, staleReaders, nil
}

// Read retrieves a value from the database.
func (db *DB) Read(dbName string, key []byte) ([]byte, error) {
	dbi, err := db.validateArgs(dbName, key)
	if err != nil {
		return nil, err
	}
	// read the value
	var val []byte
	err = db.env.View(func(txn *lmdb.Txn) (err error) {
		val, err = txn.Get(dbi, key)
		return err
	})
	return val, err
}

// Write inserts a key/value pair into the database.
func (db *DB) Write(dbName string, key, value []byte) error {
	dbi, err := db.validateArgs(dbName, key)
	if err != nil {
		return err
	}
	// write the key/value pair
	return db.Update(func(txn *lmdb.Txn) error {
		return txn.Put(dbi, key, value, 0)
	})
}

// Delete removes a key/value pair from the database.
func (db *DB) Delete(dbName string, key []byte) error {
	dbi, err := db.validateArgs(dbName, key)
	if err != nil {
		return err
	}
	// delete the key/value pair
	return db.Update(func(txn *lmdb.Txn) error {
		return txn.Del(dbi, key, nil)
	})
}

// Update runs an LMDB transaction.
//
// Usage:
//
//	err := db.Update(func(txn *lmdb.Txn) error {
//		dbi := db.GetDBis()["users"]
//		data, err := txn.Get(dbi, []byte("user:123"))
//		if err != nil {
//			return err
//		}
//		if !shouldUpdate(data) {
//			return nil
//		}
//		return txn.Put(dbi, []byte("user:123"), update(data), 0)
//	})
func (db *DB) Update(op lmdb.TxnOp) error {
	if atomic.LoadUint32(&db.closed) != 0 {
		return ErrDBClosed
	}
	res := make(chan error)
	db.uOps <- &updateOp{op, res}
	return <-res
}

// View runs a read-only LMDB transaction.
//
// Usage:
//
//	err := db.View(func(txn *lmdb.Txn) error {
//		dbi := db.GetDBis()["users"]
//		data, err := txn.Get(dbi, []byte("user:123"))
//		if err != nil {
//			return err
//		}
//		process(data)
//		return nil
//	})
func (db *DB) View(op lmdb.TxnOp) error {
	if atomic.LoadUint32(&db.closed) != 0 {
		return ErrDBClosed
	}
	return db.env.View(op)
}

// GetDBis returns a copy of database names to DBI handle mappings.
func (db *DB) GetDBis() map[string]lmdb.DBI {
	dbis := make(map[string]lmdb.DBI, len(db.dbs))
	for k, v := range db.dbs {
		dbis[k] = v
	}
	return dbis
}

// Close cleanly shuts down the LMDB environment.
func (db *DB) Close() {
	db.closeOnce.Do(func() {
		atomic.StoreUint32(&db.closed, 1)
		close(db.uOps)
		db.wg.Wait()
		db.env.Close()
	})
}

// validateArgs is a helper for Read, Write, and Delete argument parsing.
func (db *DB) validateArgs(dbName string, key []byte) (lmdb.DBI, error) {
	if dbName == "" {
		return 0, ErrDbNameNotFound
	}
	if (key == nil) || (len(key) == 0) {
		return 0, ErrEmptyKey
	}
	dbi, ok := db.dbs[dbName]
	if !ok {
		return 0, ErrDbNameNotFound
	}
	return dbi, nil
}
