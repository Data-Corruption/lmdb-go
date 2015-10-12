package lmdb

/*
#include "lmdb.h"
*/
import "C"

import (
	"fmt"
	"os"
	"syscall"
)

type OpError struct {
	Op    string
	Errno error
}

func (err *OpError) Error() string {
	return fmt.Sprintf("%s: %s", err.Op, err.Errno)
}

const (
	// Error codes defined by LMDB.  See the list of LMDB return codes for more
	// information about each
	//
	//		http://symas.com/mdb/doc/group__errors.html

	KeyExist        Errno = C.MDB_KEYEXIST
	NotFound        Errno = C.MDB_NOTFOUND
	PageNotFound    Errno = C.MDB_PAGE_NOTFOUND
	Corrupted       Errno = C.MDB_CORRUPTED
	Panic           Errno = C.MDB_PANIC
	VersionMismatch Errno = C.MDB_VERSION_MISMATCH
	Invalid         Errno = C.MDB_INVALID
	MapFull         Errno = C.MDB_MAP_FULL
	DBsFull         Errno = C.MDB_DBS_FULL
	ReadersFull     Errno = C.MDB_READERS_FULL
	TLSFull         Errno = C.MDB_TLS_FULL
	TxnFull         Errno = C.MDB_TXN_FULL
	CursorFull      Errno = C.MDB_CURSOR_FULL
	PageFull        Errno = C.MDB_PAGE_FULL
	MapResized      Errno = C.MDB_MAP_RESIZED
	Incompatible    Errno = C.MDB_INCOMPATIBLE
	BadRSlot        Errno = C.MDB_BAD_RSLOT
	BadTxn          Errno = C.MDB_BAD_TXN
	BadValSize      Errno = C.MDB_BAD_VALSIZE
	BadDBI          Errno = C.MDB_BAD_DBI
)

// Errno is an error type that represents the (unique) errno values defined by
// LMDB.  Other errno values (such as EINVAL) are represented with type
// syscall.Errno.
//
// Most often helper functions such as IsNotFound may be used instead of
// dealing with Errno values directly.
//
//		lmdb.IsNotFound(err)
//		lmdb.IsErrno(err, lmdb.TxnFull)
//		lmdb.IsErrnoSys(err, syscall.EINVAL)
//		lmdb.IsErrnoFn(err, os.IsPermission)
type Errno C.int

// minimum and maximum values produced for the Errno type. syscall.Errnos of
// other values may still be produced.
const minErrno, maxErrno C.int = C.MDB_KEYEXIST, C.MDB_LAST_ERRCODE

func (e Errno) Error() string {
	return C.GoString(C.mdb_strerror(C.int(e)))
}

// _operrno is for use by tests that can't import C
func _operrno(op string, ret int) error {
	return operrno(op, C.int(ret))
}

func operrno(op string, ret C.int) error {
	if ret == C.MDB_SUCCESS {
		return nil
	}
	if minErrno <= ret && ret <= maxErrno {
		return &OpError{Op: op, Errno: Errno(ret)}
	}
	return &OpError{Op: op, Errno: syscall.Errno(ret)}
}

// IsNotFound returns true if the key requested in Txn.Get or Cursor.Get does
// not exist or if the Cursor reached the end of the database without locating
// a value (EOF).
func IsNotFound(err error) bool {
	return IsErrno(err, NotFound)
}

// IsNotExist returns true the path passed to the Env.Open method does not
// exist.
func IsNotExist(err error) bool {
	return IsErrnoFn(err, os.IsNotExist)
}

// IsMapFull returns true if the environment map size has been reached.
func IsMapFull(err error) bool {
	return IsErrno(err, MapFull)
}

// IsResized returns true if the environment has grown.
func IsMapResized(err error) bool {
	return IsErrno(err, MapResized)
}

// IsErrno returns true if err's errno is the given errno.
func IsErrno(err error, errno Errno) bool {
	return IsErrnoFn(err, func(err error) bool { return err == errno })
}

// IsErrnoSys returns true if err's errno is the given errno.
func IsErrnoSys(err error, errno syscall.Errno) bool {
	return IsErrnoFn(err, func(err error) bool { return err == errno })
}

// IsErrnoFn calls fn on the error underlying err and returns the result.  If
// err is an *OpError then err.Errno is passed to fn.  Otherwise err is passed
// directly to fn.
func IsErrnoFn(err error, fn func(error) bool) bool {
	if err == nil {
		return false
	}
	if err, ok := err.(*OpError); ok {
		return fn(err.Errno)
	}
	return fn(err)
}
