package runner

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/casualjim/dat"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

const (
	txPending = iota
	txCommitted
	txRollbacked
	txErred
)

// ErrTxRollbacked occurs when Commit() or Rollback() is called on a
// transaction that has already been rollbacked.
var ErrTxRollbacked = errors.New("Nested transaction already rolled back")

// Tx is a transaction for the given Session
type Tx struct {
	sync.Mutex
	*sqlx.Tx
	*Queryable
	IsRollbacked bool
	state        int
	stateStack   []int
}

// WrapSqlxTx creates a Tx from a sqlx.Tx
func WrapSqlxTx(tx *sqlx.Tx) *Tx {
	newtx := &Tx{Tx: tx, Queryable: &Queryable{tx}}
	if dat.Strict {
		time.AfterFunc(1*time.Minute, func() {
			if !newtx.IsRollbacked && newtx.state == txPending {
				panic("A database transaction was not closed!")
			}
		})
	}
	return newtx
}

// Begin creates a transaction for the given database
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Beginx()
	if err != nil {
		if dat.Strict {
			logger.Fatal("Could not create transaction")
		}
		logger.Error("begin.error", zap.Error(err))
		return nil, err
	}
	logger.Debug("begin tx")
	return WrapSqlxTx(tx), nil
}

// Begin returns this transaction
func (tx *Tx) Begin() (*Tx, error) {
	tx.Lock()
	defer tx.Unlock()
	if tx.IsRollbacked {
		return nil, ErrTxRollbacked
	}

	logger.Debug("begin nested tx")
	tx.pushState()
	return tx, nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.IsRollbacked {
		logger.Error("Cannot commit", zap.Error(ErrTxRollbacked))
		return ErrTxRollbacked
	}

	if tx.state == txCommitted {
		logger.Error("Transaction has already been commited")
		return errors.New("transaction has already been commited")
	}
	if tx.state == txRollbacked {
		logger.Error("Transaction has already been rolled back")
		return errors.New("transaction has already been rolled back")
	}

	if len(tx.stateStack) == 0 {
		err := tx.Tx.Commit()
		if err != nil {
			tx.state = txErred
			logger.Error("commit.error", zap.Error(err))
			return err
		}
	}

	logger.Debug("commit")
	tx.state = txCommitted
	return nil
}

// Rollback cancels the transaction
func (tx *Tx) Rollback() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.IsRollbacked {
		logger.Error("Cannot rollback", zap.Error(ErrTxRollbacked))
		return ErrTxRollbacked
	}
	if tx.state == txCommitted {
		logger.Error("Cannot rollback, transaction has already been commited")
		return errors.New("cannot rollback, transaction has already been commited")
	}

	// rollback is sent to the database even in nested state
	err := tx.Tx.Rollback()
	if err != nil {
		tx.state = txErred
		logger.Error("Unable to rollback", zap.Error(err))
		return fmt.Errorf("Unable to rollback: %v", err)
	}

	logger.Debug("rollback")
	tx.state = txRollbacked
	tx.IsRollbacked = true
	return nil
}

// AutoCommit commits a transaction IF neither Commit or Rollback were called.
func (tx *Tx) AutoCommit() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.state == txRollbacked || tx.IsRollbacked {
		tx.popState()
		return nil
	}

	err := tx.Tx.Commit()
	if err != nil {
		tx.state = txErred
		if dat.Strict {
			logger.Fatal("Could not commit transaction", zap.Error(err))
		}
		tx.popState()
		logger.Error("transaction.AutoCommit.commit_error", zap.Error(err))
		return err
	}
	logger.Debug("autocommit")
	tx.state = txCommitted
	tx.popState()
	return err
}

// AutoRollback rolls back transaction IF neither Commit or Rollback were called.
func (tx *Tx) AutoRollback() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.IsRollbacked || tx.state == txCommitted {
		tx.popState()
		return nil
	}

	err := tx.Tx.Rollback()
	if err != nil {
		tx.state = txErred
		if dat.Strict {
			logger.Fatal("Could not rollback transaction", zap.Error(err))
		}
		tx.popState()
		logger.Error("transaction.AutoRollback.rollback_error", zap.Error(err))
		return fmt.Errorf("transaction.AutoRollback.rollback_error: %v", err)
	}
	logger.Debug("autorollback")
	tx.state = txRollbacked
	tx.IsRollbacked = true
	tx.popState()
	return err
}

// Select creates a new SelectBuilder for the given columns.
// This disambiguates between Queryable.Select and sqlx's Select
func (tx *Tx) Select(columns ...string) *dat.SelectBuilder {
	return tx.Queryable.Select(columns...)
}

func (tx *Tx) pushState() {
	tx.stateStack = append(tx.stateStack, tx.state)
	tx.state = txPending
}

func (tx *Tx) popState() {
	if len(tx.stateStack) == 0 {
		return
	}

	var val int
	val, tx.stateStack = tx.stateStack[len(tx.stateStack)-1], tx.stateStack[:len(tx.stateStack)-1]
	tx.state = val
}
