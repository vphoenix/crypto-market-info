package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

type temporaryStorageError struct{ error }

func (e temporaryStorageError) Temporary() bool { return true }
func (e temporaryStorageError) Unwrap() error   { return e.error }

// database/sql can finish its cancellation rollback before the caller's next
// statement. Preserve that attempt's context cause instead of treating ErrTxDone
// as a permanent storage failure. An unexplained ErrTxDone remains an error.
// Call before cancelling the attempt context on ordinary function return.
func transactionError(ctx context.Context, err error) error {
	if errors.Is(err, sql.ErrTxDone) && ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	return storageError(err)
}

func storageError(err error) error {
	if err == nil {
		return nil
	}
	var sqlErr *mysqlDriver.MySQLError
	var network net.Error
	retry := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysqlDriver.ErrInvalidConn) || errors.As(err, &network)
	if errors.As(err, &sqlErr) {
		switch sqlErr.Number {
		case 1205, 1213, 1226, 1317, 2006, 2013, 3024:
			retry = true
		}
	}
	if retry {
		return temporaryStorageError{err}
	}
	return err
}
