package pq

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq/internal/pqtest"
)

const copyTransactionIntegrationTimeout = 5 * time.Second

func TestCopyTransactionIntegrationUnfinishedCopy(t *testing.T) {
	pqtest.SkipPgbouncer(t) // Transaction pooling cannot prove physical-session reuse.
	pqtest.SkipPgpool(t)    // The proxy can replace the PostgreSQL backend independently.
	pqtest.SkipCockroach(t) // pg_terminate_backend is used to make a red run bounded.

	tests := []struct {
		name    string
		finish  func(*sql.Tx) error
		wantErr error
	}{
		{
			name:   "rollback",
			finish: (*sql.Tx).Rollback,
		},
		{
			name:    "commit",
			finish:  (*sql.Tx).Commit,
			wantErr: ErrInFailedTransaction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Keep an independent connection available to terminate the pinned
			// backend if a regression strands both protocol readers.
			admin := pqtest.MustDB(t)
			db := pqtest.MustDB(t)
			sqlConn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqlConn.Close() })

			var backendPID int
			if err := sqlConn.QueryRowContext(context.Background(),
				`select pg_backend_pid()`).Scan(&backendPID); err != nil {
				t.Fatal(err)
			}
			if _, err := sqlConn.ExecContext(context.Background(),
				`create temporary table copy_transaction_integration (value text)`); err != nil {
				t.Fatal(err)
			}

			tx, err := sqlConn.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			stmt, err := tx.PrepareContext(context.Background(),
				CopyIn("copy_transaction_integration", "value"))
			if err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			// Exceed copyin's buffer threshold so the row reaches PostgreSQL before
			// the transaction ends; the zero-row assertion below then proves rollback.
			if _, err := stmt.ExecContext(context.Background(),
				strings.Repeat("x", ciBufferFlushSize+1)); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}

			// Deliberately do not finish or close stmt. database/sql invokes the
			// driver's transaction method before closing transaction statements.
			finished := make(chan error, 1)
			go func() { finished <- tt.finish(tx) }()

			var finishErr error
			select {
			case finishErr = <-finished:
			case <-time.After(copyTransactionIntegrationTimeout):
				terminateCtx, cancel := context.WithTimeout(context.Background(), copyTransactionIntegrationTimeout)
				_, terminateErr := admin.ExecContext(terminateCtx,
					`select pg_terminate_backend($1)`, backendPID)
				cancel()
				if terminateErr != nil {
					t.Errorf("terminate blocked COPY backend: %v", terminateErr)
				}
				select {
				case <-finished:
				case <-time.After(copyTransactionIntegrationTimeout):
					t.Error("transaction end remained blocked after backend termination")
				}
				t.Fatalf("unfinished COPY %s did not complete within %s", tt.name, copyTransactionIntegrationTimeout)
			}

			if tt.wantErr == nil {
				if finishErr != nil {
					t.Fatalf("unfinished COPY %s: %v", tt.name, finishErr)
				}
			} else if !errors.Is(finishErr, tt.wantErr) {
				t.Fatalf("unfinished COPY %s error = %v; want %v", tt.name, finishErr, tt.wantErr)
			}

			verifyCtx, cancel := context.WithTimeout(context.Background(), copyTransactionIntegrationTimeout)
			defer cancel()
			var reusedPID, rows int
			if err := sqlConn.QueryRowContext(verifyCtx,
				`select pg_backend_pid()`).Scan(&reusedPID); err != nil {
				t.Fatalf("reuse connection after unfinished COPY %s: %v", tt.name, err)
			}
			if reusedPID != backendPID {
				t.Errorf("backend PID after unfinished COPY %s = %d; want same physical session %d",
					tt.name, reusedPID, backendPID)
			}
			if err := sqlConn.QueryRowContext(verifyCtx,
				`select count(*) from copy_transaction_integration`).Scan(&rows); err != nil {
				t.Fatalf("query after unfinished COPY %s: %v", tt.name, err)
			}
			if rows != 0 {
				t.Errorf("rows committed by unfinished COPY %s = %d; want 0", tt.name, rows)
			}
		})
	}
}
