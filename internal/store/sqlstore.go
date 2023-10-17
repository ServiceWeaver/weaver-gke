// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/mattn/go-sqlite3"
)

// SQLStore is a sqlite-backed implementation of a Store. All of the data in a
// SQLStore is stored locally in a single sqlite file. SQLStore's data is
// stored in two tables: (1) a next_version table that stores a global,
// monotonically increasing integer-valued version, and (2) a data table that
// stores keys along with their values and versions.
//
// For example, if we execute the following code,
//
//	store, err := NewSQLStore("/tmp/weaver.db")
//	ctx := context.Background()
//	_, err = store.Put(ctx, "eggs", "green", nil)
//	_, err = store.Put(ctx, "ham", "green", nil)
//	_, err = store.Put(ctx, "sam", "unhappy", nil)
//	_, err = store.Put(ctx, "cat", "hatted", nil)
//	_, err = store.Put(ctx, "eggs", "scrambled", nil)
//	err = store.Delete(ctx, "ham")
//
// then the database stored in in "/tmp/weaver.db" would look something like
// this:
//
//	next_version   data
//	+---------+    +---------+--------------+----------+
//	| version |    | key     | value        | version  |
//	+---------+    +---------+--------------+----------+
//	|       5 |    | "eggs"  | "scrambled"  |        4 |
//	+---------+    | "sam"   | "unhappy"    |        2 |
//	               | "cat"   | "hatted"     |        3 |
//	               +---------+--------------+----------+
//
// All operations on the database are performed as part of strictly
// serializable transactions.
//
// TODO(mwhittaker): This implementation is not the most efficient. We can use
// some more advanced SQL to make it faster, if needed. For now, it's written
// to be as simple as possible.
type SQLStore struct {
	// Note that a sql.DB object is thread-safe, which is why SQLStore is
	// thread-safe. From [1]:
	//
	// > DB is a database handle representing a pool of zero or more underlying
	// > connections. It's safe for concurrent use by multiple goroutines.
	//
	// [1]: https://pkg.go.dev/database/sql#DB
	db *sql.DB
}

// Note [database is locked]
//
// SQLite only allows a single writer at a time. When multiple clients all
// concurrently try to update the store, some of them will attempt to lock the
// database for writing, will find that it's already locked, and will receive a
// "database is locked" error. For a thorough discussion of the "database is
// locked" error, see [1].
//
// We can employ two techniques to avoid these errors. First, we have
// db.SetMaxOpenConns(1) in NewSQLStore. This limits us to one database
// connection per process. This is similar to adding a mutex to all of
// SQLStore's methods. This helps prevent "database is locked" errors when
// multiple goroutines in the same process access the same SQLStore.
//
// db.SetMaxOpenConns(1) limits the number of database connections _per
// process_, but it doesn't limit the total number of database connections
// _across all processes_. If multiple processes access the same SQLStore
// concurrently, they may still receive "database is locked" errors. To avoid
// this, we implement every Store method in a retry loop. This way, if we
// receive a "database is locked" error, we retry with exponential backoff and
// jitter.
//
// There is another potential solution outlined in [1]. We can set a busy
// timeout on the database [2]. For example, if we call sql.Open("sqlite3",
// "db?_timeout=1000"), then all operation on the database retry for up to 1000
// ms if the database is locked. We tried this approach, but for whatever
// reason, found that it did not work as advertised. Processes were still
// receiving "database is locked" errors and were not waiting the full duration
// we set.
//
// [1]: https://github.com/mattn/go-sqlite3/issues/274
// [2]: https://www.sqlite.org/c3ref/busy_timeout.html

// options controls how an opened database behaves.
type options struct {
	// multiWriters specifies that other process may be writing to the db.
	multiWriters bool
}

// Check that SQLStore implements the Store interface.
var _ Store = &SQLStore{}

// NewSQLStore returns a new SQLStore with all of its data backed in a sqlite
// database stored in dbfile. Either dbfile already exists or it doesn't:
//
//   - If dbfile does not exist, then it is created, but the directory in
//     which dbfile is stored (i.e. filepath.Dir(dbfile)) must exist.
//   - If dbfile does exist, then the store's data is loaded from it. The
//     file must have been previously created by calling NewSQLStore.
func NewSQLStore(dbfile string) (Store, error) {
	db, err := newSqlStore(dbfile)
	if err != nil {
		return nil, err
	}
	return &SQLStore{db}, nil
}

func (s *SQLStore) Put(ctx context.Context, key, value string, version *Version) (*Version, error) {
	for r := retry.Begin(); r.Continue(ctx); {
		version, err := s.put(ctx, key, value, version)
		if isLocked(err) {
			continue
		}
		return version, err
	}
	return nil, ctx.Err()
}

func (s *SQLStore) Get(ctx context.Context, key string, version *Version) (string, *Version, error) {
	for r := retry.Begin(); r.Continue(ctx); {
		value, version, err := s.get(ctx, key, version)
		if isLocked(err) {
			continue
		}
		return value, version, err
	}
	return "", nil, ctx.Err()
}

func (s *SQLStore) Delete(ctx context.Context, key string) error {
	for r := retry.Begin(); r.Continue(ctx); {
		err := s.delete(ctx, key)
		if isLocked(err) {
			continue
		}
		return err
	}
	return ctx.Err()
}

func (s *SQLStore) List(ctx context.Context, opts ListOptions) ([]string, error) {
	for r := retry.Begin(); r.Continue(ctx); {
		keys, err := s.list(ctx, opts)
		if isLocked(err) {
			continue
		}
		return keys, err
	}
	return nil, ctx.Err()
}

func newSqlStore(dbfile string) (*sql.DB, error) {
	db, err := open(dbfile, options{multiWriters: true})
	if err != nil {
		return nil, err
	}

	const create = `
		CREATE TABLE IF NOT EXISTS next_version (
			version INTEGER PRIMARY KEY
		);

		CREATE TABLE IF NOT EXISTS data (
			key BLOB PRIMARY KEY,
			value BLOB,
			version INTEGER
		)
	`
	if _, err := db.Exec(create); err != nil {
		return nil, err
	}
	return db, nil
}

func txOpts() *sql.TxOptions {
	// TODO(mwhittaker): Double check that LevelLinearizable corresponds to
	// strictly serializable transactions.
	return &sql.TxOptions{Isolation: sql.LevelLinearizable}
}

func (s *SQLStore) getNextVersion(ctx context.Context, tx *sql.Tx) (int, error) {
	rows, err := tx.QueryContext(ctx, "SELECT version FROM next_version")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var version int

	// When we create the database, there is no version entry in the
	// next_version table, so we have to insert one.
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}

		version = 0
		const stmt = `
			INSERT INTO next_version(version)
			VALUES (?);
		`
		if _, err = tx.ExecContext(ctx, stmt, version); err != nil {
			return 0, err
		}
		return version, nil
	}

	// Otherwise, we read the existing version.
	if err = rows.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (s *SQLStore) getValue(ctx context.Context, tx *sql.Tx, key string) (string, Version, error) {
	const query = `
		SELECT value, version
		FROM data
		WHERE key = ?;
	`
	rows, err := tx.QueryContext(ctx, query, key)
	if err != nil {
		return "", Version{}, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", Version{}, err
		}
		return "", Missing, nil
	}

	var value []byte
	var versionInt int
	if err = rows.Scan(&value, &versionInt); err != nil {
		return "", Version{}, err
	}
	return string(value), Version{strconv.Itoa(versionInt)}, nil
}

func (s *SQLStore) unversionedGet(ctx context.Context, key string) (string, *Version, error) {
	tx, err := s.db.BeginTx(ctx, txOpts())
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback()

	value, latest, err := s.getValue(ctx, tx, key)
	if err != nil {
		return "", nil, err
	}

	if err = tx.Commit(); err != nil {
		return "", nil, err
	}

	return value, &latest, nil
}

// insert performs the following two actions, as part of tx:
//
//  1. It sets the value of key to value with version global in the data
//     table.
//  2. It increments the version number in the next_version table.
func (s *SQLStore) insert(ctx context.Context, tx *sql.Tx, key, value string, global int) error {
	const stmt = `
		INSERT INTO data(key, value, version)
		VALUES (?, ?, ?);

		UPDATE next_version
		SET version = ?;
	`
	_, err := tx.ExecContext(ctx, stmt, key, value, global, global+1)
	return err
}

func (s *SQLStore) update(ctx context.Context, tx *sql.Tx, key, value string, global int) error {
	const stmt = `
		UPDATE data
		SET value = ?,
		    version = ?
		WHERE key = ?;

		UPDATE next_version
		SET version = ?;
	`
	_, err := tx.ExecContext(ctx, stmt, value, global, key, global+1)
	return err
}

func (s *SQLStore) put(ctx context.Context, key, value string, version *Version) (*Version, error) {
	// Start the transaction.
	tx, err := s.db.BeginTx(ctx, txOpts())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Fetch the global version.
	global, err := s.getNextVersion(ctx, tx)
	if err != nil {
		return nil, err
	}

	// Fetch the key.
	_, latestVersion, err := s.getValue(ctx, tx, key)
	if err != nil {
		return nil, err
	}

	// If version is nil, then we perform a blind write, and don't have to
	// check any versions. If version is not nil, then the write succeeds only
	// if the provided version is equal to current version.
	if version != nil && *version != latestVersion {
		return nil, NewStale(*version, &latestVersion)
	}

	if latestVersion == Missing {
		if err = s.insert(ctx, tx, key, value, global); err != nil {
			return nil, err
		}
	} else {
		if err = s.update(ctx, tx, key, value, global); err != nil {
			return nil, err
		}
	}

	// Commit the transaction.
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &Version{strconv.Itoa(global)}, nil
}

func (s *SQLStore) get(ctx context.Context, key string, version *Version) (string, *Version, error) {
	if version == nil {
		return s.unversionedGet(ctx, key)
	}

	// Unfortunately, there is no convenient and efficient way to detect
	// changes in the SQLite database, so we implement watches with polling.
	// The delay of 500ms is arbitrary.
	//
	// TODO(mwhittaker): Put more thought into a good timer duration.
	//
	// TODO(mwhittaker): We can behave like the FakeStore and have Put and
	// Delete notify Get. If another process calls Put or Delete, we won't get
	// notified but we'll catch it in our poll.
	//
	// TODO(mwhittaker): If we're issuing too many queries to the database, we
	// could have a single goroutine perform the polling.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()

		case <-ticker.C:
			value, latest, err := s.unversionedGet(ctx, key)
			if err != nil || *latest != *version {
				return value, latest, err
			}
		}
	}
}

func (s *SQLStore) delete(ctx context.Context, key string) error {
	tx, err := s.db.BeginTx(ctx, txOpts())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const stmt = `
		DELETE FROM data
		WHERE key = ?;
	`
	_, err = tx.ExecContext(ctx, stmt, key)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// list returns the list of keys currently present in the store.
func (s *SQLStore) list(ctx context.Context, opts ListOptions) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, txOpts())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	query := `SELECT key FROM data WHERE key LIKE '` + opts.Prefix + `%'`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := []string{}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

// open opens the sqlite3 database stored in filename. filename may contain
// trailing parameters as specified in the sqlite3 documentation.
func open(filename string, opt options) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}

	// TODO(mwhittaker): Pass in a context and call db.PingContext.
	if err := db.Ping(); err != nil {
		return nil, err
	}

	if opt.multiWriters {
		// See note above.
		db.SetMaxOpenConns(1)
	}

	return db, nil
}

// isLocked returns whether the error is a "database is locked" error. See Note above.
func isLocked(err error) bool {
	var sqlError sqlite3.Error
	ok := errors.As(err, &sqlError)
	return ok && (sqlError.Code == sqlite3.ErrBusy || sqlError.Code == sqlite3.ErrLocked)
}
