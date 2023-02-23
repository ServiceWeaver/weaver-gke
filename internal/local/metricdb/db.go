// Copyright 2023 Google LLC
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

package metricdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/mattn/go-sqlite3"
	"golang.org/x/exp/maps"
)

// T represents a metrics database.
type T struct {
	// We store metrics in a sqlite3 DB spread across three tables:
	// (1) defs:    definitions of known metrics.
	// (2) labels:  labels attached to metric definitions.
	// (3) records: recorded metric values.
	fname string
	db    *sql.DB

	mu         sync.RWMutex
	cachedDefs map[uint64]struct{}
}

// Open opens the default metric database on the local machine, creating
// it if necessary.
func Open(ctx context.Context) (*T, error) {
	dataDir := os.Getenv("XDG_DATA_HOME")
	if dataDir == "" {
		// Default to ~/.local/share
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		dataDir = filepath.Join(home, ".local", "share")
	}
	dataDir = filepath.Join(dataDir, "serviceweaver")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	fname := filepath.Join(dataDir, "gke_local_metrics.db")
	return OpenFile(ctx, fname)
}

// OpenFile opens the metric database stored in the provided file, creating
// it if necessary.
// Useful for tests where using the default database isn't desirable.
func OpenFile(ctx context.Context, fname string) (*T, error) {
	// The DB may be opened by multiple writers. Turn on appropriate
	// concurrency control options. See:
	//   https://www.sqlite.org/pragma.html#pragma_locking_mode
	//   https://www.sqlite.org/pragma.html#pragma_busy_timeout
	const params = "?_locking_mode=NORMAL&_busy_timeout=10000"
	db, err := sql.Open("sqlite3", fname+params)
	if err != nil {
		return nil, fmt.Errorf("open metric db %q: %w", fname, err)
	}

	// See internal/store/sqlstore.go for a discussion on how the following
	// helps with concurrency.
	db.SetMaxOpenConns(1)

	s := &T{
		fname:      fname,
		db:         db,
		cachedDefs: map[uint64]struct{}{},
	}

	const initTables = `
-- Defined metrics
CREATE TABLE IF NOT EXISTS defs (
	id INTEGER NOT NULL PRIMARY KEY,
	type INTEGER NOT NULL,
	name TEXT NOT NULL,
	bounds TEXT,
	help TEXT
);

-- Defined metric labels
CREATE TABLE IF NOT EXISTS labels (
	id INTEGER NOT NULL,
	label TEXT NOT NULL,
	value TEXT NOT NULL,
	PRIMARY KEY(id,label),
	FOREIGN KEY (id) REFERENCES defs (id)
);

-- Recorded metric values
CREATE TABLE IF NOT EXISTS records (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	value REAL NOT NULL,
	counts TEXT NOT NULL,
	FOREIGN KEY (id) REFERENCES defs (id)
);
`
	if _, err := s.execDB(ctx, initTables); err != nil {
		return nil, fmt.Errorf("open metric DB %s: %w", fname, err)
	}

	return s, nil
}

// Close closes the metric database.
func (s *T) Close() error {
	return s.db.Close()
}

// Record adds an observation for the specified metric.
func (s *T) Record(ctx context.Context, m *metrics.MetricSnapshot, time time.Time) error {
	// Ensure the metric definition is inserted into the defs table.
	if err := s.insertDef(ctx, m); err != nil {
		return err
	}

	// Insert the observation into the records table.
	var encCounts []byte
	if len(m.Counts) > 0 {
		var err error
		if encCounts, err = json.Marshal(m.Counts); err != nil {
			return err
		}
	}
	const stmt = `INSERT OR REPLACE INTO records(id, time, value, counts) values(?,?,?,?)`
	if _, err := s.execDB(ctx, stmt, int64(m.Id), time.Unix(), m.Value, string(encCounts)); err != nil {
		return fmt.Errorf("record metric update %s: %w", m.Name, err)
	}
	return nil
}

func (s *T) insertDef(ctx context.Context, m *metrics.MetricSnapshot) (err error) {
	cached := func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		_, ok := s.cachedDefs[m.Id]
		return ok
	}
	if cached() {
		return nil
	}

	// Insert into the defs table.
	encBounds, err := json.Marshal(m.Bounds)
	if err != nil {
		return err
	}
	const defsStmt = `INSERT OR IGNORE INTO defs(id, type, name, bounds, help) values(?,?,?,?,?)`
	if _, err = s.execDB(ctx, defsStmt, int64(m.Id), m.Type, m.Name, string(encBounds), m.Help); err != nil {
		return err
	}

	// Insert into the labels table.
	const labelsStmt = `INSERT OR IGNORE INTO labels(id, label, value) values(?,?,?)`
	for label, value := range m.Labels {
		if _, err = s.execDB(ctx, labelsStmt, int64(m.Id), label, value); err != nil {
			return err
		}
	}

	// Add to the cache.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cachedDefs[m.Id] = struct{}{}
	return nil
}

var queryTmpl = template.Must(template.New("query").Parse(`
--All metric ids whose name and labels match the query.
WITH matching_labels AS (
	SELECT defs.id
	FROM defs
	INNER JOIN labels ON defs.id = labels.id
	WHERE
	(defs.name=? OR ?="") {{if .}} AND (
		{{range .}}
		(labels.label=? AND value=?) OR {{end}}
		False
	){{end}}
	GROUP BY
		defs.id
	HAVING
		COUNT(labels.label) >= {{len .}}
),

--Latest recorded time for each metric id in the range [startTime, endTime].
max_time AS (
	SELECT records.id, MAX(records.time) AS time
    FROM records
    WHERE records.time >= ? AND records.time <= ?
    GROUP BY records.id
)

SELECT defs.*, records.value, records.counts,
GROUP_CONCAT(labels.label,','), GROUP_CONCAT(labels.value,',')
FROM defs, records, labels, matching_labels, max_time
WHERE
	defs.id = records.id AND
	defs.id = labels.id AND
	defs.id = matching_labels.id AND
	defs.id = max_time.id AND
	records.time = max_time.time
GROUP BY defs.id
`))

// Query calls fn with metrics whose name and labels match the provided
// arguments. If either argument is empty, no matching on that argument is
// performed. Leaving both arguments empty will call fn on all recorded metrics.
//
// Only metric values recorded in the [startTime, endTime] time period are
// considered; if there are multiple metric values recorded in the given time
// period, only the latest value for a given metric is considered. If startTime
// is zero, it is treated as negative infinity. If endTime is zero, it is
// treated as the current time.
//
// Callers should assume that metric values passed to fn are ephemeral and
// only valid during the call.
func (s *T) Query(ctx context.Context, startTime, endTime time.Time, name string, labels map[string]string, fn func(*metrics.MetricSnapshot) error) error {
	if endTime.IsZero() {
		endTime = time.Now()
	}
	var query strings.Builder
	if err := queryTmpl.Execute(&query, labels); err != nil {
		return err
	}
	args := []any{name, name}
	for label, value := range labels {
		args = append(args, label, value)
	}
	args = append(args, startTime.Unix(), endTime.Unix())
	records, err := s.queryDB(ctx, query.String(), args...)
	if err != nil {
		return err
	}
	defer records.Close()
	x := metrics.MetricSnapshot{
		Labels: map[string]string{},
	}
	var encBounds, encCounts, concatLabels, concatLabelVals string
	for records.Next() {
		var id int64
		if err := records.Scan(&id, &x.Type, &x.Name, &encBounds, &x.Help, &x.Value, &encCounts, &concatLabels, &concatLabelVals); err != nil {
			return err
		}
		x.Id = uint64(id)
		if encBounds != "" {
			if err = json.Unmarshal([]byte(encBounds), &x.Bounds); err != nil {
				return err
			}
		}
		if encCounts != "" {
			if err = json.Unmarshal([]byte(encCounts), &x.Counts); err != nil {
				return err
			}
		}
		maps.Clear(x.Labels)
		labels := strings.Split(concatLabels, ",")
		labelVals := strings.Split(concatLabelVals, ",")
		if len(labels) != len(labelVals) {
			return fmt.Errorf("different number of labels and label values")
		}
		for i := 0; i < len(labels); i++ {
			x.Labels[labels[i]] = labelVals[i]
		}
		if err := fn(&x); err != nil {
			return err
		}
	}
	return records.Err()
}

func (s *T) queryDB(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Keep retrying as long as we are getting the "locked" error.
	for r := retry.Begin(); r.Continue(ctx); {
		rows, err := s.db.QueryContext(ctx, query, args...)
		if isLocked(err) {
			continue
		}
		return rows, err
	}
	return nil, ctx.Err()
}

func (s *T) execDB(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// Keep retrying as long as we are getting the "locked" error.
	for r := retry.Begin(); r.Continue(ctx); {
		res, err := s.db.ExecContext(ctx, query, args...)
		if isLocked(err) {
			continue
		}
		return res, err
	}
	return nil, ctx.Err()
}

// isLocked returns whether the error is a "database is locked" error. See Note above.
func isLocked(err error) bool {
	var sqlError sqlite3.Error
	ok := errors.As(err, &sqlError)
	return ok && (sqlError.Code == sqlite3.ErrBusy || sqlError.Code == sqlite3.ErrLocked)
}
