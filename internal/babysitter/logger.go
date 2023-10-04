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

package babysitter

import (
	"context"

	"github.com/ServiceWeaver/weaver"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

const (
	weaverLoggerName = "github.com/ServiceWeaver/weaver/Logger"
	funcLoggerName   = "github.com/ServiceWeaver/weaver-gke/internal/babysitter/funcLogger"
)

// funcLogger overrides weaver.Logger component when running under GKE.
// It redirects log entries to a supplied function.
type funcLogger weaver.Logger

type loggerImpl struct {
	weaver.Implements[funcLogger]
	dst func(*protos.LogEntry)
}

var _ funcLogger = &loggerImpl{}

func (l *loggerImpl) LogBatch(ctx context.Context, batch *protos.LogEntryBatch) error {
	for _, e := range batch.Entries {
		l.dst(e)
	}
	return nil
}
