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
	"errors"
)

// Sequence orders a value into a sequence of unique values and returns its
// index in the sequence. The first unique value is sequenced with index 0, the
// next with index 1, and so on.
func Sequence(ctx context.Context, store Store, key, value string) (int, error) {
	var seq SequenceProto
	_, err := UpdateProto(ctx, store, key, &seq, func(*Version) error {
		if seq.Sequence == nil {
			seq.Sequence = map[string]int64{}
		}
		if _, ok := seq.Sequence[value]; ok {
			return ErrUnchanged
		}
		seq.Sequence[value] = int64(len(seq.Sequence))
		return nil
	})
	if err != nil && !errors.Is(err, ErrUnchanged) {
		return 0, err
	}
	return int(seq.Sequence[value]), nil
}
