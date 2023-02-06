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

package managertest

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/ServiceWeaver/weaver"
)

//go:generate weaver generate

type source interface {
	Emit(ctx context.Context, file, msg string) error
}

type sourceImpl struct {
	weaver.Implements[source]
	dst destination
}

func (s *sourceImpl) Init(_ context.Context) error {
	dst, err := weaver.Get[destination](s)
	if err != nil {
		return err
	}
	s.dst = dst
	return nil
}

func (s *sourceImpl) Emit(ctx context.Context, file, msg string) error {
	return s.dst.Record(ctx, file, msg)
}

type destination interface {
	Getpid(_ context.Context) (int, error)
	Record(_ context.Context, file, msg string) error
	GetAll(_ context.Context, file string) ([]string, error)
}

type destinationImpl struct {
	weaver.Implements[destination]
	mu sync.Mutex
}

func (d *destinationImpl) Getpid(_ context.Context) (int, error) {
	return os.Getpid(), nil
}

// Record adds a message.
func (d *destinationImpl) Record(_ context.Context, file, msg string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(msg + "\n")
	return err
}

// GetAll returns all added messages.
func (d *destinationImpl) GetAll(_ context.Context, file string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	str := strings.TrimSpace(string(data))
	return strings.Split(str, "\n"), nil
}
