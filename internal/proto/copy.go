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

// Package proto implements utilities for managing GCP resource protos
// (e.g., copy, equal, merge).
//
// Most of the implementations in this package are based on the implementations
// in the "official" proto package, i.e., "google.golang.org/protobuf/proto",
// with small modifications to serve Service Weaver GCP updating needs.
package proto

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Copy copies all populated message fields from src into dst, overwriting any
// existing values in dst.  Fields in skipFields aren't copied, even if they are
// populated.
// Returns the list of all fields in dst that were modified during the copy,
// or nil otherwise.
func Copy(dst, src proto.Message, skipFields []string) ([]string, error) {
	if src == nil {
		return nil, nil
	}
	if dst == nil {
		return nil, errors.New("nil dst")
	}
	d := dst.ProtoReflect()
	s := src.ProtoReflect()
	if !s.IsValid() {
		return nil, errors.New("invalid src message for copy")
	}
	if !d.IsValid() {
		return nil, errors.New("invalid dst message for copy")
	}
	if s.Descriptor() != d.Descriptor() {
		return nil, fmt.Errorf("incompatible descriptors %q and %q for copy",
			s.Descriptor().FullName(), d.Descriptor().FullName())
	}
	skip := toSet(skipFields)
	var modified []string
	s.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if _, ok := skip[fd.TextName()]; ok {
			return true
		}
		if equalField(d.Get(fd), v, fd) {
			return true
		}
		d.Set(fd, v)
		modified = append(modified, fd.TextName())
		return true
	})
	return modified, nil
}

func toSet(vals []string) map[string]bool {
	set := make(map[string]bool, len(vals))
	for _, val := range vals {
		set[val] = true
	}
	return set
}
