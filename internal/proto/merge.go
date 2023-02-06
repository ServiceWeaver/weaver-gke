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

package proto

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	pref "google.golang.org/protobuf/reflect/protoreflect"
)

// Merge merges src into dst, using the following merge strategy:
//   - Populated scalar fields in src are copied to dst.
//   - Populated singular message fields in src are recursively merged into dst.
//   - List fields in src are merged into dst as follows:
//   - If an element in src "matches" an element in dst, the src element
//     is recursively merged into the dst element. (If multiple elements in
//     dst are matched, the src element is merged only into the first
//     matched one.)
//   - Otherwise, the src element is appended to the end of dst.
//   - Whether two elements "match" depends on their type:
//     # For message types, the elements match iff their messages'
//     comparison fields (specified in cmpFields) are equal.
//     # For all other types, the elements match if they are equal.
//   - Map fields in src are merged into dst as follows:
//   - If an entry with a given key exists in both src and dst, the src
//     entry is recursively merged into the dst entry.
//   - If an entry with a given key exists only in src, the src entry
//     is inserted into dst.
//
// Value true is returned iff dst was modified during the merge.
func Merge(dst, src proto.Message, cmpFields map[string]string) (bool, error) {
	m := &merger{
		cmpFields: cmpFields,
	}
	err := m.mergeMessage(dst.ProtoReflect(), src.ProtoReflect())
	return m.modified, err
}

type merger struct {
	cmpFields map[string]string
	modified  bool
}

func (m *merger) mergeMessage(dst, src pref.Message) error {
	if dst.Descriptor() != src.Descriptor() {
		return fmt.Errorf("descriptor mismatch; want %v, got %v",
			dst.Descriptor().FullName(), src.Descriptor().FullName())
	}
	var err error
	src.Range(func(fd pref.FieldDescriptor, v pref.Value) bool {
		switch {
		case fd.IsList():
			err = m.mergeList(dst.Mutable(fd).List(), v.List(), fd)
		case fd.IsMap():
			err = m.mergeMap(dst.Mutable(fd).Map(), v.Map(), fd.MapValue())
		case fd.Message() != nil:
			err = m.mergeMessage(dst.Mutable(fd).Message(), v.Message())
		// TODO(spetrovic): The proto merge library has special handling
		// for BytesKind fields, copying the byte slice over instead
		// of sharing the same pointer.  We should figure out if we
		// need to do the same.
		default:
			if !equalValue(dst.Get(fd), v, fd) {
				dst.Set(fd, v)
				m.modified = true
			}
		}
		return err == nil
	})
	return err
}

func (m *merger) mergeList(dst, src pref.List, fd pref.FieldDescriptor) error {
	// If fd is a message, find its comparison field.
	var cmpFd pref.FieldDescriptor
	if fd.Message() != nil {
		md := fd.Message()
		fname, ok := m.cmpFields[string(md.Name())]
		if !ok {
			return fmt.Errorf(
				"no comparison field specified for list message %q", md.Name())
		}
		if cmpFd = md.Fields().ByName(pref.Name(fname)); cmpFd == nil {
			return fmt.Errorf(
				"invalid comparison field %q for list message %q", fname, md.Name())
		}
	}

	for i, n := 0, src.Len(); i < n; i++ {
		srcv := src.Get(i)
		// See if srcv already exists in dst.
		found := false
		for j, k := 0, dst.Len(); j < k; j++ {
			dstv := dst.Get(j)
			switch {
			case fd.Message() != nil:
				if equalField(srcv.Message().Get(cmpFd), dstv.Message().Get(cmpFd), cmpFd) {
					// Merge srcv into dstv.
					if err := m.mergeMessage(dstv.Message(), srcv.Message()); err != nil {
						return err
					}
					found = true
					break
				}
			default:
				if equalValue(srcv, dstv, fd) {
					found = true
					break
				}
			}
		}
		if !found {
			// Append srcv to the end of dst.
			switch {
			case fd.Message() != nil:
				dstv := dst.NewElement()
				if err := m.mergeMessage(dstv.Message(), srcv.Message()); err != nil {
					return err
				}
				dst.Append(dstv)
			default:
				dst.Append(srcv)
			}
			m.modified = true
		}
	}
	return nil
}

func (m *merger) mergeMap(dst, src pref.Map, fd pref.FieldDescriptor) error {
	var err error
	src.Range(func(key pref.MapKey, srcv pref.Value) bool {
		switch {
		case fd.Message() != nil:
			dstv := dst.Mutable(key)
			if err = m.mergeMessage(dstv.Message(), srcv.Message()); err == nil {
				dst.Set(key, dstv)
			}
		default:
			if !dst.Has(key) || !equalValue(dst.Get(key), srcv, fd) {
				dst.Set(key, srcv)
				m.modified = true
			}
		}
		return err == nil
	})
	return err
}
