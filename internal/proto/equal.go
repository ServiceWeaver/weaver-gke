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
	"bytes"
	"math"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func equalField(x, y protoreflect.Value, fd protoreflect.FieldDescriptor) bool {
	switch {
	case fd.IsList():
		return equalList(x.List(), y.List(), fd)
	case fd.IsMap():
		return equalMap(x.Map(), y.Map(), fd)
	default:
		return equalValue(x, y, fd)
	}
}

func equalList(x, y protoreflect.List, fd protoreflect.FieldDescriptor) bool {
	if x.Len() != y.Len() {
		return false
	}
	for i := 0; i < x.Len(); i++ {
		if !equalValue(x.Get(i), y.Get(i), fd) {
			return false
		}
	}
	return true
}

func equalMap(x, y protoreflect.Map, fd protoreflect.FieldDescriptor) bool {
	if x.Len() != y.Len() {
		return false
	}
	equal := true
	x.Range(func(k protoreflect.MapKey, xv protoreflect.Value) bool {
		yv := y.Get(k)
		equal = y.Has(k) && equalValue(xv, yv, fd.MapValue())
		return equal
	})
	return equal
}

func equalValue(x, y protoreflect.Value, fd protoreflect.FieldDescriptor) bool {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return x.Bool() == y.Bool()
	case protoreflect.EnumKind:
		return x.Enum() == y.Enum()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return x.Int() == y.Int()
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return x.Uint() == y.Uint()
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		xv := x.Float()
		yv := y.Float()
		if math.IsNaN(xv) || math.IsNaN(yv) {
			return math.IsNaN(xv) && math.IsNaN(yv)
		}
		return xv == yv
	case protoreflect.StringKind:
		return x.String() == y.String()
	case protoreflect.BytesKind:
		return bytes.Equal(x.Bytes(), y.Bytes())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return proto.Equal(x.Message().Interface(), y.Message().Interface())
	default:
		return x.Interface() == y.Interface()
	}
}
