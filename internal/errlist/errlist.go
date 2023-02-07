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

// Package errlist contains an ErrList type that combines multiple errors into
// a single error. The package draws inspiration from hashicorp's go-multierror
// package (https://github.com/hashicorp/go-multierror).
package errlist

import (
	"errors"
	"fmt"
	"strings"
)

// At first, it might seem strange that ErrList doesn't implement the error
// interface, but this is done to avoid a common gotcha. Consider the following
// program [1]:
//
//     package main
//
//     import "fmt"
//
//     type ErrList []error
//     func (ErrList) Error() string { return "" }
//
//     func a() ErrList { return nil }
//     func b() error   { return a() }
//
//     func main() {
//         fmt.Println("a() == nil?", a() == nil) // true
//         fmt.Println("b() == nil?", b() == nil) // false
//     }
//
// Even though b returns a nil ErrList, b() == nil is false because the nil
// value returned by b has a non-nil dynamic type (i.e. ErrList). This makes it
// very likely for a user to shoot themselves in the foot:
//
//     func foo() error {
//         var errs ErrList
//         // Do some stuff...
//         return errs
//     }
//
//     func main() {
//         if err := foo(); err != nil {
//             // This branch is ALWAYS executed!
//         }
//     }
//
// To avoid this, ErrList does NOT implement the error interface. Instead, you
// must call the ErrorOrNil method to construct an error. Doing so avoids the
// gotcha.
//
// [1] https://go.dev/play/p/hDM2eL2RnTs

// ErrList holds a list of errors.
type ErrList []error

// ErrorOrNil returns a single error that includes all of the errors in the
// provided ErrList, or nil if there are no errors.
func (e ErrList) ErrorOrNil() error {
	if len(e) == 0 {
		return nil
	}
	return errlist(e)
}

type errlist []error

// Error implements the error interface.
// TODO: Use errors.Join() once we require Go 1.20.
func (e errlist) Error() string {
	var b strings.Builder
	for _, err := range e {
		fmt.Fprintln(&b, err.Error())
	}
	return b.String()
}

// FromStrings converts a list of error strings (e.g., when received
// in a serialized message) into an ErrList.
func FromStrings(str []string) error {
	if len(str) == 0 {
		return nil
	}
	if len(str) == 1 {
		return errors.New(str[0])
	}
	err := make(errlist, len(str))
	for i, s := range str {
		err[i] = errors.New(s)
	}
	return err
}
