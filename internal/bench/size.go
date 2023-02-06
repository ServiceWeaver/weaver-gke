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

package bench

import "fmt"

type Size int

const (
	B  Size = 1
	KB Size = 1000
	MB Size = 1000 * KB
	GB Size = 1000 * MB
	TB Size = 1000 * GB
)

func (s Size) Bytes() int {
	return int(s)
}

func (s Size) String() string {
	switch {
	case s < KB:
		return fmt.Sprintf("%dB", s)
	case s < MB:
		return fmt.Sprintf("%.2fKB", float64(s)/float64(KB))
	case s < GB:
		return fmt.Sprintf("%.2fMB", float64(s)/float64(MB))
	case s < TB:
		return fmt.Sprintf("%.2fGB", float64(s)/float64(GB))
	default:
		return fmt.Sprintf("%.2fTB", float64(s)/float64(TB))
	}
}
