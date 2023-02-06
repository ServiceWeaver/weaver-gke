#!/bin/sh
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Wrapper around protoc that sets up the correct options.

# Find the go bin directory and add it to the PATH.
gobin=$(go env GOBIN)
if test -z $gobin; then
  gopath=$(go env GOPATH)
  if test -z $gopath; then
    gopath="$HOME/go"
  fi
  gobin="$gopath/bin"
fi
export PATH="$PATH:$gobin"

# Check that needed binaries are available.
protoc=$(which protoc)
if test -z $protoc; then
  printf "protoc binary not found.  Please run:\n\tsudo apt install protobuf-compiler\n, and then re-run this command.\n"
  exit 1
fi
gengo=$(which protoc-gen-go)
if test -z $gengo; then
  printf "protoc-gen-go binary not found.  Please run:\n\tgo install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26\nand then re-run this command."
  exit 1
fi

# Get the version for the github.com/ServiceWeaver/weaver module dependency.
go mod download github.com/ServiceWeaver/weaver
dep=$(go mod graph | grep "github.com/ServiceWeaver/weaver-gke github.com/ServiceWeaver/weaver@")
if test -z "$dep"; then
  printf "Go module github.com/ServiceWeaver/weaver not found.  Please run:\n\tgo mod tidy\n and then re-run this command."
  exit 1
fi
split=(${dep//@/ })
weaver=${split[2]}
if test -z $weaver; then
  printf "Internal error: cannot determine version for github.com/ServiceWeaver/weaver module."
  exit 1
fi

# Create a local copy of all proto dependencies.
tmpdir=$(mktemp -d)
mkdir -p $tmpdir/weaver/runtime/protos/
cp $(go env GOPATH)/pkg/mod/github.com/\!service\!weaver/weaver@$weaver/runtime/protos/*.proto $tmpdir/weaver/runtime/protos/

exec protoc -I . -I $tmpdir --go_out=. --go_opt=paths=source_relative,Mgoogle/protobuf/timestamp.proto=google.golang.org/protobuf/types/known/timestamppb ${1+"$@"}
