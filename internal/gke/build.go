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

package gke

import (
	"bufio"
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/template"

	"github.com/google/uuid"
)

var dockerfileTmpl = template.Must(template.New("Dockerfile").Parse(`
{{if .GoInstall}}
FROM golang:1.20-bullseye as builder
RUN echo ""{{range .GoInstall}} && go install {{.}}{{end}}
{{end}}
FROM ubuntu:rolling
WORKDIR /weaver/
RUN apt-get update
RUN apt-get install -y ca-certificates
COPY . .
{{if .GoInstall}}
COPY --from=builder /go/bin/ /weaver/
{{end}}
ENTRYPOINT ["/bin/bash", "-c"]
`))

// buildSpec holds information about a container image build.
type buildSpec struct {
	Tags      []string    // Tags attached to the built image.
	Files     []string    // Files that should be copied to the container.
	GoInstall []string    // Binary targets that should be 'go install'-ed
	Config    CloudConfig // Cloud project configuration.
}

// buildImage builds a container image with the given specification,
// returning the URL of the built image.
// All the files in spec.Files will be copied to the container's /weaver/
// directory.
// The container entrypoint will be "/bin/bash -c".
func buildImage(ctx context.Context, spec buildSpec) error {
	// Create a new working directory.
	workDir := filepath.Join(os.TempDir(), fmt.Sprintf("weaver%s", uuid.New().String()))
	if err := os.Mkdir(workDir, 0o700); err != nil {
		return err
	}
	defer os.RemoveAll(workDir)

	// Create:
	//  workDir/
	//    file1
	//    file2
	//    ...
	//    fileN
	//    Dockerfile        - docker build instructions
	//    cloudbuild.yaml   - cloud build instructions

	// Copy files to the workDir.
	for _, file := range spec.Files {
		workDirFile := filepath.Join(workDir, filepath.Base(filepath.Clean(file)))
		if err := cp(file, workDirFile); err != nil {
			return err
		}
	}

	// Create workDir/Dockerfile.
	dockerFile, err := os.Create(filepath.Join(workDir, "Dockerfile"))
	if err != nil {
		return err
	}
	if err := dockerfileTmpl.Execute(dockerFile, spec); err != nil {
		dockerFile.Close()
		return err
	}
	if err := dockerFile.Close(); err != nil {
		return err
	}

	// Create workDir/cloudbuild.yaml
	cloudbuildFile, err := os.Create(filepath.Join(workDir, "cloudbuild.yaml"))
	if err != nil {
		return err
	}
	const tmpl = `
steps:
- name: gcr.io/cloud-builders/docker
  args:
  - build {{range $tag := .Tags}}
  - '-t'
  - '{{$tag}}' {{end}}
  - '.'
images: {{range $tag := .Tags}}
  - '{{$tag}}' {{end}}
`
	if err := template.Must(template.New("cloudbuild").Parse(tmpl)).Execute(cloudbuildFile, spec); err != nil {
		cloudbuildFile.Close()
		return err
	}
	if err := cloudbuildFile.Close(); err != nil {
		return err
	}

	// Start the build.
	_, err = runGcloud(spec.Config, "", cmdOptions{},
		"builds", "submit", "--config", cloudbuildFile.Name(), "--machine-type",
		"E2_HIGHCPU_8", workDir,
	)
	return err
}

// cp copies the src file to the dst files.
func cp(src, dst string) error {
	// Open src.
	srcf, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %q: %w", src, err)
	}
	defer srcf.Close()
	srcinfo, err := srcf.Stat()
	if err != nil {
		return fmt.Errorf("stat %q: %w", src, err)
	}

	// Create or truncate dst.
	dstf, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create %q: %w", dst, err)
	}
	defer dstf.Close()

	// Copy src to dst.
	const bufSize = 1 << 20
	if _, err := io.Copy(dstf, bufio.NewReaderSize(srcf, bufSize)); err != nil {
		return fmt.Errorf("cp %q %q: %w", src, dst, err)
	}
	if err := os.Chmod(dst, srcinfo.Mode()); err != nil {
		return fmt.Errorf("chmod %q: %w", dst, err)
	}
	return nil
}
