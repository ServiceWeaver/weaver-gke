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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.21.12
// source: internal/gke/container.proto

package gke

import (
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// ContainerMetadata contains metadata about a container running on GKE.
//
// Typically, when Service Weaver launches a container, it places the container's
// ContainerMetadata in an environment variable (see container.go). The running
// container can then parse the ContainterMetadata from the environment and use
// the metadata for various things.
//
// For example, the stdout and stderr of a container run on GKE are captured
// and logged to Google Cloud Logging [1]. Every one of these log entries
// contains a "labels" and "resource" field that looks something like this:
//
//	labels: {
//	  compute.googleapis.com/resource_name:
//	  "gke-serviceweaver-default-pool-05ad7bcf-55vd" k8s-pod/app: "todo-main-ac9156"
//	  k8s-pod/pod-template-hash: "56ffc498d"
//	}
//	resource: {
//	  labels: {
//	    cluster_name: "serviceweaver"
//	    container_name: "serviceweaver"
//	    region: "us-east1"
//	    namespace_name: "serviceweaver"
//	    pod_name: "todo-main-ac9156-56ffc498d-l57rm"
//	    project_id: "serviceweaver-gke-dev"
//	  }
//	  type: "k8s_container"
//	}
//
// When you click on the "LOGS" tab on the page for a pod, deployment, or
// stateful set, the Google Cloud Console forms a query over these fields
// and shows you the resulting log entries. We're logging entries directly
// to Google Cloud Logging---not printing them to stdout or stderr---so in
// order for the logs to appear in the "LOGS" tab, we have to embed the
// same "labels" and "resource" fields. These fields are populated using a
// ContainerMetadata.
//
// [1]: https://kubernetes.io/docs/concepts/cluster-administration/logging/
type ContainerMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Project       string            `protobuf:"bytes,1,opt,name=project,proto3" json:"project,omitempty"`                                  // GCP project
	ClusterName   string            `protobuf:"bytes,2,opt,name=cluster_name,json=clusterName,proto3" json:"cluster_name,omitempty"`       // GKE cluster name
	ClusterRegion string            `protobuf:"bytes,3,opt,name=cluster_region,json=clusterRegion,proto3" json:"cluster_region,omitempty"` // GKE cluster region (e.g., us-east1)
	Namespace     string            `protobuf:"bytes,4,opt,name=namespace,proto3" json:"namespace,omitempty"`                              // Kubernetes namespace (e.g., default)
	NodeName      string            `protobuf:"bytes,5,opt,name=node_name,json=nodeName,proto3" json:"node_name,omitempty"`                // GCP node name
	PodName       string            `protobuf:"bytes,6,opt,name=pod_name,json=podName,proto3" json:"pod_name,omitempty"`                   // Kubernetes pod name
	ContainerName string            `protobuf:"bytes,7,opt,name=container_name,json=containerName,proto3" json:"container_name,omitempty"` // Kubernetes container name
	App           string            `protobuf:"bytes,8,opt,name=app,proto3" json:"app,omitempty"`                                          // Kubernetes app label
	Telemetry     *config.Telemetry `protobuf:"bytes,9,opt,name=telemetry,proto3" json:"telemetry,omitempty"`                              // Options to configure the telemetry
	MtlsEnabled   bool              `protobuf:"varint,10,opt,name=mtls_enabled,json=mtlsEnabled,proto3" json:"mtls_enabled,omitempty"`     // Whether MTLS should be enabled
}

func (x *ContainerMetadata) Reset() {
	*x = ContainerMetadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_gke_container_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ContainerMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ContainerMetadata) ProtoMessage() {}

func (x *ContainerMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_internal_gke_container_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ContainerMetadata.ProtoReflect.Descriptor instead.
func (*ContainerMetadata) Descriptor() ([]byte, []int) {
	return file_internal_gke_container_proto_rawDescGZIP(), []int{0}
}

func (x *ContainerMetadata) GetProject() string {
	if x != nil {
		return x.Project
	}
	return ""
}

func (x *ContainerMetadata) GetClusterName() string {
	if x != nil {
		return x.ClusterName
	}
	return ""
}

func (x *ContainerMetadata) GetClusterRegion() string {
	if x != nil {
		return x.ClusterRegion
	}
	return ""
}

func (x *ContainerMetadata) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *ContainerMetadata) GetNodeName() string {
	if x != nil {
		return x.NodeName
	}
	return ""
}

func (x *ContainerMetadata) GetPodName() string {
	if x != nil {
		return x.PodName
	}
	return ""
}

func (x *ContainerMetadata) GetContainerName() string {
	if x != nil {
		return x.ContainerName
	}
	return ""
}

func (x *ContainerMetadata) GetApp() string {
	if x != nil {
		return x.App
	}
	return ""
}

func (x *ContainerMetadata) GetTelemetry() *config.Telemetry {
	if x != nil {
		return x.Telemetry
	}
	return nil
}

func (x *ContainerMetadata) GetMtlsEnabled() bool {
	if x != nil {
		return x.MtlsEnabled
	}
	return false
}

var File_internal_gke_container_proto protoreflect.FileDescriptor

var file_internal_gke_container_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x67, 0x6b, 0x65, 0x2f, 0x63,
	0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x03,
	0x67, 0x6b, 0x65, 0x1a, 0x1c, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x63, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0xda, 0x02, 0x0a, 0x11, 0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x4d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x18, 0x0a, 0x07, 0x70, 0x72, 0x6f, 0x6a, 0x65,
	0x63, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63,
	0x74, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72,
	0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x5f,
	0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x63, 0x6c,
	0x75, 0x73, 0x74, 0x65, 0x72, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x12, 0x1c, 0x0a, 0x09, 0x6e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x6e, 0x6f, 0x64,
	0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x6e, 0x6f,
	0x64, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x70, 0x6f, 0x64, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x70, 0x6f, 0x64, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x25, 0x0a, 0x0e, 0x63, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x65, 0x72, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x61,
	0x69, 0x6e, 0x65, 0x72, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x61, 0x70, 0x70, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x61, 0x70, 0x70, 0x12, 0x2f, 0x0a, 0x09, 0x74, 0x65,
	0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e,
	0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x54, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79,
	0x52, 0x09, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x12, 0x21, 0x0a, 0x0c, 0x6d,
	0x74, 0x6c, 0x73, 0x5f, 0x65, 0x6e, 0x61, 0x62, 0x6c, 0x65, 0x64, 0x18, 0x0a, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x0b, 0x6d, 0x74, 0x6c, 0x73, 0x45, 0x6e, 0x61, 0x62, 0x6c, 0x65, 0x64, 0x42, 0x29,
	0x5a, 0x27, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x53, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x57, 0x65, 0x61, 0x76, 0x65, 0x72, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x65,
	0x72, 0x2d, 0x67, 0x6b, 0x65, 0x3b, 0x67, 0x6b, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_internal_gke_container_proto_rawDescOnce sync.Once
	file_internal_gke_container_proto_rawDescData = file_internal_gke_container_proto_rawDesc
)

func file_internal_gke_container_proto_rawDescGZIP() []byte {
	file_internal_gke_container_proto_rawDescOnce.Do(func() {
		file_internal_gke_container_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_gke_container_proto_rawDescData)
	})
	return file_internal_gke_container_proto_rawDescData
}

var file_internal_gke_container_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_internal_gke_container_proto_goTypes = []interface{}{
	(*ContainerMetadata)(nil), // 0: gke.ContainerMetadata
	(*config.Telemetry)(nil),  // 1: config.Telemetry
}
var file_internal_gke_container_proto_depIdxs = []int32{
	1, // 0: gke.ContainerMetadata.telemetry:type_name -> config.Telemetry
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_internal_gke_container_proto_init() }
func file_internal_gke_container_proto_init() {
	if File_internal_gke_container_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_gke_container_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ContainerMetadata); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_gke_container_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_gke_container_proto_goTypes,
		DependencyIndexes: file_internal_gke_container_proto_depIdxs,
		MessageInfos:      file_internal_gke_container_proto_msgTypes,
	}.Build()
	File_internal_gke_container_proto = out.File
	file_internal_gke_container_proto_rawDesc = nil
	file_internal_gke_container_proto_goTypes = nil
	file_internal_gke_container_proto_depIdxs = nil
}
