// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v3.20.3
// source: rm/virtual_cluster_size.proto

package rm

import (
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

type VirtualClusterSizeSpec struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id      int64   `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Name    string  `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Alias_1 *string `protobuf:"bytes,3,opt,name=alias_1,json=alias1,proto3,oneof" json:"alias_1,omitempty"`
	Alias_2 *string `protobuf:"bytes,4,opt,name=alias_2,json=alias2,proto3,oneof" json:"alias_2,omitempty"`
	Alias_3 *string `protobuf:"bytes,5,opt,name=alias_3,json=alias3,proto3,oneof" json:"alias_3,omitempty"`
	CpuCore float64 `protobuf:"fixed64,6,opt,name=cpu_core,json=cpuCore,proto3" json:"cpu_core,omitempty"`
	MemGb   int64   `protobuf:"varint,7,opt,name=mem_gb,json=memGb,proto3" json:"mem_gb,omitempty"`
}

func (x *VirtualClusterSizeSpec) Reset() {
	*x = VirtualClusterSizeSpec{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rm_virtual_cluster_size_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VirtualClusterSizeSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VirtualClusterSizeSpec) ProtoMessage() {}

func (x *VirtualClusterSizeSpec) ProtoReflect() protoreflect.Message {
	mi := &file_rm_virtual_cluster_size_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VirtualClusterSizeSpec.ProtoReflect.Descriptor instead.
func (*VirtualClusterSizeSpec) Descriptor() ([]byte, []int) {
	return file_rm_virtual_cluster_size_proto_rawDescGZIP(), []int{0}
}

func (x *VirtualClusterSizeSpec) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *VirtualClusterSizeSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *VirtualClusterSizeSpec) GetAlias_1() string {
	if x != nil && x.Alias_1 != nil {
		return *x.Alias_1
	}
	return ""
}

func (x *VirtualClusterSizeSpec) GetAlias_2() string {
	if x != nil && x.Alias_2 != nil {
		return *x.Alias_2
	}
	return ""
}

func (x *VirtualClusterSizeSpec) GetAlias_3() string {
	if x != nil && x.Alias_3 != nil {
		return *x.Alias_3
	}
	return ""
}

func (x *VirtualClusterSizeSpec) GetCpuCore() float64 {
	if x != nil {
		return x.CpuCore
	}
	return 0
}

func (x *VirtualClusterSizeSpec) GetMemGb() int64 {
	if x != nil {
		return x.MemGb
	}
	return 0
}

var File_rm_virtual_cluster_size_proto protoreflect.FileDescriptor

var file_rm_virtual_cluster_size_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x72, 0x6d, 0x2f, 0x76, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x5f, 0x63, 0x6c, 0x75,
	0x73, 0x74, 0x65, 0x72, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x02, 0x72, 0x6d, 0x22, 0xec, 0x01, 0x0a, 0x16, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x43,
	0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x53, 0x69, 0x7a, 0x65, 0x53, 0x70, 0x65, 0x63, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x1c, 0x0a, 0x07, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x5f, 0x31, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x06, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x31, 0x88, 0x01, 0x01,
	0x12, 0x1c, 0x0a, 0x07, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x5f, 0x32, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x09, 0x48, 0x01, 0x52, 0x06, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x32, 0x88, 0x01, 0x01, 0x12, 0x1c,
	0x0a, 0x07, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x5f, 0x33, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x48,
	0x02, 0x52, 0x06, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x33, 0x88, 0x01, 0x01, 0x12, 0x19, 0x0a, 0x08,
	0x63, 0x70, 0x75, 0x5f, 0x63, 0x6f, 0x72, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x01, 0x52, 0x07,
	0x63, 0x70, 0x75, 0x43, 0x6f, 0x72, 0x65, 0x12, 0x15, 0x0a, 0x06, 0x6d, 0x65, 0x6d, 0x5f, 0x67,
	0x62, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x6d, 0x65, 0x6d, 0x47, 0x62, 0x42, 0x0a,
	0x0a, 0x08, 0x5f, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x5f, 0x31, 0x42, 0x0a, 0x0a, 0x08, 0x5f, 0x61,
	0x6c, 0x69, 0x61, 0x73, 0x5f, 0x32, 0x42, 0x0a, 0x0a, 0x08, 0x5f, 0x61, 0x6c, 0x69, 0x61, 0x73,
	0x5f, 0x33, 0x42, 0x37, 0x5a, 0x35, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x63, 0x6c, 0x69, 0x63, 0x6b, 0x7a, 0x65, 0x74, 0x74, 0x61, 0x2f, 0x67, 0x6f, 0x63, 0x6c,
	0x69, 0x63, 0x6b, 0x7a, 0x65, 0x74, 0x74, 0x61, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f,
	0x62, 0x75, 0x6c, 0x6b, 0x6c, 0x6f, 0x61, 0x64, 0x2f, 0x72, 0x6d, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_rm_virtual_cluster_size_proto_rawDescOnce sync.Once
	file_rm_virtual_cluster_size_proto_rawDescData = file_rm_virtual_cluster_size_proto_rawDesc
)

func file_rm_virtual_cluster_size_proto_rawDescGZIP() []byte {
	file_rm_virtual_cluster_size_proto_rawDescOnce.Do(func() {
		file_rm_virtual_cluster_size_proto_rawDescData = protoimpl.X.CompressGZIP(file_rm_virtual_cluster_size_proto_rawDescData)
	})
	return file_rm_virtual_cluster_size_proto_rawDescData
}

var file_rm_virtual_cluster_size_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_rm_virtual_cluster_size_proto_goTypes = []interface{}{
	(*VirtualClusterSizeSpec)(nil), // 0: rm.VirtualClusterSizeSpec
}
var file_rm_virtual_cluster_size_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_rm_virtual_cluster_size_proto_init() }
func file_rm_virtual_cluster_size_proto_init() {
	if File_rm_virtual_cluster_size_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_rm_virtual_cluster_size_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VirtualClusterSizeSpec); i {
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
	file_rm_virtual_cluster_size_proto_msgTypes[0].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_rm_virtual_cluster_size_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_rm_virtual_cluster_size_proto_goTypes,
		DependencyIndexes: file_rm_virtual_cluster_size_proto_depIdxs,
		MessageInfos:      file_rm_virtual_cluster_size_proto_msgTypes,
	}.Build()
	File_rm_virtual_cluster_size_proto = out.File
	file_rm_virtual_cluster_size_proto_rawDesc = nil
	file_rm_virtual_cluster_size_proto_goTypes = nil
	file_rm_virtual_cluster_size_proto_depIdxs = nil
}
