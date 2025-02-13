// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v3.20.3
// source: util/schema.proto

package util

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

type Schema struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Location string `protobuf:"bytes,1,opt,name=location,proto3" json:"location,omitempty"`
}

func (x *Schema) Reset() {
	*x = Schema{}
	if protoimpl.UnsafeEnabled {
		mi := &file_util_schema_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Schema) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Schema) ProtoMessage() {}

func (x *Schema) ProtoReflect() protoreflect.Message {
	mi := &file_util_schema_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Schema.ProtoReflect.Descriptor instead.
func (*Schema) Descriptor() ([]byte, []int) {
	return file_util_schema_proto_rawDescGZIP(), []int{0}
}

func (x *Schema) GetLocation() string {
	if x != nil {
		return x.Location
	}
	return ""
}

var File_util_schema_proto protoreflect.FileDescriptor

var file_util_schema_proto_rawDesc = []byte{
	0x0a, 0x11, 0x75, 0x74, 0x69, 0x6c, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x04, 0x75, 0x74, 0x69, 0x6c, 0x22, 0x24, 0x0a, 0x06, 0x53, 0x63, 0x68,
	0x65, 0x6d, 0x61, 0x12, 0x1a, 0x0a, 0x08, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x42,
	0x39, 0x5a, 0x37, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63, 0x6c,
	0x69, 0x63, 0x6b, 0x7a, 0x65, 0x74, 0x74, 0x61, 0x2f, 0x67, 0x6f, 0x63, 0x6c, 0x69, 0x63, 0x6b,
	0x7a, 0x65, 0x74, 0x74, 0x61, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x62, 0x75, 0x6c,
	0x6b, 0x6c, 0x6f, 0x61, 0x64, 0x2f, 0x75, 0x74, 0x69, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_util_schema_proto_rawDescOnce sync.Once
	file_util_schema_proto_rawDescData = file_util_schema_proto_rawDesc
)

func file_util_schema_proto_rawDescGZIP() []byte {
	file_util_schema_proto_rawDescOnce.Do(func() {
		file_util_schema_proto_rawDescData = protoimpl.X.CompressGZIP(file_util_schema_proto_rawDescData)
	})
	return file_util_schema_proto_rawDescData
}

var file_util_schema_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_util_schema_proto_goTypes = []interface{}{
	(*Schema)(nil), // 0: util.Schema
}
var file_util_schema_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_util_schema_proto_init() }
func file_util_schema_proto_init() {
	if File_util_schema_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_util_schema_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Schema); i {
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
			RawDescriptor: file_util_schema_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_util_schema_proto_goTypes,
		DependencyIndexes: file_util_schema_proto_depIdxs,
		MessageInfos:      file_util_schema_proto_msgTypes,
	}.Build()
	File_util_schema_proto = out.File
	file_util_schema_proto_rawDesc = nil
	file_util_schema_proto_goTypes = nil
	file_util_schema_proto_depIdxs = nil
}
