// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pb/node.protofiles

package taas_proto

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Node struct {
	Ip   string `protobuf:"bytes,1,opt,name=ip" json:"ip,omitempty"`
	Port uint32 `protobuf:"varint,2,opt,name=port" json:"port,omitempty"`
	Id   uint32 `protobuf:"varint,3,opt,name=id" json:"id,omitempty"`
}

func (m *Node) Reset()                    { *m = Node{} }
func (m *Node) String() string            { return proto.CompactTextString(m) }
func (*Node) ProtoMessage()               {}
func (*Node) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{0} }

func (m *Node) GetIp() string {
	if m != nil {
		return m.Ip
	}
	return ""
}

func (m *Node) GetPort() uint32 {
	if m != nil {
		return m.Port
	}
	return 0
}

func (m *Node) GetId() uint32 {
	if m != nil {
		return m.Id
	}
	return 0
}

func init() {
	proto.RegisterType((*Node)(nil), "pb.Node")
}

func init() { proto.RegisterFile("pb/node.protofiles", fileDescriptor2) }

var fileDescriptor2 = []byte{
	// 99 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2d, 0x48, 0xd2, 0xcf,
	0xcb, 0x4f, 0x49, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2a, 0x48, 0x52, 0xb2, 0xe2,
	0x62, 0xf1, 0xcb, 0x4f, 0x49, 0x15, 0xe2, 0xe3, 0x62, 0xca, 0x2c, 0x90, 0x60, 0x54, 0x60, 0xd4,
	0xe0, 0x0c, 0x62, 0xca, 0x2c, 0x10, 0x12, 0xe2, 0x62, 0x29, 0xc8, 0x2f, 0x2a, 0x91, 0x60, 0x52,
	0x60, 0xd4, 0xe0, 0x0d, 0x02, 0xb3, 0xc1, 0x6a, 0x52, 0x24, 0x98, 0xc1, 0x22, 0x4c, 0x99, 0x29,
	0x49, 0x6c, 0x60, 0x63, 0x8c, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0xa5, 0x05, 0x41, 0x46, 0x57,
	0x00, 0x00, 0x00,
}
