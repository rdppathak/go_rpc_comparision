// automatically generated by the FlatBuffers compiler, do not modify

package fileoperations

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type ReadAtRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsReadAtRequest(buf []byte, offset flatbuffers.UOffsetT) *ReadAtRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &ReadAtRequest{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *ReadAtRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *ReadAtRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *ReadAtRequest) Offset() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ReadAtRequest) MutateOffset(n int64) bool {
	return rcv._tab.MutateInt64Slot(4, n)
}

func (rcv *ReadAtRequest) Path() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ReadAtRequest) Size() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ReadAtRequest) MutateSize(n int64) bool {
	return rcv._tab.MutateInt64Slot(8, n)
}

func (rcv *ReadAtRequest) BlockSize() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *ReadAtRequest) MutateBlockSize(n int64) bool {
	return rcv._tab.MutateInt64Slot(10, n)
}

func ReadAtRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func ReadAtRequestAddOffset(builder *flatbuffers.Builder, Offset int64) {
	builder.PrependInt64Slot(0, Offset, 0)
}
func ReadAtRequestAddPath(builder *flatbuffers.Builder, Path flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(Path), 0)
}
func ReadAtRequestAddSize(builder *flatbuffers.Builder, Size int64) {
	builder.PrependInt64Slot(2, Size, 0)
}
func ReadAtRequestAddBlockSize(builder *flatbuffers.Builder, BlockSize int64) {
	builder.PrependInt64Slot(3, BlockSize, 0)
}
func ReadAtRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
