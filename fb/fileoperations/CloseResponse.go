// automatically generated by the FlatBuffers compiler, do not modify

package fileoperations

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type CloseResponse struct {
	_tab flatbuffers.Table
}

func GetRootAsCloseResponse(buf []byte, offset flatbuffers.UOffsetT) *CloseResponse {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &CloseResponse{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *CloseResponse) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *CloseResponse) Table() flatbuffers.Table {
	return rcv._tab
}

func CloseResponseStart(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func CloseResponseEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
