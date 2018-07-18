package main

import (
	"log"
	"net"
	"errors"
	"os"
	"flag"

	context "golang.org/x/net/context"

	flatbuffers "github.com/google/flatbuffers/go"
	"rpc/fb/fileoperations"

	"google.golang.org/grpc"
)

type server struct {
	id int64
	handleMap map[string]*os.File
}

func getFileHandle(path string) (*os.File, error) {
	log.Println("Fetching handle for ", path)
	handle, err := os.Open(path)
	return handle, err
}

func (s *server) readData(path string, offset int64, data []byte) (int64, error) {
	log.Println ("Fetching data for ", path)
	handle, ok := s.handleMap[path]
	if !ok {
		return 0, errors.New("Failed to fetch file handle")
	}

	ret, err := handle.ReadAt(data, offset)

	return int64(ret), err
}

func (s *server) Open(context context.Context, in *fileoperations.OpenRequest) (*flatbuffers.Builder, error) {
	log.Println("Open called...")

	s.id++
	handle, err := getFileHandle(string(in.Path()))
	if s.handleMap == nil {
		s.handleMap = make(map[string]*os.File)
	}
	s.handleMap[string(in.Path())] = handle
	b := flatbuffers.NewBuilder(0)
	fileoperations.OpenResponseStart(b)
	fileoperations.OpenResponseAddId(b, s.id)
	b.Finish(fileoperations.OpenResponseEnd(b))
	return b, err
}

func (s *server) Close(context context.Context, in *fileoperations.CloseRequest) (*flatbuffers.Builder, error) {
	log.Println("Close called...")
	b := flatbuffers.NewBuilder(0)
	fileoperations.CloseResponseStart(b)
	b.Finish(fileoperations.CloseResponseEnd(b))
	return b, nil
}

func (s *server) Size(context context.Context, in *fileoperations.SizeRequest) (*flatbuffers.Builder, error) {
	handle, _ := s.handleMap[string(in.Path())]
	fileInfo, err := handle.Stat()
	if err != nil {
		return nil, err
	}

	size := fileInfo.Size()

	b := flatbuffers.NewBuilder(0)
	fileoperations.SizeResponseStart(b)
	fileoperations.SizeResponseAddSize(b, size)
	b.Finish(fileoperations.SizeResponseEnd(b))

	return b, nil
}

func (s *server) StreamReadAt(in *fileoperations.ReadAtRequest, ser fileoperations.FileOpsService_StreamReadAtServer) (error) {
	log.Println("ReadAt called %v %v %v", int64(in.Offset()), int64(in.Size()), int64(in.BlockSize()))

	handle, _ := s.handleMap[string(in.Path())]
	var currentOffset int64 = int64(in.Offset())
	var doneSize int64 = 0
	var data []byte
	for doneSize < int64(in.Size()) {
		log.Printf ("Reading data at offset: %v", currentOffset)
		data = make([]byte, int64(in.BlockSize()))
		if doneSize + int64(in.BlockSize()) > int64(in.Size()) {
			data = make([]byte, int64(in.Size())-doneSize)
		}

		_, _ = handle.ReadAt(data, currentOffset)

		b := flatbuffers.NewBuilder(0)
		fileoperations.ReadAtResponseStart(b)
		fileoperations.ReadAtResponseAddOffset(b, currentOffset)
		b.Finish(fileoperations.ReadAtResponseEnd(b))

		if err := ser.Send(b); err != nil {
			return err
		}
		currentOffset += int64(in.BlockSize())
		doneSize += int64(in.BlockSize())
	}
	return nil
}





func main() {
	var addr string

	flag.StringVar(&addr, "addr", "", "Address on which server should be started")
	flag.Parse()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))

	fileoperations.RegisterFileOpsServiceServer(ser, &server{})
	if err := ser.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}