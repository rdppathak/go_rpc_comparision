package main

import (
	"net"
	"log"
	"context"
	"os"
	"errors"
	"flag"

	"google.golang.org/grpc"
	"rpc/pb/fileops"
)

type fileOpsServer struct {
	id int64
	handles map[string]*os.File
}

func (s *fileOpsServer) updateHandles (path string, handle *os.File) {
	if s.handles == nil {
		s.handles = make(map[string]*os.File)
	}
	s.handles[path] = handle
	s.id ++
}

func (s *fileOpsServer) fetchHandle(path string) (*os.File) {
	handle, ok := s.handles[path]
	if !ok {
		return nil
	}
	return handle
}

func (s *fileOpsServer) Open(ctx context.Context, req *fileops.OpenRequest) (*fileops.OpenResponse, error) {
	log.Printf ("Open Called.... %v", req)
	handle, err := os.Open(req.Path)
	if err != nil {
		return nil, err
	}
	s.updateHandles(req.Path, handle)
	return &fileops.OpenResponse{Id:s.id}, nil
}

func (s *fileOpsServer) Close(ctx context.Context, req *fileops.CloseRequest) (*fileops.CloseResponse, error) {
	return &fileops.CloseResponse{}, nil
}

func (s *fileOpsServer) Size(ctx context.Context, req *fileops.SizeRequest) (*fileops.SizeResponse, error) {
	if handle := s.fetchHandle(req.Path); handle == nil {
		return nil, errors.New("Handle for requested file not found")
	} else {
		fileInfo, _ := handle.Stat()
		return &fileops.SizeResponse{Size: fileInfo.Size()}, nil
	}
}

func (s *fileOpsServer) StreamReadAt(req *fileops.ReadAtRequest, stream fileops.FileOpsService_StreamReadAtServer) (error) {
	if handle := s.fetchHandle(req.Path); handle == nil {
		return errors.New("Handle for requested file not found")
	} else {
		var doneData int64 = 0
		var data []byte
		currentOffset := req.Offset
		for doneData < req.ReadSize {
			// log.Printf ("Reading offset: %v", currentOffset)
			if currentOffset + req.BlockSize > req.ReadSize {
				data = make([]byte, req.ReadSize-currentOffset)
			} else {
				data = make([]byte, req.BlockSize)
			}

			if _, err := handle.ReadAt(data, currentOffset); err != nil {
				return err
			} else {
				resp := &fileops.Chunk{Offset: currentOffset, Data: data}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
			currentOffset += req.BlockSize
			doneData += req.BlockSize
			
		}
		return nil
	}
}

func (s *fileOpsServer ) ReaderAt (ctx context.Context, req *fileops.ReaderAtRequest) (*fileops.ReaderAtResponse, error){
	if handle := s.fetchHandle(req.Path); handle == nil {
		return &fileops.ReaderAtResponse{}, errors.New("Handle for requested file not found")
	} else {
		data := make([]byte, req.ReadSize)
		if _, err := handle.ReadAt(data, req.Offset); err != nil {
			return &fileops.ReaderAtResponse{}, err
		} else {
			resp := fileops.ReaderAtResponse{Data: data}
			return &resp, nil
		}
	}
}

func newServer() *fileOpsServer {
	s := &fileOpsServer{}
	return s
}

func main() {
	var addr string

	flag.StringVar(&addr, "addr", "", "Address on which server should be started")
	flag.Parse()

	lis, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	fileops.RegisterFileOpsServiceServer(grpcServer, newServer())
	grpcServer.Serve(lis)
}