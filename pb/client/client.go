package main

import (
	// "fmt"
	// "sync"
	"time"
	"io"
	"context"
	"log"
	"rpc/pb/fileops"
	"google.golang.org/grpc"
)

type Chunk struct {
	Offset int64
	Data []byte
}

type ReadAtImpl struct {
	client fileops.FileOpsServiceClient
	path string
	id int64
	serverAddr string
	inProgress bool
	Buffer chan* Chunk
}

func NewReadAtImpl(serverAddr string) (ReadAtImpl) {
	return ReadAtImpl{serverAddr: serverAddr}
}

func (r *ReadAtImpl) Open(path string) error {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(r.serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	r.client = fileops.NewFileOpsServiceClient(conn)
	log.Printf ("%v", r.client)

	ctx, _ := context.WithCancel(context.Background()) 
	in, err := r.client.Open(ctx, &fileops.OpenRequest{Path:path})
	log.Printf ("%v, %v", in, err)
	return nil
}

func (r *ReadAtImpl) Size (path string) (int64) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	resp, err := r.client.Size(ctx, &fileops.SizeRequest{Path:path})
	if err != nil {
		log.Fatalf ("Failed to fetch disk size")
	}
	return resp.Size
}
func (r *ReadAtImpl) ReadAt(path string, readSize int64, offset int64) (int64, error) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	startTime := time.Now()
	readAtRequest := &fileops.ReadAtRequest{Path:path, Offset: offset, BlockSize: 512*1024, ReadSize: readSize}
	streamData, err := r.client.StreamReadAt(ctx, readAtRequest)

	if err != nil {
		log.Fatalf ("Unable to start data stream: %v", err)
	}

	for {
		stime := time.Now()
		out, err := streamData.Recv()
		etime := time.Now()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf  ("Failed to read streamed data: %v", err.Error())
		}
		log.Printf ("Received Offset: %v, DataLen: %v, time take: %s", out.Offset, len(out.Data), (etime.Sub(stime)))
	}
	endTime := time.Now()

	log.Printf ("Total Time to transfer %d data, %s", readSize, (endTime.Sub(startTime)))
	return 0, nil
}

func (r *ReadAtImpl) MReadAt(path string, size int64) (error) {
	log.Printf ("Starting disk read at: %d", size)
	var currentOffset int64 = 0
	var blockSize int64 = 512 * 1024
	var readSize int64
	startTime := time.Now()
	for currentOffset < size {
		readSize = blockSize
		if currentOffset + blockSize > size {
			readSize = size -currentOffset
		}
		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()
		stime := time.Now()
		_, err := r.client.ReaderAt(ctx, &fileops.ReaderAtRequest{Offset: currentOffset, ReadSize: readSize, Path: path})
		etime := time.Now()
		if err != nil {
			log.Fatalf ("Failed to call RPC readAt: %v", err)
			return err
		}
		log.Printf ("Time to read data: %s", etime.Sub(stime))

		currentOffset += blockSize
	}
	endTime := time.Now()
	log.Printf ("Total Time to transfer %d data, %s", readSize, (endTime.Sub(startTime)))
	return nil
}

func (r *ReadAtImpl) Close() (error) {
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	_, err := r.client.Close(ctx, &fileops.CloseRequest{})
	if err !=nil {
		log.Fatalf ("Failed to close disk connection")
	}
	log.Printf ("Disk Connection closed successfully")
	return nil
}

func main() {
	var path string = "C:\\DynamicVhdx.vhdx"
	readAtImpl := NewReadAtImpl("10.5.221.4:10000")
	readAtImpl.Open(path)
	defer readAtImpl.Close()
	size := readAtImpl.Size(path)
	// readAtImpl.ReadAt(path, size, 0)
	_ = readAtImpl.MReadAt(path, size)
}