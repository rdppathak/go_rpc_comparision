package main

import (
	"context"
	"log"
	"flag"
	"io"
	"time"
	"os"
	"encoding/json"
	"io/ioutil"
	flatbuffers "github.com/google/flatbuffers/go"
	"rpc/fb/fileoperations"

	"google.golang.org/grpc"
)


type FlatBufferClient struct {
	addr string
	path string
	client fileoperations.FileOpsServiceClient
}

func buildOpenRequest(path string) (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	strPath := b.CreateString(path)
	fileoperations.OpenRequestStart(b)
	fileoperations.OpenRequestAddPath(b, strPath)
	b.Finish(fileoperations.OpenRequestEnd(b))
	return b
}

func buildStreamReadAtRequest(path string, offset int64, blockSize int64, size int64) (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	strPath := b.CreateString(path)
	fileoperations.StreamReadAtRequestStart(b)
	fileoperations.StreamReadAtRequestAddPath(b, strPath)
	fileoperations.StreamReadAtRequestAddOffset(b, offset)
	fileoperations.StreamReadAtRequestAddBlockSize(b, blockSize)
	fileoperations.StreamReadAtRequestAddSize(b, size)
	b.Finish(fileoperations.StreamReadAtRequestEnd(b))
	return b
}

func buildReadAtRequest(path string, offset int64, size int64) (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	strPath := b.CreateString(path)
	fileoperations.ReadAtRequestStart(b)
	fileoperations.ReadAtRequestAddPath(b, strPath)
	fileoperations.ReadAtRequestAddOffset(b, offset)
	fileoperations.ReadAtRequestAddSize(b, size)
	b.Finish(fileoperations.ReadAtRequestEnd(b))
	return b
}

func buildSizeRequest(path string) (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	strPath := b.CreateString(path)
	fileoperations.SizeRequestStart(b)
	fileoperations.SizeRequestAddPath(b, strPath)
	b.Finish(fileoperations.CloseRequestEnd(b))
	return b
}

func buildCloseRequest(path string) (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	strPath := b.CreateString(path)
	fileoperations.CloseRequestStart(b)
	fileoperations.CloseRequestAddPath(b, strPath)
	b.Finish(fileoperations.CloseRequestEnd(b))
	return b
}

func NewFlatBufferClient (addr string, path string) (*FlatBufferClient) {
	return &FlatBufferClient{addr:addr, path:path}
}

func (f *FlatBufferClient) Open () {
	conn, err := grpc.Dial(f.addr, grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	f.client = fileoperations.NewFileOpsServiceClient(conn)

	out, err := f.client.Open(context.Background(), buildOpenRequest(f.path))
	if err != nil {
		log.Fatalf("Retrieve client failed: %v", err)
	}

	log.Println ("Open Response: %v", out)
}

func (f *FlatBufferClient) StreamReadAt(offset int64, blockSize int64, size int64) {
	var totalCalls int64 = 0
	var averageCallDur time.Duration
	var totalDuration time.Duration 
	var minCallDuration time.Duration = time.Minute
	var maxCallDuration time.Duration = time.Nanosecond

	fStartTime := time.Now()
	b := buildStreamReadAtRequest(f.path, offset, blockSize, size)
	out, err := f.client.StreamReadAt(context.Background(), b)

	if err != nil {
		log.Fatalf ("Failed to initiate ReadAt: %v", err)
	}
	
	for {
		cStartTime := time.Now()
		_, err := out.Recv()
		cEndTime := time.Now()
		callDuration := cEndTime.Sub(cStartTime)
		totalDuration += callDuration
		totalCalls++
		if callDuration <minCallDuration {
			minCallDuration = callDuration
		}

		if callDuration >maxCallDuration {
			maxCallDuration = callDuration
		}

		if err == io.EOF {
			log.Printf ("Completed data reading")
			break
		}
		if err != nil {
			log.Fatalf("Failed during data read")
		}
		// log.Printf ("Received Offset: %v", resp.Offset())
	}
	fEndTime := time.Now()

	averageCallDur = time.Duration(totalDuration.Nanoseconds() /totalCalls)
	totalDuration += fEndTime.Sub(fStartTime)

	log.Printf ("Total Calls: %d, Average Call Duration: %s, Total Duration: %s", totalCalls, averageCallDur, totalDuration)
	log.Printf ("Minimum Call Duration: %s, Maximum Call Duration: %s", minCallDuration, maxCallDuration)
}

func(f *FlatBufferClient) ReadAt(offset int64, blockSize int64, size int64) {
	log.Printf ("Calling ReadAt....")
	var currentOffset int64 = offset
	var doneSize int64 = 0
	var totalCalls int64 = 0
	var averageCallDur time.Duration
	var totalDuration time.Duration 
	var minCallDuration time.Duration = time.Minute
	var maxCallDuration time.Duration = time.Nanosecond

	fStartTime := time.Now()

	for doneSize < size {
		if doneSize + blockSize > size {
			blockSize = size - doneSize
		}

		cStartTime := time.Now()
		b := buildReadAtRequest(f.path, currentOffset, blockSize)
		_, err := f.client.ReadAt(context.Background(), b)
		cEndTime := time.Now()
		callDuration := cEndTime.Sub(cStartTime)
		// log.Printf ("Call duration :%s", callDuration)
		totalDuration += callDuration
		totalCalls++
		if callDuration <minCallDuration {
			minCallDuration = callDuration
		}

		if callDuration >maxCallDuration {
			maxCallDuration = callDuration
		}

		if err != nil {
			log.Fatalf("Failed to ReadAt: %s", err)
		}

		currentOffset += blockSize
		doneSize += blockSize

	}
	fEndTime := time.Now()

	averageCallDur = time.Duration(totalDuration.Nanoseconds() /totalCalls)
	totalDuration += fEndTime.Sub(fStartTime)

	log.Printf ("Total Calls: %d, Average Call Duration: %s, Total Duration: %s", totalCalls, averageCallDur, totalDuration)
	log.Printf ("Minimum Call Duration: %s, Maximum Call Duration: %s", minCallDuration, maxCallDuration)
}

func (f *FlatBufferClient) Size() (int64) {
	b := buildSizeRequest(f.path)
	size, err := f.client.Size(context.Background(), b)
	if err != nil {
		log.Fatalf ("Failed to fetch file size: %s", err)
	}
	return int64(size.Size())
}

func (f *FlatBufferClient) Close () {
	_, err := f.client.Close(context.Background(), buildCloseRequest(f.path))
	if err != nil {
		log.Fatalf("Retrieve client failed: %v", err)
	}
}

type Config struct {
	Path string 	`json:"path"`
	Addr string 	`json:"addr"`
	Offset int64 	`json:"offset"`
	BlockSize int64 `json:"blocksize"`
	Size int64 		`json:"size"`
}

func main() {
	var configFile string
	var stream bool
	var size int64 = 0
	config := Config {}

	flag.StringVar(&configFile, "fconfig", "", "Configuration file for client")
	flag.BoolVar(&stream, "stream", false, "Transfer data using stream or non-stream mode")

	flag.Parse()

	jsonFile, err := os.Open(configFile)

	if err != nil{
		log.Fatalf ("Failed to open test configuration file: %v", err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	err = json.Unmarshal(byteValue, &config)

	log.Printf ("Server Address: %s, File Path: %s", config.Addr, config.Path)

	fbClient := NewFlatBufferClient(config.Addr, config.Path)
	fbClient.Open()
	defer fbClient.Close()
	if config.Size == 0 {
		size = fbClient.Size()
	}

	if stream {
		log.Printf ("Using stream mode to transfer data")
		fbClient.StreamReadAt(config.Offset, config.BlockSize, size)
	} else{
		log.Printf ("Using non-stream mode to transfer data")
		fbClient.ReadAt(config.Offset, config.BlockSize, size)
		}
}