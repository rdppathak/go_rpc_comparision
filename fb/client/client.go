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

func buildReadAtRequest(path string, offset int64, blockSize int64, size int64) (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	strPath := b.CreateString(path)
	fileoperations.ReadAtRequestStart(b)
	fileoperations.ReadAtRequestAddPath(b, strPath)
	fileoperations.ReadAtRequestAddOffset(b, offset)
	fileoperations.ReadAtRequestAddBlockSize(b, blockSize)
	fileoperations.ReadAtRequestAddSize(b, size)
	b.Finish(fileoperations.ReadAtRequestEnd(b))
	return b
}

func buildCloseRequest() (*flatbuffers.Builder) {
	b := flatbuffers.NewBuilder(0)
	fileoperations.CloseRequestStart(b)
	b.Finish(fileoperations.CloseRequestEnd(b))
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
	var totalCalls int = 0
	var averageCallDur float64 = 0.0
	var totalDuration int64 = 0
	var minCallDuration int64 = 100
	var maxCallDuration int64 = 0
	fStartTime := time.Now().Unix()
	b := buildReadAtRequest(f.path, offset, blockSize, size)
	out, err := f.client.StreamReadAt(context.Background(), b)
	if err != nil {
		log.Fatalf ("Failed to initiate ReadAt: %v", err)
	}
	
	for {
		cStartTime := time.Now().Unix()
		resp, err := out.Recv()
		cEndTime := time.Now().Unix()
		callDuration := (cEndTime-cStartTime)
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
		log.Printf ("Received Offset: %v", resp.Offset())
	}
	fEndTime := time.Now().Unix()

	averageCallDur = float64(totalDuration) /float64(totalCalls)
	totalDuration += (fEndTime-fStartTime)

	log.Printf ("Total Calls: %d, Average Call Duration: %f, Total Duration: %d", totalCalls, averageCallDur, totalDuration)
	log.Printf ("Minimum Call Duration: %d, Maximum Call Duration: %d", minCallDuration, maxCallDuration)
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
	_, err := f.client.Close(context.Background(), buildCloseRequest())
	if err != nil {
		log.Fatalf("Retrieve client failed: %v", err)
	}
}

type Config struct {
	Path string 	`json:"path"`
	Addr string 	`json:"addr"`
	Offset int64 	`json:"offset"`
	BlockSize int64 `json:"blocksize"`
}

func main() {
	var configFile string
	config := Config {}

	flag.StringVar(&configFile, "fconfig", "", "Configuration file for client")

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
	size := fbClient.Size()
	fbClient.StreamReadAt(config.Offset, config.BlockSize, size)
}