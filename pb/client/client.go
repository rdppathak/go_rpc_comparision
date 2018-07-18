package main

import (
	// "fmt"
	// "sync"
	"os"
	"encoding/json"
	"io/ioutil"
	"time"
	"io"
	"context"
	"log"
	"flag"
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
func (r *ReadAtImpl) StreamReadAt(path string, readSize int64, offset int64) (int64, error) {
	var totalCalls int64 = 0
	var averageCallDur time.Duration
	var totalDuration time.Duration 
	var minCallDuration time.Duration = time.Minute
	var maxCallDuration time.Duration = time.Nanosecond

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	fStartTime := time.Now()

	readAtRequest := &fileops.ReadAtRequest{Path:path, Offset: offset, BlockSize: 512*1024, ReadSize: readSize}
	streamData, err := r.client.StreamReadAt(ctx, readAtRequest)

	if err != nil {
		log.Fatalf ("Unable to start data stream: %v", err)
	}

	for {
		stime := time.Now()
		_, err := streamData.Recv()
		etime := time.Now()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf  ("Failed to read streamed data: %v", err.Error())
		}
		// log.Printf ("Received Offset: %v, DataLen: %v, time take: %s", out.Offset, len(out.Data), (etime.Sub(stime)))
		callDuration := etime.Sub(stime)
		// log.Printf ("Time to read data: %s", etime.Sub(stime))
		totalDuration += callDuration
		totalCalls++

		if callDuration <minCallDuration {
			minCallDuration = callDuration
		}

		if callDuration >maxCallDuration {
			maxCallDuration = callDuration
		}
	}
	fEndTime := time.Now()
	averageCallDur = time.Duration(totalDuration.Nanoseconds() /totalCalls)
	totalDuration += fEndTime.Sub(fStartTime)

	log.Printf ("Total Calls: %d, Average Call Duration: %s, Total Duration: %s", totalCalls, averageCallDur, totalDuration)
	log.Printf ("Minimum Call Duration: %s, Maximum Call Duration: %s", minCallDuration, maxCallDuration)
	return 0, nil
}

func (r *ReadAtImpl) ReadAt(path string, size int64) (error) {
	log.Printf ("Starting disk read at: %d", size)
	var currentOffset int64 = 0
	var blockSize int64 = 512 * 1024
	var readSize int64

	var totalCalls int64 = 0
	var averageCallDur time.Duration
	var totalDuration time.Duration 
	var minCallDuration time.Duration = time.Minute
	var maxCallDuration time.Duration = time.Nanosecond
	
	fStartTime := time.Now()
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
		callDuration := etime.Sub(stime)
		// log.Printf ("Time to read data: %s", etime.Sub(stime))
		totalDuration += callDuration
		totalCalls++

		if callDuration <minCallDuration {
			minCallDuration = callDuration
		}

		if callDuration >maxCallDuration {
			maxCallDuration = callDuration
		}

		currentOffset += blockSize
	}
	fEndTime := time.Now()
	averageCallDur = time.Duration(totalDuration.Nanoseconds() /totalCalls)
	totalDuration += fEndTime.Sub(fStartTime)

	log.Printf ("Total Calls: %d, Average Call Duration: %s, Total Duration: %s", totalCalls, averageCallDur, totalDuration)
	log.Printf ("Minimum Call Duration: %s, Maximum Call Duration: %s", minCallDuration, maxCallDuration)
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

	readAtImpl := NewReadAtImpl(config.Addr)
	readAtImpl.Open(config.Path)
	defer readAtImpl.Close()
	size = config.Size
	if config.Size == 0{
		size = readAtImpl.Size(config.Path)
	}
	if stream {
		log.Printf ("Using stream mode to transfer data")
		readAtImpl.StreamReadAt(config.Path, size, 0)
	} else {
		log.Printf ("Using non-stream mode to transfer data")
		_ = readAtImpl.ReadAt(config.Path, size)
	}
}