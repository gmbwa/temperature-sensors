package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	temperaturev1 "temperature-sensors/gen/temperature/v1"
)

const readingBufferSize = 10000

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	readings := make(chan *temperaturev1.TemperatureReading, readingBufferSize)
	go processReadings(readings)

	grpcServer := grpc.NewServer()
	temperaturev1.RegisterTemperatureServiceServer(grpcServer, &temperatureServer{
		readings: readings,
	})

	log.Printf("TemperatureService listening on %s", *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

type temperatureServer struct {
	temperaturev1.UnimplementedTemperatureServiceServer
	readings chan<- *temperaturev1.TemperatureReading
}

func (s *temperatureServer) RecordTemperature(
	ctx context.Context,
	reading *temperaturev1.TemperatureReading,
) (*temperaturev1.RecordTemperatureResponse, error) {
	select {
	case s.readings <- reading:
		return &temperaturev1.RecordTemperatureResponse{Accepted: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func processReadings(readings <-chan *temperaturev1.TemperatureReading) {
	for range readings {
		// Simulate slow downstream processing so we can observe the channel
		// filling and backpressure propagating to the gRPC handlers.
		time.Sleep(time.Millisecond)
	}
}
