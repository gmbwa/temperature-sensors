package main

import (
	"context"
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	temperaturev1 "temperature-sensors/gen/temperature/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	temperaturev1.RegisterTemperatureServiceServer(grpcServer, &temperatureServer{})

	log.Printf("TemperatureService listening on %s", *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

type temperatureServer struct {
	temperaturev1.UnimplementedTemperatureServiceServer
}

func (s *temperatureServer) RecordTemperature(
	_ context.Context,
	reading *temperaturev1.TemperatureReading,
) (*temperaturev1.RecordTemperatureResponse, error) {
	log.Printf("%s: %.1f°C", reading.GetDeviceId(), reading.GetTemperatureC())
	return &temperaturev1.RecordTemperatureResponse{Accepted: true}, nil
}
