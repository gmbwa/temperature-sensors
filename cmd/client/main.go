package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	temperaturev1 "temperature-sensors/gen/temperature/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := temperaturev1.NewTemperatureServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RecordTemperature(ctx, &temperaturev1.TemperatureReading{
		DeviceId:     "device-123",
		TemperatureC: 4.7,
		Timestamp:    time.Now().Unix(),
	})
	if err != nil {
		log.Fatalf("RecordTemperature: %v", err)
	}

	fmt.Printf("accepted: %t\n", resp.GetAccepted())
}
