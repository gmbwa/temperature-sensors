package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	temperaturev1 "temperature-sensors/gen/temperature/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	devices := flag.Int("devices", 10, "number of simulated devices")
	flag.Parse()

	if *devices < 1 {
		log.Fatal("devices must be at least 1")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := temperaturev1.NewTemperatureServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()

	var wg sync.WaitGroup
	var successful atomic.Int64
	var failed atomic.Int64

	for i := 1; i <= *devices; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			deviceID := fmt.Sprintf("device-%d", n)
			resp, err := client.RecordTemperature(ctx, &temperaturev1.TemperatureReading{
				DeviceId:     deviceID,
				TemperatureC: 4.0 + float64(n%10)/10,
				Timestamp:    time.Now().Unix(),
			})
			if err != nil || !resp.GetAccepted() {
				failed.Add(1)
				return
			}
			successful.Add(1)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	successCount := successful.Load()
	readingsPerSecond := float64(successCount) / elapsed.Seconds()

	fmt.Printf("Devices: %d\n", *devices)
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failed.Load())
	fmt.Printf("Elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput: %.0f successful readings/sec\n", readingsPerSecond)
}
