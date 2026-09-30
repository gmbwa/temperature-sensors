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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	temperaturev1 "temperature-sensors/gen/temperature/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	devices := flag.Int("devices", 10, "number of simulated devices")
	duration := flag.Duration("duration", 0, "spread device sends across this duration (for example 60s); 0 sends all at once")
	flag.Parse()

	if *devices < 1 {
		log.Fatal("devices must be at least 1")
	}
	if *duration < 0 {
		log.Fatal("duration cannot be negative")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := temperaturev1.NewTemperatureServiceClient(conn)

	// Allow the full send window plus 30 seconds for the final requests to finish.
	timeout := 30*time.Second + *duration
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()

	var wg sync.WaitGroup
	var successful atomic.Int64
	var failed atomic.Int64
	var errorCounts [17]atomic.Int64
	var rejected atomic.Int64

	for i := 1; i <= *devices; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			if *duration > 0 {
				delay := time.Duration(int64(*duration) * int64(n-1) / int64(*devices))
				timer := time.NewTimer(delay)
				defer timer.Stop()

				select {
				case <-timer.C:
				case <-ctx.Done():
					failed.Add(1)
					errorCounts[codes.DeadlineExceeded].Add(1)
					return
				}
			}

			deviceID := fmt.Sprintf("device-%d", n)
			resp, err := client.RecordTemperature(ctx, &temperaturev1.TemperatureReading{
				DeviceId:     deviceID,
				TemperatureC: 4.0 + float64(n%10)/10,
				Timestamp:    time.Now().Unix(),
			})
			if err != nil {
				failed.Add(1)
				code := status.Code(err)
				if int(code) < len(errorCounts) {
					errorCounts[code].Add(1)
				}
				return
			}
			if !resp.GetAccepted() {
				failed.Add(1)
				rejected.Add(1)
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
	if *duration > 0 {
		fmt.Printf("Send window: %s\n", *duration)
	} else {
		fmt.Println("Send window: burst")
	}
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failed.Load())

	if failed.Load() > 0 {
		fmt.Println("Errors:")
		for code := codes.OK; code <= codes.Unauthenticated; code++ {
			if count := errorCounts[code].Load(); count > 0 {
				fmt.Printf("  %s: %d\n", code, count)
			}
		}
		if count := rejected.Load(); count > 0 {
			fmt.Printf("  Rejected: %d\n", count)
		}
	}

	fmt.Printf("Elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput: %.0f successful readings/sec\n", readingsPerSecond)
}
