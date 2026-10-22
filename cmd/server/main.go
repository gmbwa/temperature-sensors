package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"

	temperaturev1 "temperature-sensors/gen/temperature/v1"
)

const (
	readingBufferSize = 10000
	workerCount       = 20
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC listen address")
	flag.Parse()

	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalf("load .env: %v", err)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("database config: %v", err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("database connection: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	readings := make(chan *temperaturev1.TemperatureReading, readingBufferSize)
	for i := 0; i < workerCount; i++ {
		go processReadings(db, readings)
	}

	grpcServer := grpc.NewServer()
	temperaturev1.RegisterTemperatureServiceServer(grpcServer, &temperatureServer{readings: readings})

	log.Printf("TemperatureService listening on %s with %d database workers", *addr, workerCount)
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
		// Accepted means queued in memory, NOT committed to PostgreSQL.
		return &temperaturev1.RecordTemperatureResponse{Accepted: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func processReadings(db *pgxpool.Pool, readings <-chan *temperaturev1.TemperatureReading) {
	for reading := range readings {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := db.Exec(ctx,
			"INSERT INTO temperature_readings (device_id, temperature_c, recorded_at) VALUES ($1, $2, to_timestamp($3))",
			reading.GetDeviceId(), reading.GetTemperatureC(), reading.GetTimestamp(),
		)
		cancel()
		if err != nil {
			log.Printf("database insert failed for device %s: %v", reading.GetDeviceId(), err)
		}
	}
}
