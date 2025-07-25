package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func testredis() {
	// Create a new Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Create a context
	ctx := context.Background()

	// Set TTL for "existing_key"
	err := rdb.Set(ctx, "existing_key", "some_value", 10*time.Second).Err()
	if err != nil {
		fmt.Println("Error setting TTL for existing_key:", err)
		return
	}

	// Use pipeline to execute TTL command for both existing and non-existing keys
	pipe := rdb.Pipeline()

	// Assuming "existing_key" exists and "non_existing_key" does not exist
	pipe.TTL(ctx, "existing_key")
	pipe.TTL(ctx, "non_existing_key")

	// Execute the pipeline
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Println("Error executing pipeline:", err)
		return
	}

	// Print the results
	for _, cmd := range cmds {
		fmt.Println(cmd.String())
	}
}
