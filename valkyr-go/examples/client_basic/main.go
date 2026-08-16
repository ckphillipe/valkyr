package main

import (
	"context"
	"fmt"
	"os"

	valkyr "github.com/ckphillipe/valkyr/valkyr-go"
)

func main() {
	endpoint := os.Getenv("VALKYR_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:8081"
	}
	client, err := valkyr.Dial(context.Background(), endpoint, valkyr.WithAPIKey(os.Getenv("VALKYR_API_KEY")))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	route := client.Namespace("/examples").Key("hello")
	if err := route.Set(ctx, map[string]string{"message": "hello from Go"}); err != nil {
		panic(err)
	}
	result, err := route.GetWithRetry(ctx)
	if err != nil {
		panic(err)
	}
	switch result := result.(type) {
	case valkyr.Value:
		var value map[string]string
		if err := result.Decode(&value); err != nil {
			panic(err)
		}
		fmt.Println(value["message"])
	case valkyr.Miss:
		fmt.Printf("provider refresh pending for %s\n", result.RetryAfter)
	case valkyr.Unknown:
		fmt.Println("value is absent")
	}
}
