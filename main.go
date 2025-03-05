package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	trialpb "github.com/jhoblitt/kafka-trial/trialpb"
)

func main() {
	// Create a user with current time
	msg := &trialpb.Trial{
		Uuid:      uuid.New().String(),
		CreatedAt: timestamppb.New(time.Now()), // Convert time.Time to protobuf Timestamp
	}

	fmt.Println("Serialized User:", msg)
}
