package main

import (
	"context"
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	trialpb "github.com/jhoblitt/kafka-trial/trialpb"
)

// Check if the kafka topic exists
func checkTopicExists(admin *kafka.AdminClient, topicName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Describe the topic
	results, err := admin.DescribeTopics(
		ctx,
		kafka.NewTopicCollectionOfTopicNames([]string{topicName}),
	)
	if err != nil {
		return false, errors.Wrapf(err, "Failed to describe topic(s)")
	}

	// There will be one result per topic requested
	res := results.TopicDescriptions[0]
	if res.Error.Code() == kafka.ErrUnknownTopicOrPart {
		// topic does NOT exist
		return false, nil
	} else if res.Error.Code() == kafka.ErrNoError {
		// topic exists
		return true, nil
	} else {
		return false, errors.Wrapf(err, "Failed to describe topic %q", topicName)
	}
}

// Delete the kafka topic
func deleteTopic(admin *kafka.AdminClient, topicName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 3. Delete the topic (e.g., "my-topic")
	results, err := admin.DeleteTopics(
		ctx,
		[]string{topicName},
	)
	if err != nil {
		return errors.Wrapf(err, "Failed to delete topic(s)")
	}

	res := results[0]
	if res.Error.Code() != kafka.ErrNoError {
		return errors.Wrapf(res.Error, "Failed to delete topic %q", res.Topic)
	}

	// topic deleted successfully
	return nil
}

// Create the kafka topic
func createTopic(admin *kafka.AdminClient, topicName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topicSpec := []kafka.TopicSpecification{{
		Topic:             topicName,
		NumPartitions:     100,
		ReplicationFactor: 1,
	}}

	results, err := admin.CreateTopics(ctx, topicSpec)
	if err != nil {
		return errors.Wrapf(err, "Failed to create topic(s)")
	}

	res := results[0]
	if res.Error.Code() != kafka.ErrNoError {
		return errors.Wrapf(res.Error, "Failed to create topic %q", res.Topic)
	}

	// created topic successfully
	return nil
}

// consume messages from topicName
func listen(kafkaCommonConf kafka.ConfigMap, topicName string, messages *sync.Map) {
	kafkaConsumerConf := kafka.ConfigMap{
		"group.id":          "user-consumer-group",
		"auto.offset.reset": "earliest",
		//"auto.offset.reset":  "latest",
		"fetch.min.bytes":    1,
		"fetch.wait.max.ms":  5,
		"enable.auto.commit": false,
	}
	for k, v := range kafkaCommonConf {
		kafkaConsumerConf[k] = v
	}

	consumer, err := kafka.NewConsumer(&kafkaConsumerConf)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Subscribe to the topic
	err = consumer.Subscribe(topicName, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening for messages on topic %q", topicName)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Println("Consumer error:", err)
			continue
		}

		// Deserialize Protobuf message
		var trial trialpb.Trial
		err = proto.Unmarshal(msg.Value, &trial)
		if err != nil {
			log.Println("Failed to deserialize message:", err)
			continue
		}

		duration := time.Now().Sub(trial.CreatedAt.AsTime())
		log.Printf("Received %q - age %s\n", trial.Uuid, duration)
	}
}

// produce messageCount messages to topicName
func runProducer(kafkaCommonConf kafka.ConfigMap, topicName string, messageCount int64, messages *sync.Map) {
	kafkaProducerConf := kafka.ConfigMap{
		"linger.ms": 0,
	}
	for k, v := range kafkaCommonConf {
		kafkaProducerConf[k] = v
	}

	producer, err := kafka.NewProducer(&kafkaProducerConf)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// watch for when messages are flushed
	go func() {
		for event := range producer.Events() {
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					log.Println("Delivery failed:", e.TopicPartition.Error)
				} else {
					// log.Println("Message delivered:", e.TopicPartition)

					var trial trialpb.Trial
					err = proto.Unmarshal(e.Value, &trial)
					if err != nil {
						log.Println("Failed to deserialize message:", err)
					}
					duration := time.Now().Sub(trial.CreatedAt.AsTime())
					log.Printf("Sent %q - age %s\n", trial.Uuid, duration)
				}
			}
		}
	}()

	// create n messages
	for i := int64(0); i < messageCount; i++ {
		// Create a user with current time
		trial := &trialpb.Trial{
			Uuid:      uuid.New().String(),
			CreatedAt: timestamppb.New(time.Now()), // Convert time.Time to protobuf Timestamp
		}

		messages.Store(trial.Uuid, trial)

		log.Printf("Sending %q\n", trial.Uuid)

		trialBytes, err := proto.Marshal(trial)
		if err != nil {
			log.Fatal("Failed to serialize user:", err)
		}

		// Produce the message
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicName,
				Partition: kafka.PartitionAny,
			},
			Value: trialBytes,
		}

		err = producer.Produce(msg, nil)
		if err != nil {
			log.Fatal("Failed to send message:", err)
		}

		// flush each message individually
		remaining := producer.Flush(5000)
		if remaining > 0 {
			log.Fatalf("Failed to devlier all messages. %d messages remaining\n", remaining)
		}
	}
}

func main() {
	bootstrapServers := os.Getenv("KT_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		log.Fatal("env var KT_BOOTSTRAP_SERVERS is required")
	}
	saslUsername := os.Getenv("KT_SASL_USERNAME")
	if saslUsername == "" {
		log.Fatal("env var KT_SASL_USERNAME is required")
	}
	saslPassword := os.Getenv("KT_SASL_PASSWORD")
	if saslPassword == "" {
		log.Fatal("Senv var KT_SASL_PASSWORD is required")
	}
	topicName := os.Getenv("KT_TOPIC")
	if topicName == "" {
		log.Fatal("env var KT_TOPIC is required")
	}

	writerCount := flag.Int64("writers", 10, "Number of writers to run")
	messageCount := flag.Int64("messages", 10, "Number of messages to send per writer")
	flag.Parse()

	kafkaCommonConf := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "SCRAM-SHA-512",
		"sasl.username":     saslUsername,
		"sasl.password":     saslPassword,
	}

	admin, err := kafka.NewAdminClient(&kafkaCommonConf)
	if err != nil {
		log.Fatalf("Failed to create AdminClient: %v", err)
	}
	defer admin.Close()

	exists, err := checkTopicExists(admin, topicName)
	if err != nil {
		log.Fatalf("Failed to check if topic exists: %v", err)
	}

	if exists {
		// delete the topic so it can be recreated
		log.Printf("Deleting topic %q to recreate it\n", topicName)
		err := deleteTopic(admin, topicName)
		if err != nil {
			log.Fatalf("Failed to delete topic: %v", err)
		}
	}

	err = createTopic(admin, topicName)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("Created topic %q\n", topicName)

	var messages sync.Map

	// start listening for messages
	go listen(kafkaCommonConf, topicName, &messages)

	// generate messages
	var wg sync.WaitGroup

	producersStartTS := time.Now()
	for i := int64(0); i < *writerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			runProducer(kafkaCommonConf, topicName, *messageCount, &messages)
		}()
	}

	wg.Wait()
	producersEndTS := time.Now()

	// XXX temp kludge -- delay exit to listen for messages still inflight
	time.Sleep(5 * time.Second)

	producersDuration := producersEndTS.Sub(producersStartTS)
	totalMessages := *writerCount * *messageCount
	producersRate := float64(totalMessages) / producersDuration.Seconds()

	log.Printf("Producers took %s\n", producersDuration)
	log.Printf("Producers message rate %.2f/s\n", producersRate)
}
