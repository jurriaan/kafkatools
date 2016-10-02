package kafkatools

import (
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// GetSaramaClient sets up a kafka client
func GetSaramaClient(brokers string) sarama.Client {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10

	client, err := sarama.NewClient(strings.Split(brokers, ","), config)

	if err != nil {
		log.Fatal("Failed to start client: ", err)
	}

	return client
}

// GetSaramaConsumer returns a high-level kafka consumer
func GetSaramaConsumer(brokers string, consumerGroup string, topics []string) *cluster.Consumer {
	// Init config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Version = sarama.V0_10_0_0

	consumer, err := cluster.NewConsumer(strings.Split(brokers, ","), consumerGroup, topics, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}

	return consumer
}

// GenerateOffsetRequests generates the offset requests which can be used in the GetBrokerTopicOffsets function
func GenerateOffsetRequests(client sarama.Client) (requests map[*sarama.Broker]*sarama.OffsetRequest) {
	requests = make(map[*sarama.Broker]*sarama.OffsetRequest)

	topics, err := client.Topics()
	if err != nil {
		log.Fatal("Failed to fetch topics: ", err)
	}
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			log.Fatal("Failed to fetch partitions: ", err)
		}
		for _, partition := range partitions {
			broker, err := client.Leader(topic, partition)
			if err != nil {
				log.Fatalf("Cannot fetch leader for partition %d of topic %s", partition, topic)
			}

			if _, ok := requests[broker]; !ok {
				requests[broker] = &sarama.OffsetRequest{}
			}

			requests[broker].AddBlock(topic, partition, sarama.OffsetNewest, 1)
		}
	}

	return requests
}

// GetBrokerTopicOffsets fetches the offsets for all topics from a specific groker and sends them to the offset topic
func GetBrokerTopicOffsets(broker *sarama.Broker, request *sarama.OffsetRequest, offsets chan TopicPartitionOffset) {
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		log.Fatalf("Cannot fetch offsets from broker %d: %v", broker.ID(), err)
	}
	for topic, partitions := range response.Blocks {
		for partition, offsetResponse := range partitions {
			if offsetResponse.Err != sarama.ErrNoError {
				log.Printf("Error in OffsetResponse for topic %s:%d from broker %d: %s", topic, partition, broker.ID(), offsetResponse.Err.Error())
				continue
			}
			offsets <- TopicPartitionOffset{
				Partition: partition,
				Offset:    offsetResponse.Offsets[0],
				Topic:     topic,
			}
		}
	}
}
