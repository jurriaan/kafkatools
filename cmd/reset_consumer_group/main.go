package main

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	docopt "github.com/docopt/docopt-go"
	"github.com/jurriaan/kafkatools"
)

var (
	version     = "0.1"
	gitrev      = "unknown"
	versionInfo = `reset_consumer_group %s (git rev %s)`
	usage       = `reset_consumer_group - a tool to reset the consumer group offset for a specific topic

usage:
  reset_consumer_group [options] <group> <topic>

options:
  -h --help          show this screen.
  --version          show version.
  --broker [broker]  the kafka bootstrap broker
`
)

func main() {
	docOpts, err := docopt.Parse(usage, nil, true, fmt.Sprintf(versionInfo, version, gitrev), false)

	if err != nil {
		log.Panicf("[PANIC] We couldn't parse doc opts params: %v", err)
	}

	if docOpts["--broker"] == nil {
		log.Fatal("You have to provide a broker")
	}
	broker := docOpts["--broker"].(string)
	topics := strings.Split(docOpts["<topic>"].(string), ",")
	consumerGroup := docOpts["<group>"].(string)

	client := kafkatools.GetSaramaClient(broker)
	consumer := kafkatools.GetSaramaConsumer(broker, consumerGroup, topics)
	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Fatal("Could not properly close the consumer")
		}
		err = client.Close()
		if err != nil {
			log.Fatal("Could not properly close the client")
		}
		log.Println("Connection closed. Bye.")
	}()

	requests := kafkatools.GenerateOffsetRequests(client)

	var wg, wg2 sync.WaitGroup
	topicOffsetChannel := make(chan kafkatools.TopicPartitionOffset, 20)

	wg.Add(len(requests))
	for broker, request := range requests {
		// Fetch topic offsets (log end)
		go func(broker *sarama.Broker, request *sarama.OffsetRequest) {
			defer wg.Done()
			kafkatools.GetBrokerTopicOffsets(broker, request, topicOffsetChannel)
		}(broker, request)
	}

	// Setup lookup table for topic offsets
	topicOffsets := make(map[string]map[int32]kafkatools.TopicPartitionOffset)
	go func() {
		defer wg2.Done()
		wg2.Add(1)
		for topicOffset := range topicOffsetChannel {
			if _, ok := topicOffsets[topicOffset.Topic]; !ok {
				topicOffsets[topicOffset.Topic] = make(map[int32]kafkatools.TopicPartitionOffset)
			}
			topicOffsets[topicOffset.Topic][topicOffset.Partition] = topicOffset
		}
	}()

	// wait for goroutines to finish
	wg.Wait()
	close(topicOffsetChannel)
	wg2.Wait()
	// Init consumer, consume errors & messages

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	setConsumerOffsets(consumer, topics, topicOffsets)
}

func setConsumerOffsets(consumer *cluster.Consumer, topics []string, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset) {
	log.Println("Waiting for consumer to join all partitions")
	log.Println("Make sure there are no other consumers listening for this to work")

	for note := range consumer.Notifications() {
		log.Printf("Rebalanced: %+v\n", note)
		if len(note.Current) != len(topics) {
			continue
		} else {
			for _, topic := range topics {
				if len(note.Current[topic]) != len(topicOffsets[topic]) {
					continue
				}
			}
		}

		for _, topic := range topics {
			topicPartitionOffset := topicOffsets[topic]
			for partition, offset := range topicPartitionOffset {
				log.Printf("Setting %s:%d's offset to %d", topic, partition, offset.Offset)
				consumer.MarkPartitionOffset(topic, partition, offset.Offset, "")
			}
		}

		err := consumer.CommitOffsets()
		if err != nil {
			log.Fatal("Error committing offsets", err)
		}

		log.Println("Quitting")
		return
	}

}
