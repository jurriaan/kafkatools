package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

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
  -h --help             show this screen.
  --version             show version.
  --broker [broker]     the kafka bootstrap broker
  --to-time [timestamp]   set offsets to a specific timestamp
  --partition [partition] only update a specific partition
  --offset [offset] update to a certain offset
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
	partition := getPartition(docOpts)

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

	offset := getOffset(docOpts)
	groupOffsets, topicOffsets := kafkatools.FetchOffsets(client, offset)

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	groupOffsetMap := generateGroupOffsetMap(groupOffsets, topics, consumerGroup)
	setConsumerOffsets(consumer, topics, topicOffsets, groupOffsetMap, partition, offset)
}

func getPartition(docOpts map[string]interface{}) int32 {
	if docOpts["--partition"] == nil {
		return int32(-1)
	}

	r, err := strconv.Atoi(docOpts["--partition"].(string))
	if err != nil {
		log.Fatal("Couldn't parse partition", err)
	}
	return int32(r)
}

func getOffset(docOpts map[string]interface{}) int64 {
	if docOpts["--to-time"] != nil {
		atTime, err := time.Parse(time.RFC3339, docOpts["--to-time"].(string))
		if err != nil {
			log.Fatal("Invalid time format specified (RFC3339 required): ", err)
		}

		// Compute time in milliseconds
		return atTime.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
	} else if docOpts["--offset"] != nil {
		r, err := strconv.Atoi(docOpts["--offset"].(string))
		if err != nil {
			log.Fatal("Couldn't parse offset", err)
		}
		return int64(r)
	}

	return sarama.OffsetNewest
}

func generateGroupOffsetMap(groupOffsets kafkatools.GroupOffsetSlice, topics []string, consumerGroup string) (groupOffsetMap map[string]map[int32]int64) {
	groupOffsetMap = make(map[string]map[int32]int64)
	for _, groupOffset := range groupOffsets {
		if groupOffset.Group == consumerGroup {
			for _, topicOffset := range groupOffset.GroupTopicOffsets {
				for _, topic := range topics {
					if topicOffset.Topic == topic {
						for _, partitionOffset := range topicOffset.TopicPartitionOffsets {
							if _, ok := groupOffsetMap[partitionOffset.Topic]; !ok {
								groupOffsetMap[partitionOffset.Topic] = make(map[int32]int64)
							}
							groupOffsetMap[partitionOffset.Topic][partitionOffset.Partition] = partitionOffset.Offset
						}
					}
				}
			}
		}
	}
	return
}

func setConsumerOffsets(consumer *cluster.Consumer, topics []string, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset, groupOffsetMap map[string]map[int32]int64, providedPartition int32, providedOffset int64) {
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
			if providedPartition != -1 {
				if providedOffset == -1 {
					providedOffset = topicPartitionOffset[providedPartition].Offset
				}

				log.Printf("Setting %s:%d's offset from %d to %d", topic, providedPartition, groupOffsetMap[topic][providedPartition], providedOffset)
				consumer.MarkPartitionOffset(topic, providedPartition, providedOffset-1, "")
			} else {
				for partition, offset := range topicPartitionOffset {
					log.Printf("Setting %s:%d's offset from %d to %d", topic, partition, groupOffsetMap[topic][partition], offset.Offset)
					consumer.MarkPartitionOffset(topic, partition, offset.Offset-1, "")
				}
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
