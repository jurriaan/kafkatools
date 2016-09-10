package main

import (
	"fmt"
	"log"
	url "net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/Shopify/sarama"
	docopt "github.com/docopt/docopt-go"
	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/olekukonko/tablewriter"
)

var (
	version     = "0.1"
	gitref      = "unknown"
	versionInfo = `consumer_offsets %s (git rev %s)`
	usage       = `consumer_offsets - A tool for monitoring kafka consumer offsets and lag

usage:
  consumer_offsets [options]

options:
  -h --help          show this screen.
  --version          show version.
  --broker [broker]  the kafka bootstrap broker
  --influxdb [url]   send the data to influxdb (url format: influxdb://user:pass@host:port/database)
`
)

func getSaramaClient(broker string) sarama.Client {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient([]string{broker}, config)

	if err != nil {
		log.Fatal("Failed to start client: ", err)
	}

	return client
}

func getInfluxClient(urlStr string) (client influxdb.Client, batchConfig influxdb.BatchPointsConfig) {
	u, err := url.Parse(urlStr)
	if err != nil || u.Scheme != "influxdb" {
		log.Fatalf("error parsing url %v: %v", urlStr, err)
	}

	addr := &url.URL{
		Host:   u.Host,
		Scheme: "http",
		Path:   "",
	}

	database := u.Path[1:]
	log.Printf("Connecting to %s, db: %s", addr.String(), database)

	password, _ := u.User.Password()
	client, err = influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:     addr.String(),
		Username: u.User.Username(),
		Password: password,
	})

	if err != nil {
		log.Fatalln("Error: ", err)
	}

	batchConfig = influxdb.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	}

	return client, batchConfig
}

func generateOffsetRequests(client sarama.Client) (requests map[*sarama.Broker]*sarama.OffsetRequest) {
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

func main() {
	docOpts, err := docopt.Parse(usage, nil, true, fmt.Sprintf(versionInfo, version, gitref), false)

	if err != nil {
		log.Panicf("[PANIC] We couldn't parse doc opts params: %v", err)
	}

	if docOpts["--broker"] == nil {
		log.Fatal("You have to provide a broker")
	}
	broker := docOpts["--broker"].(string)

	client := getSaramaClient(broker)

	requests := generateOffsetRequests(client)

	var wg, wg2 sync.WaitGroup
	topicOffsetChannel := make(chan topicPartitionOffset, 20)
	groupOffsetChannel := make(chan groupOffset, 10)

	wg.Add(2 * len(requests))
	for broker, request := range requests {
		// Fetch topic offsets (log end)
		go func(broker *sarama.Broker, request *sarama.OffsetRequest) {
			defer wg.Done()
			getBrokerTopicOffsets(broker, request, topicOffsetChannel)
		}(broker, request)

		// Fetch group offsets
		go func(broker *sarama.Broker) {
			defer wg.Done()
			getBrokerGroupOffsets(broker, groupOffsetChannel)
		}(broker)
	}

	// Setup lookup table for topic offsets
	topicOffsets := make(map[string]map[int32]topicPartitionOffset)
	var groupOffsets groupOffsetSlice
	go func() {
		defer wg2.Done()
		wg2.Add(1)
		for topicOffset := range topicOffsetChannel {
			if _, ok := topicOffsets[topicOffset.topic]; !ok {
				topicOffsets[topicOffset.topic] = make(map[int32]topicPartitionOffset)
			}
			topicOffsets[topicOffset.topic][topicOffset.partition] = topicOffset
		}
	}()

	go func() {
		defer wg2.Done()
		wg2.Add(1)
		for offset := range groupOffsetChannel {
			groupOffsets = append(groupOffsets, offset)
		}
		sort.Sort(groupOffsets)
	}()

	// wait for goroutines to finish
	wg.Wait()
	close(topicOffsetChannel)
	close(groupOffsetChannel)
	wg2.Wait()

	if docOpts["--influxdb"] != nil {
		writeToInflux(docOpts["--influxdb"].(string), groupOffsets, topicOffsets)
	} else {
		printTable(groupOffsets, topicOffsets)
	}
}

func writeToInflux(url string, groupOffsets groupOffsetSlice, topicOffsets map[string]map[int32]topicPartitionOffset) {
	client, batchConfig := getInfluxClient(url)

	bp, batchErr := influxdb.NewBatchPoints(batchConfig)

	if batchErr != nil {
		log.Fatalln("Error: ", batchErr)
	}

	curTime := time.Now()
	for _, groupOffset := range groupOffsets {
		for _, topicOffset := range groupOffset.groupTopicOffsets {
			for _, partitionOffset := range topicOffset.topicPartitionOffsets {
				tags := map[string]string{
					"consumerGroup": groupOffset.group,
					"topic":         topicOffset.topic,
					"partition":     strconv.Itoa(int(partitionOffset.partition)),
				}

				var gOffset, tOffset, lag interface{}

				gOffset = int(partitionOffset.offset)
				tOffset = int(topicOffsets[topicOffset.topic][partitionOffset.partition].offset)
				lag = tOffset.(int) - gOffset.(int)

				fields := make(map[string]interface{})
				fields["partitionOffset"] = tOffset
				if gOffset.(int) >= 0 {
					fields["groupOffset"] = gOffset
					fields["lag"] = lag
				}

				pt, err := influxdb.NewPoint("consumer_offset", tags, fields, curTime)

				if err != nil {
					log.Fatalln("Error: ", err)
				}

				bp.AddPoint(pt)
			}
		}
	}

	// Write the batch
	err := client.Write(bp)
	if err != nil {
		log.Fatal("Could not write points to influxdb", err)
	}
	log.Println("Written points to influxdb")
}

func printTable(groupOffsets groupOffsetSlice, topicOffsets map[string]map[int32]topicPartitionOffset) {
	var totals groupTopicTotalSlice

	for _, groupOffset := range groupOffsets {
		group := fmt.Sprintf("Group %s:", groupOffset.group)
		fmt.Println(group)
		fmt.Println(strings.Repeat("=", len(group)))

		for _, topicOffset := range groupOffset.groupTopicOffsets {
			fmt.Printf("topic: %s (%d partitions)\n", topicOffset.topic, len(topicOffsets[topicOffset.topic]))
			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"partition", "end of log", "group offset", "lag"})
			totalLag := 0
			for _, partitionOffset := range topicOffset.topicPartitionOffsets {
				gOffset := partitionOffset.offset
				tOffset := topicOffsets[topicOffset.topic][partitionOffset.partition].offset

				gOffsetPretty := strconv.Itoa(int(gOffset))
				lag := tOffset - gOffset
				lagPretty := strconv.Itoa(int(lag))
				if gOffset <= -1 {
					gOffsetPretty = "--"
					lagPretty = "--"
				} else if lag > 0 {
					totalLag = totalLag + int(lag)
				}
				table.Append([]string{strconv.Itoa(int(partitionOffset.partition)), strconv.Itoa(int(tOffset)), gOffsetPretty, lagPretty})
			}
			table.SetFooter([]string{"", "", "Total", strconv.Itoa(totalLag)}) // Add Footer
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.SetFooterAlignment(tablewriter.ALIGN_LEFT)
			table.Render()

			totals = append(totals, groupTopicTotal{group: groupOffset.group, topic: topicOffset.topic, totalLag: totalLag})
		}
		fmt.Println("")
	}

	fmt.Println("TOTALS:")
	fmt.Println("=======")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"group", "topic", "total lag"})
	for _, total := range totals {
		table.Append([]string{total.group, total.topic, strconv.Itoa(total.totalLag)})
	}

	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.Render()
}

func getBrokerTopicOffsets(broker *sarama.Broker, request *sarama.OffsetRequest, offsets chan topicPartitionOffset) {
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
			offsets <- topicPartitionOffset{partition: partition, offset: offsetResponse.Offsets[0], topic: topic}
		}
	}
}

func getBrokerGroupOffsets(broker *sarama.Broker, groupOffsetChannel chan groupOffset) {
	groupsResponse, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		log.Fatal("Failed to list groups: ", err)
	}
	var groups []string
	for group := range groupsResponse.Groups {
		groups = append(groups, group)
	}
	groupsDesc, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groups})
	if err != nil {
		log.Fatal("Failed to describe groups: ", err)
	}

	var wg sync.WaitGroup
	wg.Add(len(groupsDesc.Groups))

	for _, desc := range groupsDesc.Groups {
		go func(desc *sarama.GroupDescription) {
			defer wg.Done()
			var offset groupOffset
			offset.group = desc.GroupId

			request := getOffsetFetchRequest(desc)

			offsets, err := broker.FetchOffset(request)
			if err != nil {
				log.Fatal("Failed to fetch offsets")
			}

			for topic, partitionmap := range offsets.Blocks {
				groupTopic := groupTopicOffset{topic: topic}
				for partition, block := range partitionmap {
					topicPartition := topicPartitionOffset{partition: partition, offset: block.Offset, topic: topic}
					groupTopic.topicPartitionOffsets = append(groupTopic.topicPartitionOffsets, topicPartition)
				}
				sort.Sort(groupTopic.topicPartitionOffsets)
				offset.groupTopicOffsets = append(offset.groupTopicOffsets, groupTopic)
			}

			sort.Sort(offset.groupTopicOffsets)
			groupOffsetChannel <- offset
		}(desc)
	}
	wg.Wait()
}

func getOffsetFetchRequest(desc *sarama.GroupDescription) *sarama.OffsetFetchRequest {
	request := new(sarama.OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = desc.GroupId

	for _, memberDesc := range desc.Members {
		assignArr := memberDesc.MemberAssignment
		if len(assignArr) == 0 {
			continue
		}

		assignment := parseMemberAssignment(assignArr)
		for _, topicAssignment := range assignment.assignments {
			for _, partition := range topicAssignment.partitions {
				request.AddPartition(topicAssignment.topic, partition)
			}
		}
	}

	return request
}
