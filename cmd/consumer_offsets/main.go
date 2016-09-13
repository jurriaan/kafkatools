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
	"github.com/jurriaan/kafkatools"
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

func main() {
	docOpts, err := docopt.Parse(usage, nil, true, fmt.Sprintf(versionInfo, version, gitref), false)

	if err != nil {
		log.Panicf("[PANIC] We couldn't parse doc opts params: %v", err)
	}

	if docOpts["--broker"] == nil {
		log.Fatal("You have to provide a broker")
	}
	broker := docOpts["--broker"].(string)

	client := kafkatools.GetSaramaClient(broker)

	requests := kafkatools.GenerateOffsetRequests(client)

	var wg, wg2 sync.WaitGroup
	topicOffsetChannel := make(chan kafkatools.TopicPartitionOffset, 20)
	groupOffsetChannel := make(chan kafkatools.GroupOffset, 10)

	wg.Add(2 * len(requests))
	for broker, request := range requests {
		// Fetch topic offsets (log end)
		go func(broker *sarama.Broker, request *sarama.OffsetRequest) {
			defer wg.Done()
			kafkatools.GetBrokerTopicOffsets(broker, request, topicOffsetChannel)
		}(broker, request)

		// Fetch group offsets
		go func(broker *sarama.Broker) {
			defer wg.Done()
			getBrokerGroupOffsets(broker, groupOffsetChannel)
		}(broker)
	}

	// Setup lookup table for topic offsets
	topicOffsets := make(map[string]map[int32]kafkatools.TopicPartitionOffset)
	var groupOffsets kafkatools.GroupOffsetSlice
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

func writeToInflux(url string, groupOffsets kafkatools.GroupOffsetSlice, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset) {
	client, batchConfig := getInfluxClient(url)

	bp, batchErr := influxdb.NewBatchPoints(batchConfig)

	if batchErr != nil {
		log.Fatalln("Error: ", batchErr)
	}

	curTime := time.Now()

	bp = addGroupOffsetPoints(bp, topicOffsets, groupOffsets, curTime)
	bp = addTopicOffsetPoints(bp, topicOffsets, curTime)

	// Write the batch
	err := client.Write(bp)
	if err != nil {
		log.Fatal("Could not write points to influxdb", err)
	}
	log.Println("Written points to influxdb")
}

func addGroupOffsetPoints(batchPoints influxdb.BatchPoints, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset, groupOffsets kafkatools.GroupOffsetSlice, curTime time.Time) influxdb.BatchPoints {
	for _, groupOffset := range groupOffsets {
		for _, topicOffset := range groupOffset.GroupTopicOffsets {
			for _, partitionOffset := range topicOffset.TopicPartitionOffsets {
				tags := map[string]string{
					"consumerGroup": groupOffset.Group,
					"topic":         topicOffset.Topic,
					"partition":     strconv.Itoa(int(partitionOffset.Partition)),
				}

				var gOffset, tOffset, lag interface{}

				gOffset = int(partitionOffset.Offset)
				tOffset = int(topicOffsets[topicOffset.Topic][partitionOffset.Partition].Offset)
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

				batchPoints.AddPoint(pt)
			}
		}
	}
	return batchPoints
}

func addTopicOffsetPoints(batchPoints influxdb.BatchPoints, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset, curTime time.Time) influxdb.BatchPoints {
	for topic, partitionMap := range topicOffsets {
		for partition, offset := range partitionMap {
			tags := map[string]string{
				"topic":     topic,
				"partition": strconv.Itoa(int(partition)),
			}

			fields := make(map[string]interface{})
			fields["partitionOffset"] = int(offset.Offset)

			pt, err := influxdb.NewPoint("topic_offset", tags, fields, curTime)

			if err != nil {
				log.Fatalln("Error: ", err)
			}

			batchPoints.AddPoint(pt)
		}
	}

	return batchPoints
}

type groupTopicTotal struct {
	Group    string
	Topic    string
	TotalLag int
}

func printTable(groupOffsets kafkatools.GroupOffsetSlice, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset) {
	var totals []groupTopicTotal

	for _, groupOffset := range groupOffsets {
		group := fmt.Sprintf("Group %s:", groupOffset.Group)
		fmt.Println(group)
		fmt.Println(strings.Repeat("=", len(group)))

		for _, topicOffset := range groupOffset.GroupTopicOffsets {
			fmt.Printf("topic: %s (%d partitions)\n", topicOffset.Topic, len(topicOffsets[topicOffset.Topic]))
			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"partition", "end of log", "group offset", "lag"})
			totalLag := 0
			for _, partitionOffset := range topicOffset.TopicPartitionOffsets {
				gOffset := partitionOffset.Offset
				tOffset := topicOffsets[topicOffset.Topic][partitionOffset.Partition].Offset

				gOffsetPretty := strconv.Itoa(int(gOffset))
				lag := tOffset - gOffset
				lagPretty := strconv.Itoa(int(lag))
				if gOffset <= -1 {
					gOffsetPretty = "--"
					lagPretty = "--"
				} else if lag > 0 {
					totalLag = totalLag + int(lag)
				}
				table.Append([]string{strconv.Itoa(int(partitionOffset.Partition)), strconv.Itoa(int(tOffset)), gOffsetPretty, lagPretty})
			}
			table.SetFooter([]string{"", "", "Total", strconv.Itoa(totalLag)}) // Add Footer
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.SetFooterAlignment(tablewriter.ALIGN_LEFT)
			table.Render()

			totals = append(totals, groupTopicTotal{Group: groupOffset.Group, Topic: topicOffset.Topic, TotalLag: totalLag})
		}
		fmt.Println("")
	}

	fmt.Println("TOTALS:")
	fmt.Println("=======")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"group", "topic", "total lag"})
	for _, total := range totals {
		table.Append([]string{total.Group, total.Topic, strconv.Itoa(total.TotalLag)})
	}

	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.Render()
}

func getBrokerGroupOffsets(broker *sarama.Broker, groupOffsetChannel chan kafkatools.GroupOffset) {
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
			var offset kafkatools.GroupOffset
			offset.Group = desc.GroupId

			request := getOffsetFetchRequest(desc)

			offsets, err := broker.FetchOffset(request)
			if err != nil {
				log.Fatal("Failed to fetch offsets")
			}

			for topic, partitionmap := range offsets.Blocks {
				groupTopic := kafkatools.GroupTopicOffset{Topic: topic}
				for partition, block := range partitionmap {
					topicPartition := kafkatools.TopicPartitionOffset{Partition: partition, Offset: block.Offset, Topic: topic}
					groupTopic.TopicPartitionOffsets = append(groupTopic.TopicPartitionOffsets, topicPartition)
				}
				sort.Sort(groupTopic.TopicPartitionOffsets)
				offset.GroupTopicOffsets = append(offset.GroupTopicOffsets, groupTopic)
			}

			sort.Sort(offset.GroupTopicOffsets)
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

		assignment := kafkatools.ParseMemberAssignment(assignArr)
		for _, topicAssignment := range assignment.Assignments {
			for _, partition := range topicAssignment.Partitions {
				request.AddPartition(topicAssignment.Topic, partition)
			}
		}
	}

	return request
}
