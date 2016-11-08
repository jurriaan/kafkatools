package main

import (
	"fmt"
	"log"
	url "net/url"
	"os"
	"strings"
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
	gitrev      = "unknown"
	versionInfo = `consumer_offsets %s (git rev %s)`
	usage       = `consumer_offsets - A tool for monitoring kafka consumer offsets and lag

usage:
  consumer_offsets [options]

options:
  -h --help             show this screen.
  --version             show version.
  --broker [broker]     the kafka bootstrap broker
  --at-time [timestamp] fetch offsets at a specific timestamp
  --influxdb [url]      send the data to influxdb (url format: influxdb://user:pass@host:port/database)
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
	docOpts, err := docopt.Parse(usage, nil, true, fmt.Sprintf(versionInfo, version, gitrev), false)

	if err != nil {
		log.Panicf("[PANIC] We couldn't parse doc opts params: %v", err)
	}

	if docOpts["--broker"] == nil {
		log.Fatal("You have to provide a broker")

	}
	broker := docOpts["--broker"].(string)

	client := kafkatools.GetSaramaClient(broker)

	if docOpts["--influxdb"] != nil {
		influxClient, batchConfig := getInfluxClient(docOpts["--influxdb"].(string))

		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			log.Println("Sending metrics to InfluxDB")
			groupOffsets, topicOffsets := kafkatools.FetchOffsets(client, sarama.OffsetNewest)
			writeToInflux(influxClient, batchConfig, groupOffsets, topicOffsets)
		}
	} else {
		offset := sarama.OffsetNewest

		if docOpts["--at-time"] != nil {
			atTime, err := time.Parse(time.RFC3339, docOpts["--at-time"].(string))
			if err != nil {
				log.Fatal("Invalid time format specified (RFC3339 required): ", err)
			}

			// Compute time in milliseconds
			offset = atTime.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		}
		groupOffsets, topicOffsets := kafkatools.FetchOffsets(client, offset)
		printTable(groupOffsets, topicOffsets)
	}
}

func writeToInflux(client influxdb.Client, batchConfig influxdb.BatchPointsConfig, groupOffsets kafkatools.GroupOffsetSlice, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset) {
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
			totalPartitionOffset := 0
			totalGroupOffset := 0
			totalLag := 0
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
				totalPartitionOffset += tOffset.(int)
				if gOffset.(int) >= 0 {
					fields["groupOffset"] = gOffset
					totalGroupOffset += gOffset.(int)
					fields["lag"] = lag
					totalLag += lag.(int)
				}

				pt, err := influxdb.NewPoint("consumer_offset", tags, fields, curTime)

				if err != nil {
					log.Fatalln("Error: ", err)
				}

				batchPoints.AddPoint(pt)
			}

			tags := map[string]string{
				"consumerGroup": groupOffset.Group,
				"topic":         topicOffset.Topic,
				"partition":     "*",
			}

			fields := map[string]interface{}{
				"lag":             totalLag,
				"groupOffset":     totalGroupOffset,
				"partitionOffset": totalPartitionOffset,
			}

			pt, err := influxdb.NewPoint("consumer_offset", tags, fields, curTime)

			if err != nil {
				log.Fatalln("Error: ", err)
			}

			batchPoints.AddPoint(pt)
		}
	}
	return batchPoints
}

func addTopicOffsetPoints(batchPoints influxdb.BatchPoints, topicOffsets map[string]map[int32]kafkatools.TopicPartitionOffset, curTime time.Time) influxdb.BatchPoints {
	for topic, partitionMap := range topicOffsets {
		var totalOffset int64
		for partition, offset := range partitionMap {
			tags := map[string]string{
				"topic":     topic,
				"partition": strconv.Itoa(int(partition)),
			}

			fields := make(map[string]interface{})
			fields["partitionOffset"] = int(offset.Offset)

			totalOffset += offset.Offset

			pt, err := influxdb.NewPoint("topic_offset", tags, fields, curTime)

			if err != nil {
				log.Fatalln("Error: ", err)
			}

			batchPoints.AddPoint(pt)
		}

		tags := map[string]string{
			"topic":     topic,
			"partition": "*",
		}

		fields := map[string]interface{}{
			"partitionOffset": totalOffset,
		}

		pt, err := influxdb.NewPoint("topic_offset", tags, fields, curTime)

		if err != nil {
			log.Fatalln("Error: ", err)
		}

		batchPoints.AddPoint(pt)
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
