package kafkatools

// TopicPartitionOffset information
type TopicPartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

// GroupTopicOffset contains the partition offset of a topic
type GroupTopicOffset struct {
	Topic                 string
	TopicPartitionOffsets TopicPartitionOffsetSlice
}

// GroupOffset contains the topic offsets for a specific Group
type GroupOffset struct {
	Group             string
	GroupTopicOffsets GroupTopicOffsetSlice
}

// GroupOffsetSlice for sorting
type GroupOffsetSlice []GroupOffset

// GroupTopicOffsetSlice for sorting
type GroupTopicOffsetSlice []GroupTopicOffset

// TopicPartitionOffsetSlice for sorting
type TopicPartitionOffsetSlice []TopicPartitionOffset

func (a GroupOffsetSlice) Len() int                    { return len(a) }
func (a GroupOffsetSlice) Swap(i, j int)               { a[i], a[j] = a[j], a[i] }
func (a GroupOffsetSlice) Less(i, j int) bool          { return a[i].Group < a[j].Group }
func (a GroupTopicOffsetSlice) Len() int               { return len(a) }
func (a GroupTopicOffsetSlice) Swap(i, j int)          { a[i], a[j] = a[j], a[i] }
func (a GroupTopicOffsetSlice) Less(i, j int) bool     { return a[i].Topic < a[j].Topic }
func (a TopicPartitionOffsetSlice) Len() int           { return len(a) }
func (a TopicPartitionOffsetSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TopicPartitionOffsetSlice) Less(i, j int) bool { return a[i].Partition < a[j].Partition }
