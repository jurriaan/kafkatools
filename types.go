package main

type topicPartitionOffset struct {
	topic     string
	partition int32
	offset    int64
}

type groupTopicOffset struct {
	topic                 string
	topicPartitionOffsets topicPartitionOffsetSlice
}
type groupOffset struct {
	group             string
	groupTopicOffsets groupTopicOffsetSlice
}

type groupOffsetSlice []groupOffset
type groupTopicOffsetSlice []groupTopicOffset
type topicPartitionOffsetSlice []topicPartitionOffset

func (a groupOffsetSlice) Len() int                    { return len(a) }
func (a groupOffsetSlice) Swap(i, j int)               { a[i], a[j] = a[j], a[i] }
func (a groupOffsetSlice) Less(i, j int) bool          { return a[i].group < a[j].group }
func (a groupTopicOffsetSlice) Len() int               { return len(a) }
func (a groupTopicOffsetSlice) Swap(i, j int)          { a[i], a[j] = a[j], a[i] }
func (a groupTopicOffsetSlice) Less(i, j int) bool     { return a[i].topic < a[j].topic }
func (a topicPartitionOffsetSlice) Len() int           { return len(a) }
func (a topicPartitionOffsetSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a topicPartitionOffsetSlice) Less(i, j int) bool { return a[i].partition < a[j].partition }
