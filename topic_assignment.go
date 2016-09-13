package kafkatools

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

// TopicAssignment contains the assigned partitions of a topic
type TopicAssignment struct {
	Topic      string
	Partitions []int32
}

// MemberAssignment contains the assignments of a consumer group member
type MemberAssignment struct {
	Version     int
	Assignments []TopicAssignment
}

// ParseMemberAssignment parses a binary byteArr
func ParseMemberAssignment(byteArr []byte) (assignments MemberAssignment) {
	buf := bytes.NewBuffer(byteArr)
	assignments.Version = int(readInt16(buf))
	elements := int(readInt32(buf))
	assignments.Assignments = make([]TopicAssignment, elements)
	for i := range assignments.Assignments {
		assignments.Assignments[i].Topic = readString(buf)
		assignments.Assignments[i].Partitions = readInt32Arr(buf)
	}

	return assignments
}

func readInt16(buf io.Reader) (val int16) {
	if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
		log.Fatal("binary.Read failed:", err)
	}
	return val
}

func readInt32(buf io.Reader) (val int32) {
	if err := binary.Read(buf, binary.BigEndian, &val); err != nil {
		log.Fatal("binary.Read failed:", err)
	}
	return val
}

func readInt32Arr(buf io.Reader) (val []int32) {
	length := readInt32(buf)
	val = make([]int32, length)
	for i := range val {
		val[i] = readInt32(buf)
	}
	return val
}

func readString(buf io.Reader) (val string) {
	length := readInt16(buf)
	bytes := make([]byte, length)
	if _, err := buf.Read(bytes); err != nil {
		log.Fatal("buf.Read failed:", err)
	}
	val = string(bytes)
	return val
}
