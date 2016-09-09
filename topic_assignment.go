package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

type topicAssignment struct {
	topic      string
	partitions []int32
}

type memberAssignment struct {
	version     int
	assignments []topicAssignment
}

// func main() {
//	//	b := []byte{0, 1, 0, 0, 0, 1, 0, 24, 117, 112, 100, 97, 116, 101, 100, 45, 105, 116, 101, 109, 45, 105, 100, 115, 45, 115, 116, 97, 103, 105, 110, 103, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 255, 255, 255, 255}
//	b := []byte{0, 1, 0, 0, 0, 1, 0, 24, 110, 101, 119, 115, 108, 101, 116, 116, 101, 114, 45, 117, 115, 101, 114, 115, 45, 115, 116, 97, 103, 105, 110, 103, 0, 0, 0, 1, 0, 0, 0, 5, 255, 255, 255, 255}
//	ass := parseMemberAssignment(b)
//	log.Println(ass)
// }

func parseMemberAssignment(byteArr []byte) (assignments memberAssignment) {
	buf := bytes.NewBuffer(byteArr)
	assignments.version = int(readInt16(buf))
	elements := int(readInt32(buf))
	assignments.assignments = make([]topicAssignment, elements)
	for i := range assignments.assignments {
		assignments.assignments[i].topic = readString(buf)
		assignments.assignments[i].partitions = readInt32Arr(buf)
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
