package main

import (
	"reflect"
	"testing"
)

func TestComplexTopicAssignment(t *testing.T) {
	data := []byte{0, 0, 0, 0, 0, 4, 0, 8, 116, 101, 115, 116, 105, 110, 103, 50, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 7, 116, 101, 115, 116, 105, 110, 103, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 8, 116, 101, 115, 116, 105, 110, 103, 51, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 8, 116, 101, 115, 116, 105, 110, 103, 52, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 10, 0, 0, 0, 11, 0, 0, 0, 12, 0, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 0}

	out := parseMemberAssignment(data)
	if out.version != 0 {
		t.Error("Expected version to equal 0, got ", out.version)
	}

	if len(out.assignments) != 4 {
		t.Error("Expected 4 assignments, got ", len(out.assignments))
	}

	topicNames := make([]string, 4)

	for i, assignment := range out.assignments {
		topicNames[i] = assignment.topic
	}

	expect := []string{"testing2", "testing", "testing3", "testing4"}

	if !reflect.DeepEqual(topicNames, expect) {
		t.Errorf("Expected the topics %v, got %v", expect, topicNames)
	}
}
