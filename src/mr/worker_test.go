package mr

import "testing"

func TestDoMapTask(t *testing.T) {
	type args struct {
		task *Task
		mapf func(string, string) []KeyValue
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				task: &Task{
					ID:        1,
					File:      "../main/pg-tom_sawyer.txt",
					ReduceNum: 10,
				},
				mapf: dummyMapFunc,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DoMapTask(tt.args.task, tt.args.mapf)
		})
	}
}

// 一个简单的 Map 函数，用于测试
func dummyMapFunc(filename, content string) []KeyValue {
	words := make([]string, 0)
	for _, word := range content {
		if word == ' ' || word == '\n' {
			continue
		}
		words = append(words, string(word))
	}

	kva := make([]KeyValue, len(words))
	for i, word := range words {
		kva[i] = KeyValue{Key: word, Value: "1"}
	}
	return kva
}
