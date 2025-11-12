package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const groupStorageDir = "storage/groups"

type ConsumerGroup struct {
	Name      string                 `json:"name"`
	Offsets   map[string]map[int]int `json:"offsets"`
	Consumers map[string]*Consumer
	mu        sync.Mutex
}

type Consumer struct {
	Name        string
	Assignments map[string][]int
}

func NewConsumerGroup(name string) *ConsumerGroup {
	cg := &ConsumerGroup{
		Name:      name,
		Offsets:   make(map[string]map[int]int),
		Consumers: make(map[string]*Consumer),
	}
	cg.LoadOffSets()
	return cg
}

func (cg *ConsumerGroup) AddConsumer(consumerName string, topic *Topic) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.Consumers == nil {
		cg.Consumers = make(map[string]*Consumer)
	}

	if _, exists := cg.Consumers[consumerName]; exists {
		fmt.Printf("[info] consumer '%s' already exists in group '%s'\n", consumerName, cg.Name)
		return
	}
	totalConsumers := len(cg.Consumers) + 1
	assignments := make(map[string][]int)
	for i, p := range topic.Partitions {
		if i%totalConsumers == len(cg.Consumers) {
			assignments[topic.TopicName] = append(assignments[topic.TopicName], p.ID)
		}
	}

	cg.Consumers[consumerName] = &Consumer{
		Name:        consumerName,
		Assignments: assignments,
	}
	fmt.Printf("[info] added consumer '%s' with partitions %+v\n", consumerName, assignments)
}

func (cg *ConsumerGroup) ConsumeAll(b *Broker, topicName string) ([]Message, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	topic := b.GetOrCreateTopic(topicName)

	if cg.Offsets[topicName] == nil {
		cg.Offsets[topicName] = make(map[int]int)
	}

	var newMsgs []Message
	for _, part := range topic.Partitions {
		all := part.GetMessages()
		offset := cg.Offsets[topicName][part.ID]
		if offset < len(all) {
			newMsgs = append(newMsgs, all[offset:]...)
			cg.Offsets[topicName][part.ID] = len(all)
		}
	}
	cg.SaveOffsets()
	return newMsgs, nil
}

func (cg *ConsumerGroup) ConsumeByConsumer(b *Broker, consumerName, topicName string) ([]Message, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	consumer, ok := cg.Consumers[consumerName]
	if !ok {
		return nil, fmt.Errorf("consumer '%s' not found in group '%s'", consumerName, cg.Name)
	}

	topic := b.GetOrCreateTopic(topicName)
	assignments := consumer.Assignments[topicName]
	if len(assignments) == 0 {
		return []Message{}, nil
	}

	if cg.Offsets[topicName] == nil {
		cg.Offsets[topicName] = make(map[int]int)
	}

	var msgs []Message
	for _, partID := range assignments {
		part := topic.Partitions[partID]
		all := part.GetMessages()
		offset := cg.Offsets[topicName][partID]
		if offset < len(all) {
			msgs = append(msgs, all[offset:]...)
			cg.Offsets[topicName][partID] = len(all)
		}
	}
	cg.SaveOffsets()
	return msgs, nil
}

func (cg *ConsumerGroup) SaveOffsets() error {
	if err := os.MkdirAll(groupStorageDir, os.ModePerm); err != nil {
		return err
	}
	filepath := filepath.Join(groupStorageDir, cg.Name+".json")
	data, err := json.MarshalIndent(cg, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath, data, 0644)
}

func (cg *ConsumerGroup) LoadOffSets() {
	filePath := filepath.Join(groupStorageDir, cg.Name+".json")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println("[warn] failed to read offsets for groups: ", err)
		return
	}
	_ = json.Unmarshal(data, cg)
}
