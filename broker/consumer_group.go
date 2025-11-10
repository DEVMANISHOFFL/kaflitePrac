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
	Name    string         `json:"name"`
	Offsets map[string]int `json:"offsets"`
	mu      sync.Mutex
}

func NewConsumerGroup(name string) *ConsumerGroup {
	cg := &ConsumerGroup{
		Name:    name,
		Offsets: make(map[string]int),
	}
	cg.LoadOffSets()
	return cg
}
func (cg *ConsumerGroup) Consume(b *Broker, topicName string) ([]Message, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	topic := b.GetOrCreateTopic(topicName)
	all := topic.GetAllMessages()

	offset := cg.Offsets[topicName]
	if offset >= len(all) {
		return []Message{}, nil
	}

	newMsgs := all[offset:]
	cg.Offsets[topicName] = len(all)
	cg.SaveOffsets()

	return newMsgs, nil
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
