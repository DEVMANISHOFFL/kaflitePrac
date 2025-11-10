package broker

import (
	"fmt"
	"sync"
	"time"
)

type Topic struct {
	TopicName  string
	Partitions []*Partition
	mu         sync.Mutex
	nextPart   int
}

type Message struct {
	ID        int       `json:"id"`
	Value     string    `json:"value"`
	TimeStamp time.Time `json:"timestamp"`
}

func NewTopic(name string, numParts int) *Topic {
	partitions := make([]*Partition, numParts)
	for i := range partitions {
		partitions[i] = &Partition{ID: i}
	}
	return &Topic{
		TopicName:  name,
		Partitions: partitions,
		nextPart:   0,
	}
}

func (t *Topic) AddMessage(value string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	part := t.Partitions[t.nextPart]
	t.nextPart = (t.nextPart + 1) % len(t.Partitions)

	msg := Message{
		ID:        part.Size() + 1,
		Value:     value,
		TimeStamp: time.Now(),
	}
	part.Append(msg)
	fmt.Printf("[info] Added message to topic '%s' partition %d\n", t.TopicName, part.ID)

	SavePartition(t.TopicName, part.ID, part.GetMessages())
}

func (t *Topic) GetAllMessages() []Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	var all []Message
	for _, p := range t.Partitions {
		all = append(all, p.GetMessages()...)
	}
	return all
}

func (t *Topic) RestoreFromDisk() {
	partitionData, err := LoadPartitions(t.TopicName)
	if err != nil {
		fmt.Printf("[warn] could not load partitions for '%s': %v\n", t.TopicName, err)
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for partID, msgs := range partitionData {
		if partID >= len(t.Partitions) {
			continue
		}
		for _, msg := range msgs {
			t.Partitions[partID].Append(msg)
		}
	}
	fmt.Printf("[info] restored topic '%s' (%d partitions)\n", t.TopicName, len(partitionData))
}
