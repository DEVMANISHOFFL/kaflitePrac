package broker

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Broker struct {
	Topics map[string]*Topic
	Groups map[string]*ConsumerGroup
	mu     sync.Mutex
}

func NewBroker() *Broker {
	b := &Broker{
		Topics: make(map[string]*Topic),
		Groups: make(map[string]*ConsumerGroup),
	}
	b.LoadAllTopics()
	return b
}

func (b *Broker) LoadAllTopics() {
	entries, err := os.ReadDir(storageDir)
	if err != nil {
		fmt.Println("[error] failed to read storage dir:", err)
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		if topicName == "groups" {
			continue
		}

		topic := NewTopic(topicName, 3)
		topic.RestoreFromDisk()
		b.Topics[topicName] = topic
		fmt.Printf("[info] loaded topic '%s' from partitions\n", topicName)
	}

}

func (b *Broker) GetOrCreateTopic(topicName string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, ok := b.Topics[topicName]; ok {
		return t
	}

	t := NewTopic(topicName, 3)
	b.Topics[topicName] = t
	fmt.Printf("[info] created new topic '%s' with 3 partitions\n", topicName)
	return t
}

func (b *Broker) GetOrCreateGroup(name string) *ConsumerGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	if g, ok := b.Groups[name]; ok {
		return g
	}
	g := NewConsumerGroup(name)
	b.Groups[name] = g
	fmt.Printf("[info] created consumer group '%s'\n", name)
	return g
}

func (b *Broker) Publish(topicName, msg string) Message {
	topic := b.GetOrCreateTopic(topicName)
	topic.AddMessage(msg)

	message := Message{
		Value:     msg,
		TimeStamp: time.Now(),
	}
	return message
}

func (b *Broker) Consume(topicName string) []Message {
	topic := b.GetOrCreateTopic(topicName)
	return topic.GetAllMessages()
}

func (b *Broker) ListTopics() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	names := make([]string, 0, len(b.Topics))
	for n := range b.Topics {
		names = append(names, n)
	}
	return names
}
