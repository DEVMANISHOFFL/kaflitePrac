package broker

import (
	"fmt"
	"path/filepath"
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
	files, err := filepath.Glob("storage/*.json")
	if err != nil {
		fmt.Println("[error] failed to read topics folder:", err)
		return
	}

	for _, file := range files {
		topicName := filepath.Base(file[:len(file)-5])
		messages, err := LoadMessage(topicName)
		if err != nil {
			fmt.Printf("[warn] could not load topic '%s': %v\n", topicName, err)
			continue
		}
		b.Topics[topicName] = &Topic{
			TopicName: topicName,
			Messages:  messages,
		}
		fmt.Printf("[info] loaded topics: %s (%d messages)\n", topicName, len(messages))
	}
}

func (b *Broker) GetOrCreateTopic(topicName string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, exists := b.Topics[topicName]
	if !exists {
		fmt.Printf("[info] Auto-creating topic '%s'\n", topicName)
		topic = &Topic{TopicName: topicName, Messages: []Message{}}
		b.Topics[topicName] = topic
		SaveMessages(topicName, topic.Messages)
	} else {
		message, err := LoadMessage(topicName)
		if err == nil {
			topic.Messages = message
		} else {
			fmt.Printf("[warn] Could not load messages for topic '%s': %v\n", topicName, err)
		}
	}

	fmt.Printf("[ok] Topic is ready: %s.json\n", topicName)
	return topic
}

func (b *Broker) GetOrCreateGroup(name string) *ConsumerGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	if g, ok := b.Groups[name]; ok {
		return g
	}

	g := NewConsumerGroup(name)
	b.Groups[name] = g
	fmt.Printf("[info] Created consumer group: %s\n", name)
	return g

}

func (b *Broker) Publish(topicName, msg string) Message {
	topic := b.GetOrCreateTopic(topicName)
	topic.mu.Lock()
	defer topic.mu.Unlock()

	message := Message{
		ID:        len(topic.Messages) + 1,
		Value:     msg,
		TimeStamp: time.Now(),
	}
	topic.Messages = append(topic.Messages, message)
	SaveMessages(topic.TopicName, topic.Messages)
	return message
}

func (b *Broker) Consume(topicName string) []Message {
	topic := b.GetOrCreateTopic(topicName)
	topic.mu.Lock()
	defer topic.mu.Unlock()
	return topic.Messages
	// msg, _ := LoadMessage(topicName)
	// return msg
}

func (b *Broker) ListTopics() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	topics := []string{}
	for name := range b.Topics {
		topics = append(topics, name)
	}
	return topics
}
