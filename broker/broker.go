package broker

import (
	"fmt"
	"sync"
	"time"
)

type Broker struct {
	Topics map[string]*Topic
	mu     sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		Topics: make(map[string]*Topic),
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
		SaveMessages(topicName, []Message{})
	}
	msg, _ := LoadMessage(topicName)

	topics := &Topic{
		TopicName: topicName,
		Messages:  msg,
	}

	SaveMessages(topicName, topic.Messages)
	b.Topics[topicName] = topic
	fmt.Printf("Topic Created: %s.json\n", topicName)
	return topics
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

// consume calls reforms the new file which is the problem

func (b *Broker) Consume(topicName string) []Message {
	topic := b.GetOrCreateTopic(topicName)
	topic.mu.Lock()
	defer topic.mu.Unlock()
	return topic.Messages
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
