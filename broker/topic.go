package broker

import (
	"sync"
	"time"
)

type Topic struct {
	TopicName string
	Messages  []Message
	mu        sync.Mutex
}

type Message struct {
	ID        int    `json:"id"`
	Value     string `json:"value"`
	TimeStamp time.Time
}

func NewTopic(name string) *Topic {
	return &Topic{
		TopicName: name,
		Messages:  []Message{},
	}
}

func (t *Topic) AddMessage(msg string) {
	m := Message{
		Value:     msg,
		TimeStamp: time.Now(),
	}
	t.Messages = append(t.Messages, m)

}
