package broker

type Broker struct {
	Topics map[string]*Topic
}

func NewBroker() *Broker {
	return &Broker{
		Topics: make(map[string]*Topic),
	}
}

func (b *Broker) CreateTopic(name string) {
	if _, exists := b.Topics[name]; !exists {
		b.Topics[name] = NewTopic(name)
	}
}

func (b *Broker) Publish(topicName, msg string) {
	if topic, exists := b.Topics[topicName]; exists {
		topic.AddMessage(msg)
	}
}

func (b *Broker) Consume(topicName string) []string {
	if topic, exists := b.Topics[topicName]; exists {
		return topic.Message
	}
	return []string{}
}
