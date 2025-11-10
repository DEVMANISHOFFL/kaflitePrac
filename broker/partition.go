package broker

import "sync"

type Partition struct {
	ID       int
	Messages []Message
	mu       sync.Mutex
}

func (p *Partition) Append(msg Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Messages = append(p.Messages, msg)
}

func (p *Partition) GetMessages() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Message(nil), p.Messages...) 
}

func (p *Partition) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.Messages)
}
