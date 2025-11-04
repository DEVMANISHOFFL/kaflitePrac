package broker

type Topic struct {
	Name    string
	Message []string
}

func NewTopic(name string) *Topic {
	return &Topic{
		Name:    name,
		Message: []string{},
	}
}

func (t *Topic) AddMessage(msg string) {
	t.Message = append(t.Message, msg)
}


