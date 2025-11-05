package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const storageDir = "storage"

func SaveMessages(topicName string, messages []Message) error {
	if err := os.MkdirAll(storageDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create storage dir: %v", err)
	}

	filePath := filepath.Join(storageDir, topicName+".json")

	data, err := json.MarshalIndent(messages, "", " ")
	if err != nil {
		return fmt.Errorf("failed to marshal messages: %v", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}
	return nil
}

func LoadMessage(topicName string) ([]Message, error) {
	filePath := filepath.Join(storageDir, topicName+".json")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return []Message{}, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	var messages []Message
	if err := json.Unmarshal(data, &messages); err != nil {
		return nil, fmt.Errorf("failed to unmarshal messages: %v", err)
	}
	return messages, nil
}
