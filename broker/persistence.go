package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const storageDir = "storage"

func SavePartition(topicName string, partitionID int, messages []Message) error {
	dir := filepath.Join(storageDir, topicName)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create storage dir: %v", err)
	}

	filePath := filepath.Join(dir, fmt.Sprintf("partition-%d.json", partitionID))
	data, err := json.MarshalIndent(messages, "", " ")
	if err != nil {
		return fmt.Errorf("failed to marshal messages: %v", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}
	return nil
}

func LoadPartitions(topicName string) (map[int][]Message, error) {
	dir := filepath.Join(storageDir, topicName)
	partitions := make(map[int][]Message)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return partitions, nil 
		}
		return nil, fmt.Errorf("failed to read dir: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var partID int
		fmt.Sscanf(entry.Name(), "partition-%d.json", &partID)

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			fmt.Printf("[warn] failed to read %s: %v\n", entry.Name(), err)
			continue
		}

		var msgs []Message
		if err := json.Unmarshal(data, &msgs); err != nil {
			fmt.Printf("[warn] failed to parse %s: %v\n", entry.Name(), err)
			continue
		}

		partitions[partID] = msgs
	}
	return partitions, nil
}
