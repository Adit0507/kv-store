package store

import (
	"fmt"
	"kvstore/bloomfilter"
	"os"
	"sort"
	"strings"
)

// sorted string table on disk
type SSTable struct {
	ID       int
	Filter   *bloomfilter.BloomFilter
	FilePath string
	KeyCount int
	MinKey   string //smallest key
	MaxKey   string //largest key
}

func NewSSTable(id int, data map[string]string, directory string) (*SSTable, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot create SSTable with empty data")
	}

	filter := bloomfilter.OptimalBloomFilter(len(data), 0.01)

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
		filter.Add(k) // addin each key to the Bloom filter
	}
	sort.Strings(keys)

	minKey := keys[0]
	maxKey := keys[len(keys)-1]

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// creatin ssTable file
	filePath := fmt.Sprintf("%s/sstable_%d.dat", directory, id)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	for _, key := range keys {
		value := data[key]

		_, err := fmt.Fprintf(file, "%s:%s\n", key, value)
		if err != nil {
			return nil, err
		}
	}

	return &SSTable{
		ID:       id,
		Filter:   filter,
		FilePath: filePath,
		KeyCount: len(data),
		MinKey:   minKey,
		MaxKey:   maxKey,
	}, nil
}

// retrieves value for key from SSTable
func (sst *SSTable) Get(key string) (string, bool, error) {
	if !sst.Filter.Contains(key) {
		return "", false, nil
	}

	file, err := os.Open(sst.FilePath)
	if err != nil {
		return "", false, err
	}
	defer file.Close()

	data, err := os.ReadFile(sst.FilePath)
	if err != nil {
		return "", false, err
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] == key {
			return parts[1], true, nil
		}
	}


	return "", false, nil
}
