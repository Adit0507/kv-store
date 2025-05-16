package store

import (
	"container/list"
	"fmt"
)

// kv store with Memtable and SSTables
type KeyValueStore struct {
	memTable      *MemTable
	sstables      *list.List
	dataDir       string
	nextSSTableID int
}

func NewKeyValueStore(memTableSize int, dataDir string) *KeyValueStore {
	return &KeyValueStore{
		memTable:      NewMemTable(memTableSize),
		sstables:      list.New(),
		dataDir:       dataDir,
		nextSSTableID: 1,
	}
}

// add or update kv pair in store
func (kv *KeyValueStore) Put(key, value string) error {
	kv.memTable.Put(key, value) //addin to memtable

	if kv.memTable.IsFull() {
		if err := kv.flushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

// converts the Memtable to SSTable and saves it to disk
func (kv *KeyValueStore) flushMemTable() error {
	data := kv.memTable.GetData()

	// creatin new sstable
	sstable, err := NewSSTable(kv.nextSSTableID, data, kv.dataDir)
	if err != nil {
		return err
	}

	// add to list of sstables
	kv.sstables.PushFront(sstable)
	kv.nextSSTableID++

	// clear memetable
	kv.memTable.Clear()

	fmt.Printf("Flushed MemTable to SSTable %d with %d keys\n", sstable.ID, sstable.KeyCount)

	return nil
}

func (kv *KeyValueStore) Get(key string) (string, bool, error) {
	if val, found := kv.memTable.Get(key); found {
		fmt.Printf("Found key '%s' in MemTable\n", key)
		return val, true, nil
	}

	checkCount := 0
	skipCount := 0

	for e := kv.sstables.Front(); e != nil; e = e.Next() {
		sstable := e.Value.(*SSTable)

		if key < sstable.MinKey || key > sstable.MaxKey {
			fmt.Printf("Skipping SSTable %d: key '%s' out of range [%s, %s]\n",
				sstable.ID, key, sstable.MinKey, sstable.MaxKey)
			skipCount++
			continue
		}

		// checking bloom filter first
		if !sstable.Filter.Contains(key) {
			fmt.Printf("Bloom filter indicates key '%s' is NOT in SSTable %d\n", key, sstable.ID)
			skipCount++
			continue
		}

		fmt.Printf("Checking SSTable %d for key '%s' \n", sstable.ID, key)
		checkCount++

		value, found, err := sstable.Get(key)
		if err != nil {
			return "", false, err
		}

		if found {
			fmt.Printf("Found key '%s' in SSTable %d\n", key, sstable.ID)
			return value, true, nil
		}

		fmt.Printf("False positive! Key '%s' not in SSTable %d despite Bloom filter indication\n", key, sstable.ID)
	}

	fmt.Printf("Key '%s' not found. Checked %d SSTable(s), skipped %d SSTable(s)\n", key, checkCount, skipCount)

	return "", false, nil
}

func (kv *KeyValueStore) PrintStats() {
	fmt.Println("\n--- KeyValueStore Stats ---")
	fmt.Printf("MemTable: %d entries, %d/%d bytes used\n",
		len(kv.memTable.data), kv.memTable.currentSize, kv.memTable.maxSize)

	sstableCount := kv.sstables.Len()
	totalKeys := 0

	for e := kv.sstables.Front(); e != nil; e = e.Next() {
		sstable := e.Value.(*SSTable)
		totalKeys += sstable.KeyCount
	}

	fmt.Printf("SSTables: %d tables, %d total keys\n", sstableCount, totalKeys)
	fmt.Println("--------------------------\n")
}
