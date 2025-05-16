package main

import (
	"fmt"
	"kvstore/store"
)

func main() {
	kv := store.NewKeyValueStore(200, "data")

	fmt.Println("Adding initial data....")
	for i := 1; i <= 50; i++ {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value-%d", i*10)
		kv.Put(key, value)
	}

	kv.PrintStats()

	keysToFind := []string{"key01", "key25", "key50", "nonexistent"}
	fmt.Println("Looking up keys...")
	for _, key := range keysToFind {
		val, found, err := kv.Get(key)
		if err != nil {
			fmt.Printf("Error looking up key '%s': %v\n", key, err)
		} else if found {
			fmt.Printf("Final result: '%s' = '%s'\n", key, val)
		} else {
			fmt.Printf("Final result: '%s' not found\n", key)
		}

		fmt.Println()
	}

	// more data to force a flush
	fmt.Println("adding more data to force flush")
	for i := 51; i <= 100; i++ {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value-%d", i*10)
		kv.Put(key, value)
	}

	kv.PrintStats()

	keysToFind = []string{"key75", "key20", "key99"}
	fmt.Println("Looking up more keys...")

	for _, key := range keysToFind {
		val, found, err := kv.Get(key)
		if err != nil {
			fmt.Printf("Error looking up key '%s': %v\n", key, err)
		} else if found {
			fmt.Printf("Final result: '%s' = '%s'\n", key, val)
		} else {
			fmt.Printf("Final result: '%s' not found\n", key)
		}

		fmt.Println()
	}

}
