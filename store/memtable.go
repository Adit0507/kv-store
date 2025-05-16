package store

type MemTable struct { // in-memory portion of kv store
	data        map[string]string
	maxSize     int
	currentSize int
}

func NewMemTable(maxSize int) *MemTable {
	return &MemTable{
		data: make(map[string]string),
		maxSize: maxSize,
		currentSize: 0,
	}
}

func (mt *MemTable) Put(key, value string) {	//adds or updates a kv pair in memtable
	if oldVal, exists := mt.data[key]; exists {
		mt.currentSize -= len(key) + len(oldVal)	//if key exists, subtract its size first
	}

	mt.data[key] = value
	mt.currentSize += len(key) + len(value)
}

// retrieves value for key from memtable
func (mt *MemTable) Get(key string) (string, bool) {
	val, exists := mt.data[key]

	return val, exists
}

func (mt *MemTable) IsFull() bool {
	return mt.currentSize >= mt.maxSize
}

func (mt *MemTable) GetData() map[string] string {
	dataCopy := make(map[string] string, len(mt.data))

	for k, v := range mt.data {
		dataCopy[k] = v
	}

	return dataCopy
}

func (mt *MemTable) Clear() {
	mt.data = make(map[string]string)
	mt.currentSize = 0
}