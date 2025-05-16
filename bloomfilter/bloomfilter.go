package bloomfilter

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bitArray []bool
	size     uint
	numHash  uint
}

// creatin a bloom filter with speciified size nad no. of hash functions
func NewBloomFilter(size uint, numHash uint) *BloomFilter {
	return &BloomFilter{
		bitArray: make([]bool, size),
		size:     size,
		numHash:  numHash,
	}
}

// creates a bloom filter with optimal parameters
func OptimalBloomFilter(expectedElements int, falsePositiveProb float64) *BloomFilter {
	size := uint(math.Ceil(-float64(expectedElements) * math.Log(falsePositiveProb) / math.Pow(math.Log(2), 2))) //bit array size
	numHash := uint(math.Ceil(float64(size) / float64(expectedElements) * math.Log(2)))                          //no. of hash functiions

	return NewBloomFilter(size, numHash)
}

// generatin different hash values for string
func (bf *BloomFilter) hash(data string, seed uint) uint {
	h := fnv.New64a()
	h.Write([]byte(data))
	h.Write([]byte{byte(seed)})

	return uint(h.Sum64() % uint64(bf.size))
}

func (bf *BloomFilter) Add(element string) {
	for i := uint(0); i < bf.numHash; i++ {
		position := bf.hash(element, i)
		bf.bitArray[position] = true
	}
}

func (bf *BloomFilter) Contains(element string) bool {
	for i := uint(0); i < bf.numHash; i++ {
		position := bf.hash(element, i)

		if !bf.bitArray[position] {
			return false
		}
	}

	return true
}
