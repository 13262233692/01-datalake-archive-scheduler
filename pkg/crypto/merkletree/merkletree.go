package merkletree

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"sort"
	"strconv"
	"time"
)

type HashFunc func(data []byte) []byte

type MerkleTree struct {
	root     *MerkleNode
	leaves   []*MerkleNode
	rootHash string
	hashFunc HashFunc
}

type MerkleNode struct {
	Hash  string
	Left  *MerkleNode
	Right *MerkleNode
}

func DefaultHashFunc(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func NewMerkleTree(hashes []string, hashFunc ...HashFunc) *MerkleTree {
	hf := DefaultHashFunc
	if len(hashFunc) > 0 {
		hf = hashFunc[0]
	}

	if len(hashes) == 0 {
		return &MerkleTree{
			rootHash: emptyHash(hf),
			hashFunc: hf,
		}
	}

	leaves := make([]*MerkleNode, len(hashes))
	for i, h := range hashes {
		leaves[i] = &MerkleNode{Hash: h}
	}

	root := buildTree(leaves, hf)

	return &MerkleTree{
		root:     root,
		leaves:   leaves,
		rootHash: root.Hash,
		hashFunc: hf,
	}
}

func buildTree(nodes []*MerkleNode, hashFunc HashFunc) *MerkleNode {
	if len(nodes) == 1 {
		return nodes[0]
	}

	var upperLevel []*MerkleNode

	for i := 0; i < len(nodes); i += 2 {
		left := nodes[i]
		var right *MerkleNode

		if i+1 < len(nodes) {
			right = nodes[i+1]
		} else {
			right = &MerkleNode{Hash: left.Hash}
		}

		combined := left.Hash + right.Hash
		hashBytes := hashFunc([]byte(combined))
		parent := &MerkleNode{
			Hash:  hex.EncodeToString(hashBytes),
			Left:  left,
			Right: right,
		}
		upperLevel = append(upperLevel, parent)
	}

	return buildTree(upperLevel, hashFunc)
}

func emptyHash(hashFunc HashFunc) string {
	return hex.EncodeToString(hashFunc([]byte("")))
}

func (t *MerkleTree) GetRootHash() string {
	return t.rootHash
}

func (t *MerkleTree) GetLeafCount() int {
	return len(t.leaves)
}

func (t *MerkleTree) GetLeaves() []*MerkleNode {
	return t.leaves
}

func RecordHash(recordID int64, fields map[string]interface{}, hashFunc ...HashFunc) string {
	hf := DefaultHashFunc
	if len(hashFunc) > 0 {
		hf = hashFunc[0]
	}

	var keys []string
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var combined string
	combined += fmt.Sprintf("id:%d|", recordID)

	for _, k := range keys {
		v := fields[k]
		combined += fmt.Sprintf("%s:%v|", k, serializeValue(v))
	}

	return hex.EncodeToString(hf([]byte(combined)))
}

func serializeValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func NewStreamingHash(hashFunc ...HashFunc) *StreamingHash {
	hf := DefaultHashFunc
	if len(hashFunc) > 0 {
		hf = hashFunc[0]
	}

	return &StreamingHash{
		hashFunc:  hf,
		leafHashes: make([]string, 0),
		recordCount: 0,
	}
}

type StreamingHash struct {
	hashFunc    HashFunc
	leafHashes []string
	recordCount int
	mu          chan struct{}
}

func (sh *StreamingHash) Add(recordID int64, fields map[string]interface{}) {
	h := RecordHash(recordID, fields, sh.hashFunc)
	sh.leafHashes = append(sh.leafHashes, h)
	sh.recordCount++
}

func (sh *StreamingHash) AddHash(hash string) {
	sh.leafHashes = append(sh.leafHashes, hash)
	sh.recordCount++
}

func (sh *StreamingHash) BuildTree() *MerkleTree {
	return NewMerkleTree(sh.leafHashes, sh.hashFunc)
}

func (sh *StreamingHash) RecordCount() int {
	return sh.recordCount
}

func (sh *StreamingHash) Reset() {
	sh.leafHashes = sh.leafHashes[:0]
	sh.recordCount = 0
}

func CoreFieldsMap(recordID int64, amount float64, createdAt, updatedAt time.Time, orderID string) map[string]interface{} {
	return map[string]interface{}{
		"amount":     amount,
		"created_at": createdAt,
		"updated_at": updatedAt,
		"order_id":   orderID,
	}
}

func VerifyTree(hashes []string, expectedRoot string) bool {
	tree := NewMerkleTree(hashes)
	return tree.GetRootHash() == expectedRoot
}

func BatchMerkleRoot(hashes []string) string {
	tree := NewMerkleTree(hashes)
	return tree.GetRootHash()
}

type HashAccumulator struct {
	hasher hash.Hash
	count  int64
}

func NewHashAccumulator() *HashAccumulator {
	return &HashAccumulator{
		hasher: sha256.New(),
		count:  0,
	}
}

func (ha *HashAccumulator) Write(data []byte) {
	ha.hasher.Write(data)
	ha.count++
}

func (ha *HashAccumulator) Sum() string {
	return hex.EncodeToString(ha.hasher.Sum(nil))
}

func (ha *HashAccumulator) Count() int64 {
	return ha.count
}

func (ha *HashAccumulator) Reset() {
	ha.hasher.Reset()
	ha.count = 0
}
