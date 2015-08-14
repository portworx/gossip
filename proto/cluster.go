package proto

type (
	KeyType     string
	ValueType   interface{}
	KeyValueMap map[KeyType]ValueType
)

func (m KeyValueMap) Update(key KeyType, value ValueType) {
	m[key] = value
}

func (m KeyValueMap) Remove(k KeyType) {
	delete(m, k)
}

// ClusterInfoStore is a best-effort key value store
// that stores information associated with nodes.
// For each node, it provides the associated information
// in the form of a key-value store.
type ClusterInfoStore interface {
	// Adds a new node to the list of nodes.
	// Returns an eror if the node already exists
	AddNode(ip string) error
	// Removes a previously added node from the list of nodes
	RemoveNode(ip string) error
	// Returns a list of all available nodes
	GetNodes() []string

	// Adds/updates a given key-value pair for this node
	Update(key KeyType, value ValueType)
	// Removes the given key from this stores key value map.
	// No-op if the key is missing.
	Remove(key KeyType)

	GetValuesForNode(ip string) KeyValueMap
}
