package proto

type (
	KeyType string
	ValueType interface{}
	KeyValueMap map[KeyType]ValueType
)

// NodeInfoStore is a best-effort key value store
// that stores information associated with nodes.
// For each node, it provides the associated information
// in the form of a key-value store.
type NodeInfoStore interface {
	// Adds a new node to the list of nodes.
	// Returns an eror if the node already exists
	AddNode(ip string) error
	// Removes a previously added node from the list of nodes
	RemoveNode(ip string) error
	// Returns a list of all available nodes
	GetNodes() []string

	// Adds/updates a given key-value pair for this node
	Update(key KeyType, value ValueType) error
	// Removes the given key from this stores key value map.
	// No-op if the key is missing.
	Remove(key KeyType) error

	GetValuesForNode(ip string) KeyValueMap
}
