package message

// PendingExecutionWindow preserves the Gob descriptor of archived migration
// proposals. The formal protocol rejects a non-nil window in incoming messages.
type PendingExecutionWindow struct {
	Epoch   uint64
	Sources []string
}
