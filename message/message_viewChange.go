package message

var (
	ViewChangePropose MessageType = "ViewChangePropose"
	NewChange         MessageType = "NewChange"
)

type ViewChangeMsg struct {
	ViewVersion uint64 `json:",omitempty"` // GLAS: next monotonic view version.
	CurView     int
	NextView    int
	SeqID       int
	FromNode    uint64
}

type NewViewMsg struct {
	ViewVersion uint64 `json:",omitempty"` // GLAS: next monotonic view version.
	CurView     int
	NextView    int
	NewSeqID    int
	FromNode    uint64
}
