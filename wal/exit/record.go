package exit

type Record struct {
	ID      string
	Type    string
	Data    []byte
	Ack     bool
	Created int64
}
