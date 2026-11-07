package statistics

type Statistics struct {
	EventType string `db:"event_type" json:"event_type"`
	Count     int    `db:"count" json:"count"`
}
