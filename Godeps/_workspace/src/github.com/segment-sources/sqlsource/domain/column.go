package domain

type Column struct {
	Schema       string
	Table        string
	Name         string
	IsPrimaryKey bool
}
