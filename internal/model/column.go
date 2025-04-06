package model

type Column struct {
	Type     string
	Name     string
	Nullable bool
	isKey    bool
}
