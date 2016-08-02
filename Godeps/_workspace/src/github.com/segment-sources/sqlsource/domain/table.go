package domain

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type TableState struct {
	ScannedRows  uint64      `json:"scanned_rows,omitempty"`
	MarkerColumn string      `json:"marker_column,omitempty"`
	LastMarker   interface{} `json:"last_marker,omitempty"`
}

type Table struct {
	SchemaName  string     `json:"-"`
	TableName   string     `json:"-"`
	PrimaryKeys []string   `json:"primary_keys"`
	Columns     []string   `json:"columns"`
	State       TableState `json:"-"`
}

func (t *Table) IncrScanned() {
	atomic.AddUint64(&t.State.ScannedRows, 1)
}

func (t *Table) ColumnToSQL() string {
	c := []string{}
	for _, column := range t.Columns {
		c = append(c, fmt.Sprintf("%q", column))
	}

	return strings.Join(c, ", ")
}
