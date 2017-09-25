package domain

import (
	"encoding/json"
	"io"
	"sync"
)

type Description struct {
	m       sync.Mutex
	schemas map[string]map[string]*Table
}

func NewDescription() *Description {
	return &Description{
		schemas: make(map[string]map[string]*Table),
	}
}

func NewDescriptionFromReader(r io.Reader) (*Description, error) {
	d := NewDescription()
	if err := json.NewDecoder(r).Decode(&d.schemas); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Description) SchemaCount() int {
	return len(d.schemas)
}

func (d *Description) AddColumn(c *Column) {
	d.m.Lock()
	defer d.m.Unlock()

	if _, ok := d.schemas[c.Schema]; !ok {
		d.schemas[c.Schema] = map[string]*Table{}
	}

	var table *Table

	if tValue, ok := d.schemas[c.Schema][c.Table]; !ok {
		newTable := &Table{SchemaName: c.Schema, TableName: c.Table}
		d.schemas[c.Schema][c.Table] = newTable
		table = newTable
	} else {
		table = tValue
	}

	if c.IsPrimaryKey {
		table.PrimaryKeys = append(table.PrimaryKeys, c.Name)
	}

	table.Columns = append(table.Columns, c.Name)
}

func (d *Description) Save(w io.Writer) error {
	b, err := json.MarshalIndent(d.schemas, "", "\t")
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (d *Description) Iter() <-chan *Table {
	out := make(chan *Table)

	go func() {
		for schemaName := range d.schemas {
			schemaValue := d.schemas[schemaName]
			for tableName := range schemaValue {
				t := schemaValue[tableName]
				t.TableName = tableName
				t.SchemaName = schemaName
				out <- t
			}
		}
		close(out)
	}()

	return out
}
