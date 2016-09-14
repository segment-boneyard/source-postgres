package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/segment-sources/sqlsource/domain"
)

type tableDescriptionRow struct {
	Catalog    string `db:"table_catalog"`
	SchemaName string `db:"table_schema"`
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	IsPrimary  bool   `db:"is_primary_key"`
}

type Postgres struct {
	Connection *sqlx.DB
}

func (p *Postgres) Init(c *domain.Config) error {
	var extraOptions bytes.Buffer
	if len(c.ExtraOptions) > 0 {
		extraOptions.WriteRune('?')
		extraOptions.WriteString(strings.Join(c.ExtraOptions, "&"))
	}

	connectionString := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s%s",
		c.Username, c.Password, c.Hostname, c.Port, c.Database, extraOptions.String(),
	)

	db, err := sqlx.Connect("pgx", connectionString)
	if err != nil {
		return err
	}

	p.Connection = db

	return nil
}

func (p *Postgres) Scan(t *domain.Table) (*sqlx.Rows, error) {
	query := fmt.Sprintf("SELECT %s FROM %q.%q", t.ColumnToSQL(), t.SchemaName, t.TableName)
	logrus.Debugf("Executing query: %v", query)

	return p.Connection.Queryx(query)
}

func (p *Postgres) Transform(row map[string]interface{}) map[string]interface{} {
	return row
}

func (p *Postgres) Describe() (*domain.Description, error) {
	describeQuery := `
    with o_1 as (SELECT
        _s.nspname AS table_schema,
        _t.relname  AS table_name,
        c.conkey AS column_positions
      FROM pg_catalog.pg_constraint c
        LEFT JOIN pg_catalog.pg_class _t ON c.conrelid = _t.oid
        LEFT JOIN pg_catalog.pg_class referenced_table ON c.confrelid = referenced_table.oid
        LEFT JOIN pg_catalog.pg_namespace _s ON _t.relnamespace = _s.oid
        LEFT JOIN pg_catalog.pg_namespace referenced_schema ON referenced_table.relnamespace = referenced_schema.oid
      WHERE c.contype = 'p')

    select c.table_catalog, c.table_schema, c.table_name, c.column_name, CASE WHEN c.ordinal_position = ANY(o_1.column_positions) THEN true ELSE false END as "is_primary_key"
        FROM o_1 INNER JOIN information_schema.columns c
            ON o_1.table_schema = c.table_schema
            AND o_1.table_name = c.table_name;
    `

	res := domain.NewDescription()

	rows, err := p.Connection.Queryx(describeQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		row := &tableDescriptionRow{}
		if err := rows.StructScan(row); err != nil {
			return nil, err
		}
		res.AddColumn(&domain.Column{Name: row.ColumnName, Schema: row.SchemaName, Table: row.TableName, IsPrimaryKey: row.IsPrimary})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
