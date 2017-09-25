package postgres

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segment-sources/sqlsource/driver"
)

const chunkSize = 1000000

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

func (p *Postgres) Scan(t *domain.Table, lastPkValues []interface{}) (driver.SqlRows, error) {
	// in most cases whereClause will simply look like "id" > 114, but since the source supports compound PKs
	// we must be able to include all PK columns in the query. For example, for a table with 3-column PK:
	//	a | b | c
	//	---+---+---
	//	1 | 1 | 1
	//	1 | 1 | 2
	//	1 | 2 | 1
	//	1 | 2 | 2
	//	2 | 1 | 1
	//
	// whereClause selecting records after (1, 1, 1) should look like:
	// a > 1 OR a = 1 AND b > 1 OR a = 1 AND b = 1 AND c > 1
	whereClause := "true"
	if len(lastPkValues) > 0 {
		// {"a > 1", "a = 1 AND b > 1", "a = 1 AND b = 1 AND c > 1"}
		whereOrList := []string{}

		for i, pk := range t.PrimaryKeys {
			// {"a = 1", "b = 1", "c > 1"}
			choiceAndList := []string{}
			for j := 0; j < i; j++ {
				choiceAndList = append(choiceAndList, fmt.Sprintf(`"%s" = $%d`, t.PrimaryKeys[j], j+1))
			}
			choiceAndList = append(choiceAndList, fmt.Sprintf(`"%s" > $%d`, pk, i+1))
			whereOrList = append(whereOrList, strings.Join(choiceAndList, " AND "))
		}
		whereClause = strings.Join(whereOrList, " OR ")
	}

	orderByList := make([]string, 0, len(t.PrimaryKeys))
	for _, column := range t.PrimaryKeys {
		orderByList = append(orderByList, fmt.Sprintf(`"%s"`, column))
	}
	orderByClause := strings.Join(orderByList, ", ")

	query := fmt.Sprintf("SELECT %s FROM %q.%q WHERE %s ORDER BY %s LIMIT %d", t.ColumnToSQL(), t.SchemaName,
		t.TableName, whereClause, orderByClause, chunkSize)

	logger := logrus.WithFields(logrus.Fields{
		"query": query,
		"args": lastPkValues,
	})
	logger.Debugf("Executing query")
	return p.Connection.Queryx(query, lastPkValues...)
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
