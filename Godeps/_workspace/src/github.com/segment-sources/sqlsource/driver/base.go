package driver

import (
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segmentio/go-snakecase"
	"github.com/segmentio/objects-go"
)

type Driver interface {
	Init(*domain.Config) error
	Describe() (*domain.Description, error)
	Scan(t *domain.Table) (*sqlx.Rows, error)
	Transform(row map[string]interface{}) map[string]interface{}
}

type Base struct {
	Driver Driver
}

func (b *Base) ScanTable(t *domain.Table, publisher domain.ObjectPublisher) error {
	rows, err := b.Driver.Scan(t)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		row := map[string]interface{}{}
		if err := rows.MapScan(row); err != nil {
			return err
		}
		log.WithFields(log.Fields{"row": row, "table": t.TableName, "schema": t.SchemaName}).Debugf("Received Row")
		t.IncrScanned()

		row = b.Driver.Transform(row)
		pks := []string{}
		for _, p := range t.PrimaryKeys {
			pks = append(pks, fmt.Sprintf("%v", row[p]))
		}

		publisher(&objects.Object{
			ID:         strings.Join(pks, "_"),
			Collection: snakecase.Snakecase(fmt.Sprintf("%s_%s", t.SchemaName, t.TableName)),
			Properties: row,
		})
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}
