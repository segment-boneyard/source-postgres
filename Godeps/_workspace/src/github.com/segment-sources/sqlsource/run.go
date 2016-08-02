package sqlsource

import (
	"io"
	"os"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/asaskevich/govalidator"
	"github.com/segment-sources/sqlsource/domain"
	"github.com/segment-sources/sqlsource/driver"
	"github.com/segmentio/objects-go"
	"github.com/tj/docopt"
	"github.com/tj/go-sync/semaphore"
)

const (
	Version = "0.0.1-beta"
)

var usage = `
Usage:
  dbsource
    [--debug]
    [--init]
    [--concurrency=<c>]
    [--schema=<schema-path>]
    --write-key=<segment-write-key>
    --hostname=<hostname>
    --port=<port>
    --username=<username>
    --password=<password>
    --database=<database>
    [-- <extra-driver-options>...]
  dbsource -h | --help
  dbsource --version

Options:
    "github.com/segmentio/source-db-lib/internal/domain"
  -h --help                   Show this screen
  --version                   Show version
  --write-key=<key>           Segment source write key
  --concurrency=<c>           Number of concurrent table scans [default: 1]
  --hostname=<hostname>       Database instance hostname
  --port=<port>               Database instance port number
  --username=<username>       Database instance username
  --password=<password>       Database instance password
  --database=<database>       Database instance name
  --schema=<schema-path>	  The path to the schema json file [default: schema.json]

`

func Run(d driver.Driver) {
	app := &driver.Base{d}

	m, err := docopt.Parse(usage, nil, true, Version, false)
	if err != nil {
		logrus.Error(err)
		return
	}

	segmentClient := objects.New(m["--write-key"].(string))

	setWrapper := func(o *objects.Object) {
		if err := segmentClient.Set(o); err != nil {
			logrus.WithFields(logrus.Fields{"id": o.ID, "collection": o.Collection, "properties": o.Properties}).Warn(err)
		}
	}

	config := &domain.Config{
		Init:         m["--init"].(bool),
		Hostname:     m["--hostname"].(string),
		Port:         m["--port"].(string),
		Username:     m["--username"].(string),
		Password:     m["--password"].(string),
		Database:     m["--database"].(string),
		ExtraOptions: m["<extra-driver-options>"].([]string),
	}

	if m["--debug"].(bool) {
		logrus.SetLevel(logrus.DebugLevel)
	}

	concurrency, err := strconv.Atoi(m["--concurrency"].(string))
	if err != nil {
		logrus.Error(err)
		return
	}

	// Validate the configuration
	if _, err := govalidator.ValidateStruct(config); err != nil {
		logrus.Error(err)
		return
	}

	// Open the schema
	schemaFile, err := os.OpenFile(m["--schema"].(string), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logrus.Error(err)
		return
	}
	defer schemaFile.Close()

	if err := app.Driver.Init(config); err != nil {
		logrus.Error(err)
		return
	}

	// Initialize the source
	if config.Init {
		description, err := app.Driver.Describe()
		if err != nil {
			logrus.Error(err)
			return
		}
		if err := description.Save(schemaFile); err != nil {
			logrus.Error(err)
			return
		}

		schemaFile.Sync()
		logrus.Infof("Saved to `%s`", schemaFile.Name())
		return
	}

	description, err := domain.NewDescriptionFromReader(schemaFile)
	if err == io.EOF {
		logrus.Error("Empty schema, did you run `--init`?")
		return
	} else if err != nil {
		logrus.Error(err)
		return
	}

	sem := make(semaphore.Semaphore, concurrency)

	for table := range description.Iter() {
		sem.Acquire()
		go func(table *domain.Table) {
			defer sem.Release()
			logrus.WithFields(logrus.Fields{"table": table.TableName, "schema": table.SchemaName}).Info("Scan started")
			if err := app.ScanTable(table, setWrapper); err != nil {
				logrus.Error(err)
			}
			logrus.WithFields(logrus.Fields{"table": table.TableName, "schema": table.SchemaName}).Info("Scan finished")
		}(table)
	}

	sem.Wait()
	segmentClient.Close()

	// Log status
	for table := range description.Iter() {
		logrus.WithFields(logrus.Fields{"schema": table.SchemaName, "table": table.TableName, "count": table.State.ScannedRows}).Info("Sync Finished")
	}
}
