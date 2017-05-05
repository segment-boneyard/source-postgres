package main

import "github.com/segment-sources/sqlsource"
import "github.com/segment-sources/postgres"

func main() {
	sqlsource.Run(&postgres.Postgres{})
}
