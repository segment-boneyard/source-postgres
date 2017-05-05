package main

import "github.com/segment-sources/sqlsource"

func main() {
	sqlsource.Run(&Postgres{})
}
