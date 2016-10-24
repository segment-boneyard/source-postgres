package main

import (
	"github.com/Lilibuth12/sqlsource/driver"
	"github.com/segment-sources/sqlsource"
)

func main() {
	sqlsource.Run(&driver.Postgres{})
}
