package domain

type Config struct {
	Init         bool
	Driver       string
	Hostname     string `valid:"host"`
	Port         string `valid:"port"`
	Username     string
	Password     string
	Database     string
	ExtraOptions []string
}
