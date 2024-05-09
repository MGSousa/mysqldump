package mysqldump

import (
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const DEFAULT_LOG_TIMESTAMP = "2006-01-02 15:04:05"

var replacer *strings.Replacer

func parseDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func joinS(s []string, delimiter string) string {
	return strings.Join(s, delimiter)
}

func splitS(s, delimiter string) []string {
	return strings.Split(s, delimiter)
}

func sanitize(input string) string {
	if replacer == nil {
		replacer = strings.NewReplacer(
			"\x00", "\\0",
			"'", "\\'",
			"\"", "\\\"",
			"\b", "\\b",
			"\n", "\\n",
			"\r", "\\r",
			"\x1A", "\\Z", // ASCII 26 == x1A
		)
	}
	return replacer.Replace(input)
}

func untilNow(start time.Time) string {
	return time.Since(start).String()
}

func trim(s string) string {
	return strings.TrimSpace(strings.TrimLeft(s, "\n"))
}
