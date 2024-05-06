package mysqldump

import (
	"fmt"
	"strings"
)

func splitS(s string, delimiter string) []string {
	return strings.Split(s, delimiter)
}

// GetDBNameFromDSN get Database name from DSN
func GetDBNameFromDSN(dsn string) (string, error) {
	s := splitS(dsn, "/")
	if len(s) == 2 {
		return splitS(s[1], "?")[0], nil
	}

	return "", fmt.Errorf("dsn error: %s", dsn)
}

// GetDBHostFromDSN get Hostname from DSN
func GetDBHostFromDSN(dsn string) (string, error) {
	s := splitS(dsn, "@")
	if len(s) == 2 {
		h := splitS(s[1], "/")[0]
		if strings.HasPrefix(h, "tcp(") {
			h, _ = strings.CutPrefix(h, "tcp(")
			h, _ = strings.CutSuffix(h, ")")
		}
		if strings.TrimSpace(h) == "" {
			return "127.0.0.1", nil
		}
		return h, nil
	}
	return "", fmt.Errorf("dsn error: %s", dsn)
}
