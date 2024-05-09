package mysqldump

import (
	"compress/flate"
	"io"
)

/*
Plugin functions for dump procedure
*/

// WithDropTable Delete table
func WithDropTable() DumpOption {
	return func(option *dumpOption) {
		option.isDropTable = true
	}
}

// WithData Export table data
func WithData() DumpOption {
	return func(option *dumpOption) {
		option.isData = true
	}
}

// WithAllDatabases Export all databases
func WithAllDatabases() DumpOption {
	return func(option *dumpOption) {
		option.isAllDB = true
	}
}

// WithUseDb Whether to add a specified library statement.
// If there are multiple libraries, this setting is invalid.
func WithUseDb() DumpOption {
	return func(option *dumpOption) {
		option.isUseDb = true
	}
}

// WithDBs Export specified databases, mutually exclusive with WithAllDatabases
// WithAllDatabases has higher priority
func WithDBs(databases ...string) DumpOption {
	return func(option *dumpOption) {
		option.Dbs = databases
	}
}

// WithTables Export specific tables
func WithTables(tables ...string) DumpOption {
	return func(option *dumpOption) {
		option.tables = tables
	}
}

// WithAllTables Export all tables
func WithAllTables() DumpOption {
	return func(option *dumpOption) {
		option.isAllTables = true
	}
}

// WithMultiInsert Export multi-inserts in one command
func WithMultiInsert(num int) DumpOption {
	return func(option *dumpOption) {
		option.perDataNumber = num
	}
}

// WithWriter Export to specified writer (file, stdOut, etc.)
func WithWriter(writer io.Writer) DumpOption {
	return func(option *dumpOption) {
		option.writer = writer
	}
}

// WithLogErrors Whether to output logs
func WithLogErrors() DumpOption {
	return func(option *dumpOption) {
		option.log = true
	}
}

// WithCompression Whether to compress desired file with gzip
func WithCompression(level string) DumpOption {
	return func(option *dumpOption) {
		option.isCompressed = true

		switch level {
		case "BEST", "MAX":
			option.compressionLevel = flate.BestCompression
		case "FAST", "MIN":
			option.compressionLevel = flate.BestSpeed
		default:
			option.compressionLevel = flate.DefaultCompression
		}
	}
}
