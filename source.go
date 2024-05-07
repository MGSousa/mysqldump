package mysqldump

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

type (
	sourceOption struct {
		dryRun      bool
		mergeInsert int
		debug       bool
	}

	SourceOption func(*sourceOption)
)

func WithDryRun() SourceOption {
	return func(o *sourceOption) {
		o.dryRun = true
	}
}

func WithMergeInsert(size int) SourceOption {
	return func(o *sourceOption) {
		o.mergeInsert = size
	}
}

func WithDebug() SourceOption {
	return func(o *sourceOption) {
		o.debug = true
	}
}

type dbWrapper struct {
	DB     *sql.DB
	debug  bool
	dryRun bool
}

func newDBWrapper(db *sql.DB, dryRun, debug bool) *dbWrapper {
	return &dbWrapper{
		DB:     db,
		dryRun: dryRun,
		debug:  debug,
	}
}

// Exec Execute SQL statement
func (db *dbWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db.debug {
		log.Printf("[DEBUG] [query]\n%s\n", query)
	}

	if db.dryRun {
		return nil, nil
	}
	return db.DB.Exec(query, args...)
}

// Source Import a writer source (file, stdOut, etc.) to a MySQL/MariaDB Database
// nolint: gocyclo
func Source(dsn string, reader io.Reader, opts ...SourceOption) error {
	var (
		err error
		db  *sql.DB
		o   sourceOption
	)

	start := time.Now()
	log.Printf("[info] [source] start at %s\n", start.Format(DEFAULT_LOG_TIMESTAMP))

	// calculate dump Execution Time
	defer func() {
		end := time.Now()
		log.Printf("[info] [source] end at %s, cost %s\n", end.Format(DEFAULT_LOG_TIMESTAMP), end.Sub(start))
	}()

	// iterate over existing plugins (With...)
	// and execute it
	for _, opt := range opts {
		opt(&o)
	}

	dbName, err := GetDBNameFromDSN(dsn)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	// Open database
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}
	defer db.Close()

	// DB Wrapper
	dbWrapper := newDBWrapper(db, o.dryRun, o.debug)

	// Use database
	_, err = dbWrapper.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}
	db.SetConnMaxLifetime(3600)

	// set autocommit
	_, err = dbWrapper.Exec("SET autocommit=0;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	r := bufio.NewReader(reader)
	for {
		line, err := r.ReadString(';')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("[error] %v\n", err)
			return err
		}

		ssql := string(line)
		ssql = trim(ssql)

		if o.mergeInsert > 1 && strings.HasPrefix(ssql, "INSERT INTO") {
			var insertSQLs []string
			insertSQLs = append(insertSQLs, ssql)
			for i := 0; i < o.mergeInsert-1; i++ {
				line, err := r.ReadString(';')
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Printf("[error] %v\n", err)
					return err
				}

				ssql2 := string(line)
				ssql2 = trim(ssql2)
				if strings.HasPrefix(ssql2, "INSERT INTO") {
					insertSQLs = append(insertSQLs, ssql2)
					continue
				}

				break
			}
			// INSERT
			ssql, err = mergeInsert(insertSQLs)
			if err != nil {
				log.Printf("[error] [mergeInsert] %v\n", err)
				return err
			}
		}

		_, err = dbWrapper.Exec(ssql)
		if err != nil {
			log.Printf("[error] %v\n", err)
			return err
		}
	}

	_, err = dbWrapper.Exec("COMMIT;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	_, err = dbWrapper.Exec("SET autocommit=1;")
	if err != nil {
		log.Printf("[error] %v\n", err)
		return err
	}

	return nil
}

/*
Convert:
  - INSERT INTO `test` VALUES (1, 'a');
  - INSERT INTO `test` VALUES (2, 'b');

Into this:
  - INSERT INTO `test` VALUES (1, 'a'), (2, 'b');
*/
func mergeInsert(insertSQLs []string) (string, error) {
	if len(insertSQLs) == 0 {
		return "", errors.New("no input provided")
	}
	builder := strings.Builder{}
	sql1 := insertSQLs[0]
	sql1 = strings.TrimSuffix(sql1, ";")
	builder.WriteString(sql1)
	for i, insertSQL := range insertSQLs[1:] {
		if i < len(insertSQLs)-1 {
			builder.WriteString(",")
		}

		valuesIdx := strings.Index(insertSQL, "VALUES")
		if valuesIdx == -1 {
			return "", errors.New("invalid SQL: missing VALUES keyword")
		}
		sqln := insertSQL[valuesIdx:]
		sqln = strings.TrimPrefix(sqln, "VALUES")
		sqln = strings.TrimSuffix(sqln, ";")
		builder.WriteString(sqln)
	}

	builder.WriteString(";")
	return builder.String(), nil
}

func trim(s string) string {
	s = strings.TrimLeft(s, "\n")
	s = strings.TrimSpace(s)
	return s
}
