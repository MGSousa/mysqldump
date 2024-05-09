package mysqldump

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/MGSousa/mysqldump/extensions"
	_ "github.com/go-sql-driver/mysql"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

type (
	dumpOption struct {
		Host string
		// Export the specified database, mutually exclusive with WithAllDatabases, WithAllDatabases has higher priority
		Dbs []string
		// Startime
		Startime time.Time
		// Client version
		Version string

		//Export table data
		isData bool
		// Export all databases
		isAllDB bool
		// Export the specified table, mutually exclusive with isAllTables, isAllTables has higher priority
		tables []string
		// Export all tables
		isAllTables bool
		// Whether to delete the table
		isDropTable bool
		// Whether to add a library selection script. When exporting multiple libraries, this setting is enabled by default.
		isUseDb bool
		// Batch insert to improve export efficiency
		perDataNumber int
		// writer defaults to os.Stdout
		writer io.Writer
		// Whether to output debug logs
		log bool
		// Whether to compress the output with gzip
		// only works if the Writer stream is a file
		isCompressed     bool
		compressionLevel int
	}
	triggerStruct struct {
		Trigger   string
		Event     string
		Table     string
		Statement string
		Timing    string
	}
	DumpOption func(*dumpOption)
)

var (
	dpOpt       dumpOption
	allTriggers map[string][]triggerStruct
)

// Dump exports DB contents from MySQL/MariaDB to a writer source (file, stdOut, etc.)
// nolint: gocyclo
func Dump(dsn string, opts ...DumpOption) (err error) {
	if err = dpOpt.dump(dsn, opts...); err != nil {
		return
	}

	if dpOpt.isCompressed {
		if dpOpt.log {
			log.Println("[gzip] [info] gzip compression enabled")
		}

		gz := extensions.NewGzip(dpOpt.compressionLevel)
		switch dpOpt.writer.(type) {
		case *os.File:
			gz.Filename = dpOpt.writer.(*os.File).Name()
		default:
			log.Println("[gzip] [error] writer stream is not a file!")
			return
		}

		if err = gz.Compress(); err != nil {
			log.Printf("[gzip] [error] %v \n", err)
			return
		}
	}
	return
}

func (o *dumpOption) dump(dsn string, opts ...DumpOption) (err error) {
	o.Startime = time.Now()
	log.Printf("[BACKUP] [dump] started at %s\n", o.Startime.Format(DEFAULT_LOG_TIMESTAMP))

	defer func() {
		end := time.Now()
		log.Printf("[BACKUP] [dump] terminated at %s, execution time %s\n", end.Format(DEFAULT_LOG_TIMESTAMP), end.Sub(o.Startime))
	}()

	// iterate over existing plugins (With...)
	// and execute it
	for _, opt := range opts {
		opt(o)
	}

	// parse DSN options
	cfg, err := parseDSN(dsn)
	if err != nil {
		log.Printf("[parse-dsn] [error] %v \n", err)
		return err
	}

	// check if multiple DBs are selected
	// if not then fetch the DB name from current DSN
	if len(o.Dbs) == 0 {
		o.Dbs = []string{
			cfg.DBName,
		}
	}
	if len(o.tables) == 0 {
		o.isAllTables = true
	}

	if o.writer == nil {
		o.writer = os.Stdout
		o.isCompressed = false
	}
	buf := bufio.NewWriter(o.writer)
	defer buf.Flush()

	// get database host
	o.Host = cfg.Addr

	// open connection to Client
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		if o.log {
			log.Printf("[error] %v \n", err)
		}
		return err
	}
	defer db.Close()

	if err = db.QueryRow("SELECT version()").Scan(&o.Version); err != nil {
		log.Printf("[error] %v \n", err)
		return err
	}

	tpl, err := NewTemplate()
	if err != nil {
		log.Printf("[template] [error] %v \n", err)
		return err
	}

	// inject header template
	if err := tpl.Header.Execute(buf, o); err != nil {
		log.Printf("[header] [error] %v \n", err)
		return err
	}

	if o.isAllDB {
		o.Dbs, err = getDBs(db)
		if err != nil {
			if o.log {
				log.Printf("[error] %v \n", err)
			}
			return err
		}
	}
	if len(o.Dbs) > 1 {
		o.isUseDb = true
	}

	for _, dbStr := range o.Dbs {
		_, err = db.Exec(fmt.Sprintf("USE `%s`", dbStr))
		if err != nil {
			if o.log {
				log.Printf("[error] %v \n", err)
			}
			return err
		}

		var tables []string
		if o.isAllTables {
			tmp, err := getAllTables(db)
			if err != nil {
				if o.log {
					log.Printf("[error] %v \n", err)
				}
				return err
			}
			tables = tmp
		} else {
			tables = o.tables
		}
		if o.isUseDb {
			buf.WriteString(fmt.Sprintf("USE `%s`;\n", dbStr))
		}

		for _, table := range tables {
			tt, err := getTableType(db, table)
			if err != nil {
				return err
			}

			if tt == "TABLE" {
				if o.isDropTable {
					buf.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
				}

				// Export table structure
				err = o.writeTableStruct(db, table, buf)
				if err != nil {
					if o.log {
						log.Printf("[error] %v \n", err)
					}
					return err
				}
				// Export table data if set
				if o.isData {
					err = writeTableData(db, table, buf, o.perDataNumber)
					if err != nil {
						if o.log {
							log.Printf("[error] %v \n", err)
						}
						return err
					}
				}
				err := writeTableTrigger(db, table, buf)
				if err != nil {
					if o.log {
						log.Printf("[error] %v \n", err)
					}
					return err
				}
			}
			if tt == "VIEW" {
				if o.isDropTable {
					buf.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS  `%s`;\n", table))
				}
				// Export view structure
				err = writeViewStruct(db, table, buf)
				if err != nil {
					if o.log {
						log.Printf("[error] %v \n", err)
					}
					return err
				}
			}
		}
	}

	// inject footer template
	if err := tpl.Footer.Execute(buf, o); err != nil {
		log.Printf("[footer] [error] %v \n", err)
		return err
	}
	return nil
}

func getTableType(db *sql.DB, table string) (t string, err error) {
	var tableType string
	if err = db.QueryRow(
		fmt.Sprintf("SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s'", table)).
		Scan(&tableType); err != nil {
		return "", err
	}

	switch tableType {
	case "BASE TABLE":
		return "TABLE", nil
	case "VIEW":
		return "VIEW", nil
	default:
		return "", nil
	}
}

func getCreateTableSQL(db *sql.DB, table string, checkExists bool) (string, error) {
	var createTableSQL string

	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL)
	if err != nil {
		return "", err
	}
	if checkExists {
		createTableSQL = strings.Replace(createTableSQL, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	}
	return createTableSQL, nil
}

func getDBs(db *sql.DB) ([]string, error) {
	var dbs []string
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var db string
		err = rows.Scan(&db)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return dbs, nil
}

func getAllTables(db *sql.DB) ([]string, error) {
	var tables []string
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (o dumpOption) writeTableStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- Table structure for %s\n", table))
	buf.WriteString("-- ----------------------------\n")

	createTableSQL, err := getCreateTableSQL(db, table, !o.isDropTable)
	if err != nil {
		return err
	}
	buf.WriteString(createTableSQL)
	buf.WriteString(";")

	buf.WriteString("\n\n")
	return nil
}

func writeViewStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	var (
		createTableSQL, charact, connect string
	)

	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- View structure for %s\n", table))
	buf.WriteString("-- ----------------------------\n")

	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL, &charact, &connect)
	if err != nil {
		return err
	}
	buf.WriteString(createTableSQL)
	buf.WriteString(";")

	buf.WriteString("\n\n")
	return nil
}

func writeTableData(db *sql.DB, table string, buf *bufio.Writer, perDataNumber int) error {
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("--Dumping data for table %s\n", table))
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("LOCK TABLES `%s` WRITE;\n", table))
	buf.WriteString(fmt.Sprintf("/*!40000 ALTER TABLE `%s` DISABLE KEYS */;\n", table))

	lineRows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}
	defer lineRows.Close()

	var columns []string
	columns, err = lineRows.Columns()
	if err != nil {
		return err
	}
	columnTypes, err := lineRows.ColumnTypes()
	if err != nil {
		return err
	}

	var values [][]interface{}
	rowId := 0

	for lineRows.Next() {
		ssql := ""
		if rowId == 0 || perDataNumber < 2 || rowId%perDataNumber == 0 {
			if rowId > 0 {
				ssql = ";\n"
			}

			ssql += "INSERT INTO `" + table + "` (`" + strings.Join(columns, "`,`") + "`) VALUES \n"
		} else {
			buf.WriteString(",\n")
		}

		row := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i := range columns {
			rowPointers[i] = &row[i]
		}
		err = lineRows.Scan(rowPointers...)
		if err != nil {
			return err
		}
		rowString, err := buildRowData(row, columnTypes)
		if err != nil {
			return err
		}
		ssql += "(" + rowString + ")"
		rowId += 1
		buf.WriteString(ssql)
		values = append(values, row)
	}

	buf.WriteString(";\n")
	buf.WriteString(fmt.Sprintf("/*!40000 ALTER TABLE `%s` ENABLE KEYS */;\n", table))
	buf.WriteString("UNLOCK TABLES;\n\n")
	return nil
}

func buildRowData(row []interface{}, columnTypes []*sql.ColumnType) (ssql string, err error) {
	for i, col := range row {
		if col == nil {
			ssql += "NULL"
		} else {
			Type := columnTypes[i].DatabaseTypeName()

			Type = strings.Replace(Type, "UNSIGNED", "", -1)
			Type = strings.Replace(Type, " ", "", -1)
			switch Type {
			case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
				if bs, ok := col.([]byte); ok {
					ssql += fmt.Sprintf("%s", string(bs))
				} else {
					ssql += fmt.Sprintf("%d", col)
				}

			case "FLOAT", "DOUBLE":
				if bs, ok := col.([]byte); ok {
					ssql += fmt.Sprintf("%s", string(bs))
				} else {
					ssql += fmt.Sprintf("%f", col)
				}

			case "DECIMAL", "DEC":
				ssql += fmt.Sprintf("%s", col)

			case "DATE":
				t, ok := col.(time.Time)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", t.Format("2006-01-02"))

			case "DATETIME", "TIMESTAMP":
				t, ok := col.(time.Time)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", t.Format(DEFAULT_LOG_TIMESTAMP))

			case "TIME":
				t, ok := col.([]byte)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", string(t))

			case "YEAR":
				t, ok := col.([]byte)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("%s", string(t))

			case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
				ssql += fmt.Sprintf("'%s'", sanitize(fmt.Sprintf("%s", col)))

			case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
				ssql += fmt.Sprintf("0x%X", col)

			case "ENUM", "SET", "JSON":
				ssql += fmt.Sprintf("'%s'", col)

			case "BOOL", "BOOLEAN":
				if col.(bool) {
					ssql += "true"
				} else {
					ssql += "false"
				}

			default:
				return "", fmt.Errorf("unsupported type: %s", Type)
			}
		}
		if i < len(row)-1 {
			ssql += ","
		}
	}
	return ssql, nil
}

func writeTableTrigger(db *sql.DB, table string, buf *bufio.Writer) error {
	var sql []string

	triggers, err := getTrigger(db, table)
	if err != nil {
		return err
	}
	if len(triggers) > 0 {
		sql = append(sql, "-- ----------------------------")
		sql = append(sql, fmt.Sprintf("-- Dump table triggers of %s--------", table))
		sql = append(sql, "-- ----------------------------")
	}
	for _, v := range triggers {
		sql = append(sql, "DELIMITER ;;")
		sql = append(sql, "/*!50003 SET SESSION SQL_MODE=\"\" */;;")
		sql = append(sql, fmt.Sprintf("/*!50003 CREATE TRIGGER `%s` %s %s ON `%s` FOR EACH ROW %s */;;", v.Trigger, v.Timing, v.Event, v.Table, v.Statement))
		sql = append(sql, "DELIMITER ;")
		sql = append(sql, "/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;\n")
	}
	buf.WriteString(strings.Join(sql, "\n"))
	return nil
}

func getTrigger(db *sql.DB, table string) (trigger []triggerStruct, err error) {
	if allTriggers != nil {
		trigger = allTriggers[table]
		return trigger, nil
	} else {
		allTriggers = make(map[string][]triggerStruct)
	}

	trgs, err := db.Query("SHOW TRIGGERS")
	if err != nil {
		return trigger, err
	}
	defer trgs.Close()

	var columns []string
	columns, err = trgs.Columns()

	for trgs.Next() {
		trgrow := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i := range columns {
			rowPointers[i] = &trgrow[i]
		}
		err = trgs.Scan(rowPointers...)
		if err != nil {
			return trigger, err
		}

		var trigger triggerStruct
		for k, v := range trgrow {
			switch columns[k] {
			case "Table":
				trigger.Table = fmt.Sprintf("%s", v)
			case "Event":
				trigger.Event = fmt.Sprintf("%s", v)
			case "Trigger":
				trigger.Trigger = fmt.Sprintf("%s", v)
			case "Statement":
				trigger.Statement = fmt.Sprintf("%s", v)
			case "Timing":
				trigger.Timing = fmt.Sprintf("%s", v)
			}
		}
		allTriggers[trigger.Table] = append(allTriggers[trigger.Table], trigger)
	}
	return allTriggers[table], nil
}
