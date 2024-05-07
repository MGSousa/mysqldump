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

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

type dumpOption struct {
	//Export table data
	isData bool
	// Export the specified database, mutually exclusive with WithAllDatabases, WithAllDatabases has higher priority
	dbs []string
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
	// Whether to output log
	log bool
	// Client version
	version string
}
type triggerStruct struct {
	Trigger   string
	Event     string
	Table     string
	Statement string
	Timing    string
}

var allTriggers map[string][]triggerStruct

type DumpOption func(*dumpOption)

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
		option.dbs = databases
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

// Whether to output logs
func WithLogErrors() DumpOption {
	return func(option *dumpOption) {
		option.log = true
	}
}

// Dump Export DB contents from MySQL/MariaDB to a writer source (file, stdOut, etc.)
// nolint: gocyclo
func Dump(dsn string, opts ...DumpOption) error {
	var (
		err error
		o   dumpOption
	)

	start := time.Now()
	log.Printf("[INFO] [dump] started at %s\n", start.Format(DEFAULT_LOG_TIMESTAMP))

	// calculate dump Execution Time
	defer func() {
		end := time.Now()
		log.Printf("[INFO] [dump] terminated at %s, cost %s\n", end.Format(DEFAULT_LOG_TIMESTAMP), end.Sub(start))
	}()

	for _, opt := range opts {
		opt(&o)
	}

	if len(o.dbs) == 0 {
		dbName, err := GetDBNameFromDSN(dsn)
		if err != nil {
			log.Printf("[error] %v \n", err)
			return err
		}
		o.dbs = []string{
			dbName,
		}
	}
	if len(o.tables) == 0 {
		o.isAllTables = true
	}

	if o.writer == nil {
		o.writer = os.Stdout
	}

	buf := bufio.NewWriter(o.writer)
	defer buf.Flush()

	// get database host
	dbHost, err := GetDBHostFromDSN(dsn)
	if err != nil {
		log.Printf("[error] %v \n", err)
		return err
	}

	// open connection to Database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		if o.log {
			log.Printf("[error] %v \n", err)
		}
		return err
	}
	defer db.Close()

	v := db.QueryRow("SELECT version()")
	if err = v.Scan(&o.version); err != nil {
		log.Printf("[error] %v \n", err)
		return err
	}

	// Header
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString("-- MySQL Database Dump\n")
	buf.WriteString("-- Host: " + dbHost + "\n")
	buf.WriteString("-- Database(s): " + joinS(o.dbs, ",") + "\n")
	buf.WriteString("-- Server version: " + o.version + "\n")
	buf.WriteString("-- Start Time: " + start.Format(DEFAULT_LOG_TIMESTAMP) + "\n")
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString("\n\n")
	buf.WriteString("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n")

	var dbs []string
	if o.isAllDB {
		dbs, err = getDBs(db)
		if err != nil {
			if o.log {
				log.Printf("[error] %v \n", err)
			}
			return err
		}
	} else {
		dbs = o.dbs
	}
	if len(dbs) > 1 {
		o.isUseDb = true
	}

	for _, dbStr := range dbs {
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
			// When exporting multiple libraries, the library selection operation will be added.
			// Otherwise, the library selection operation will not be added.
			buf.WriteString(fmt.Sprintf("USE `%s`;\n", dbStr))
		}

		// 3. 导出表
		for _, table := range tables {
			tt, err := getTableType(db, table)
			if err != nil {
				return err
			}

			if tt == "TABLE" {
				// Drop table if set
				if o.isDropTable {
					buf.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
				}

				// Export table structure
				err = writeTableStruct(db, table, buf)
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
				// Drop view if set
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

	buf.WriteString("-- ----------------------------\n")
	buf.WriteString("-- Dumped by mysqldump\n")
	buf.WriteString("-- Cost Time: " + time.Since(start).String() + "\n")
	buf.WriteString("-- ----------------------------\n")
	buf.Flush()

	return nil
}

func getTableType(db *sql.DB, table string) (t string, err error) {
	var tableType string
	err = db.QueryRow(
		fmt.Sprintf("SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s'", table)).
		Scan(&tableType)
	if err != nil {
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

func getCreateTableSQL(db *sql.DB, table string) (string, error) {
	var createTableSQL string

	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL)
	if err != nil {
		return "", err
	}
	// IF NOT EXISTS
	createTableSQL = strings.Replace(createTableSQL, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
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

func writeTableStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- Table structure for %s\n", table))
	buf.WriteString("-- ----------------------------\n")

	createTableSQL, err := getCreateTableSQL(db, table)
	if err != nil {
		return err
	}
	buf.WriteString(createTableSQL)
	buf.WriteString(";")

	buf.WriteString("\n\n")
	buf.WriteString("\n\n")
	return nil
}

func writeViewStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- View structure for %s\n", table))
	buf.WriteString("-- ----------------------------\n")

	var (
		createTableSQL string
		charact        string
		connect        string
	)
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL, &charact, &connect)
	if err != nil {
		return err
	}
	buf.WriteString(createTableSQL)
	buf.WriteString(";")

	buf.WriteString("\n\n")
	buf.WriteString("\n\n")
	return nil
}

func writeTableData(db *sql.DB, table string, buf *bufio.Writer, perDataNumber int) error {
	buf.WriteString("-- ----------------------------\n")
	buf.WriteString(fmt.Sprintf("-- Records of %s\n", table))
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
			case "DATETIME":
				t, ok := col.(time.Time)
				if !ok {
					return "", err
				}
				ssql += fmt.Sprintf("'%s'", t.Format(DEFAULT_LOG_TIMESTAMP))
			case "TIMESTAMP":
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
				r := strings.NewReplacer("\n", "\\n", "'", "\\'", "\r", "\\r", "\"", "\\\"")
				ssql += fmt.Sprintf("'%s'", r.Replace(fmt.Sprintf("%s", col)))
				// ssql += fmt.Sprintf("'%s'", strings.Replace(fmt.Sprintf("%s", col), "'", "''", -1))
			case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
				ssql += fmt.Sprintf("0x%X", col)
			case "ENUM", "SET":
				ssql += fmt.Sprintf("'%s'", col)
			case "BOOL", "BOOLEAN":
				if col.(bool) {
					ssql += "true"
				} else {
					ssql += "false"
				}
			case "JSON":
				ssql += fmt.Sprintf("'%s'", col)
			default:
				// unsupported type
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
