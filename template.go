package mysqldump

import "text/template"

const (
	header = `-- mysqldump
-- Server Host: {{ .Host }}
-- Database(s): {{ join .Dbs ", " }}
-- Start Time: {{ .Startime.Format "2006-01-02 15:04:05" }}
-- ------------------------------------------------------
-- Server version:	{{ .Version }}

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8mb4 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
`

	footer = `
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- ----------------------------
-- Dumped by mysqldump
-- Execution Time: {{ untilNow .Startime }}
-- ----------------------------
`
)

type Template struct {
	Header, Footer *template.Template
}

// NewTemplate
func NewTemplate() (t Template, err error) {
	t = Template{}

	if t.Header, err = template.New("mysqldumpHeader").
		Funcs(template.FuncMap{"join": joinS}).
		Parse(header); err != nil {
		return
	}
	if t.Footer, err = template.New("mysqldumpFooter").
		Funcs(template.FuncMap{"untilNow": untilNow}).
		Parse(footer); err != nil {
		return
	}
	return
}
