package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

//////////////////////////////////////////////////////////
// 主処理
//////////////////////////////////////////////////////////

func strarray_exists(aa []string, b string) bool {
	for _, a := range aa {
		if a == b {
			return true
		}
	}
	return false
}

func printMarkdown(res *sql.Rows, table string, exclude_keys []string, fp io.Writer) error {
	fmt.Fprintf(fp, "### %s\n", table)
	fmt.Fprintf(fp, "| Field | Type | Null | Key | Def | Extra |\n")
	fmt.Fprintf(fp, "|:----- |:---- |:----:|:---:|:--- |:----- |\n")
	for res.Next() {
		var name, key, extra string
		var isnull []uint8
		var ktype, defval interface{}
		err := res.Scan(&name, &ktype, &isnull, &key, &defval, &extra)
		if err != nil {
			return err
		}

		if !strarray_exists(exclude_keys, name) {
			isn := "Y"
			if isnull[0] == 78 {
				isn = "N"
			}
			defv := fmt.Sprintf("%s", defval)
			if defv == "%!s(<nil>)" {
				defv = ""
			}
			if name == "id" && key == "PRI" && extra == "auto_increment" {
				extra = ""
			}
			fmt.Fprintf(fp, "|%30s|%13s|%s|%3s|%2v|%4s|\n", name, ktype, isn, key, defv, extra)
		}
	}
	fmt.Fprintf(fp, "\n")
	return nil

}

func printERdiagram(res *sql.Rows, table string, exclude_keys []string, fp io.Writer) error {
	fmt.Fprintf(fp, "\n  %s {\n", table)
	for res.Next() {
		var name, key string
		var ktype, isnull, defval, extra interface{}
		err := res.Scan(&name, &ktype, &isnull, &key, &defval, &extra)
		if err != nil {
			return err
		}
		if !strarray_exists(exclude_keys, name) {
			fmt.Fprintf(fp, "    %s %s %s\n", ktype, name, key)
		}
	}
	fmt.Fprintf(fp, "  }\n\n")
	return nil
}

func printTable(dsn, kind, prefix string, exclude_keys []string, fp io.Writer) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	sql := "SHOW TABLES"
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Printf("query failed: %s\n", sql)
		return err
	}
	defer rows.Close()

	tables := make([]string, 0, 4096)
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return err
		}

		if strings.HasPrefix(table, prefix) {
			tables = append(tables, table)
		}
	}

	for _, table := range tables {
		sql = "SHOW COLUMNS FROM " + table
		res, err := db.Query(sql)
		if err != nil {
			fmt.Printf("query failed: %s\n", sql)
			return err
		}
		defer res.Close()

		if kind == "erd" {
			printERdiagram(res, table, exclude_keys, fp)
		} else {
			printMarkdown(res, table, exclude_keys, fp)
		}

	}

	return nil
}

//////////////////////////////////////////////////////////
// コマンドライン
//////////////////////////////////////////////////////////

//------------------------------------------
// -exclude option
//------------------------------------------
type sliceString []string

func (s *sliceString) String() string {
	return fmt.Sprintf("%s", *s)
}

func (s *sliceString) Set(v string) error {
	*s = append(*s, v)
	return nil
}

//------------------------------------------
// -dsn option
//------------------------------------------
type dbinfo struct {
	Database string `json:"database"`
	User     string `json:"user"`
	Passwd   string `json:"passwd"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

func Json2Dsn(fname string) (string, error) {
	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		return "", err
	}

	var db dbinfo
	err = json.Unmarshal(bytes, &db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", db.User, db.Passwd, db.Host, db.Port, db.Database), nil
}

/**
 * data source として適切か?
 */
func isDsnFormat(dsn string) bool {
	pattern := `^[a-z0-9]+:.*@tcp\([a-z0-9_.-]+:[0-9]+\)/[a-z0-9_.-]+$`
	matched, _ := regexp.MatchString(pattern, dsn)
	return matched
}

/**
 * *.json が指定されたら，ファイル解析し，
 * それ以外だったら dsn 形式かチェックする
 */
func parseDsnOption(opt string) (string, error) {
	dsn := opt
	if strings.HasSuffix(opt, ".json") {
		d, err := Json2Dsn(opt)
		if err != nil {
			return "", err
		}
		dsn = d
	}
	if !isDsnFormat(dsn) {
		return "", fmt.Errorf("invalid format: %s", dsn)
	}
	return dsn, nil
}

func usage(str string) {
	if str != "" {
		fmt.Fprintf(os.Stderr, str)
	}
	flag.Usage()
	os.Exit(1)
}

//////////////////////////////////////////////////////////
// main
//////////////////////////////////////////////////////////
func main() {
	var exclude sliceString
	var (
		dsnop  = flag.String("dsn", "", "Data Source Name: user:pass@tcp(ip:port)/db or dbinfo.json")
		prefix = flag.String("p", "", "table name")
		kind   = flag.String("k", "erd", "print ER diagram (erd) or markdown (md)")
	)
	flag.Var(&exclude, "e", "exclude keys")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: "+os.Args[0]+" -dsn dsn [-p prefix]")
		flag.PrintDefaults()
	}
	flag.Parse()

	dsn, err := parseDsnOption(*dsnop)
	if err != nil {
		usage("-dsn invalid\n")
		return
	}
	if *kind != "erd" && *kind != "md" {
		usage("-k invalid\n")
		return
	}

	err = printTable(dsn, *kind, *prefix, exclude, os.Stdout)
	if err != nil {
		panic(err)
	}

}
