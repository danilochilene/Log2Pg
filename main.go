package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"strings"
)

const (
	dbUser     = "user"
	dbPassword = "password"
	dbHost     = "127.0.0.1"
	dbName     = "dbname"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: Log2Pg [inputfile]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func whoami() string {
	hostname, err := os.Hostname()
	checkErr(err)
	return hostname
}

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Input log is missing.")
		os.Exit(1)
	}

	file, err := os.Open(os.Args[1])
	checkErr(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	dbinfo := fmt.Sprintf("user=%s host=%s password=%s dbname=%s sslmode=disable",
		dbUser, dbHost, dbPassword, dbName)
	db, err := sql.Open("postgres", dbinfo)
	checkErr(err)
	defer db.Close()
	fmt.Println("### Inserting values into", dbHost)
	for scanner.Scan() {
		s := strings.Fields(scanner.Text())
		// Ignore lines that starts with #
		if strings.HasPrefix(s[0], "#") {
			continue
		}

		var lastInsertID string
		timeStamp := []string{s[2], s[3]}
		data := strings.Join(timeStamp, " ")
		err = db.QueryRow("INSERT INTO logs(origin,destiny,date,method,service,status,server) values($1,$2,$3,$4,$5,$6,$7);",
			s[1], s[0], data, s[4], s[5], s[6], whoami()).Scan(&lastInsertID)
	}

}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
