package main

import (
	"database/sql"
	"fmt"
	"github.com/raviks789/testdb/cleanup"
	log "github.com/sirupsen/logrus"
	"github.com/raviks789/testdb/config"
	"sync"
)

func main() {
	fmt.Printf("\n")

	fmt.Println("Testing clean up on testdb")
	var m sync.Mutex
	config.Congigrun(&m)
	db, err := sql.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")
	if err != nil {
		log.Println(err)
	}

	cu := cleanup.NewCleanup(db)
	if errcu := cu.Start(); errcu != nil {
		log.Error(errcu)
	}
	forever()
}

func forever() {
	select{}
}
