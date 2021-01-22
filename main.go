package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/raviks789/testdb/cleanup"
	"github.com/raviks789/testdb/config"
	"github.com/raviks789/testdb/supervar"
	"log"
	"sync"
	//"github.com/raviks789/testdb/insertworker"
)

func main() {
	fmt.Printf("\n")

	fmt.Println("Testing clean up on testdb")
	var m sync.Mutex
	config.Congigrun(&m)
	db, err := sqlx.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")
	if err != nil {
		log.Println(err)
	}
	defer db.Close()

	super := supervar.SuperVar{
		Db: db,
		Wg: &sync.WaitGroup{},
	}
	//go insertworker.Workerrun()
	go cleanup.Cleanuprun(&super)

	forever()
}

func forever() {
	select{}
}
