package main

import (
	"fmt"
	"github.com/raviks789/testdb/config"

	//"github.com/raviks789/testdb/insertworker"
)

func main() {
	fmt.Printf("\n")

	fmt.Println("Testing clean up on testdb")
	//go insertworker.Workerrun()
	//go cleanup.Cleanuprun()
	go config.Congigrun()
	forever()
}

func forever() {
	select{}
}
