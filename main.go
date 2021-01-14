package main

import (
	"fmt"
	"github.com/raviks789/testdb/cleanup"

	//"github.com/raviks789/testdb/insertworker"
)

func main() {
	fmt.Printf("\n")

	fmt.Println("Testing clean up on testdb")
	//go insertworker.Workerrun()
	go cleanup.Cleanuprun()
	forever()
}

func forever() {
	select{}
}
