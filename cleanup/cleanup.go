package cleanup

import (
	"log"
	"runtime"
	"sync"
	"time"
	"github.com/jmoiron/sqlx"
	"fmt"
)

type CleanUpTable struct {
	Db     *sqlx.DB
	Table  string
	Period time.Duration
	Tick   time.Duration
	Limit  int64
}


func (c CleanUpTable) start(wg *sync.WaitGroup) {
	go c.controller(wg)
}

func (c CleanUpTable) controller(wg *sync.WaitGroup) {
	for {
		if err := c.cleanup(); err != nil {
			wg.Done()
			log.Print(err)
			return
		}
		select {
		case <-time.Tick(c.Tick):
		}
	}
}

func (c CleanUpTable) cleanup() error {

	redo := true

	rowsAffected := make(chan int64, 1)
	defer close(rowsAffected)

	errs := make(chan error, 1)
	defer close(errs)
	limit := c.Limit
	for redo {
		go func() {
			result, err := c.Db.Exec(
				fmt.Sprintf(`DELETE FROM %s WHERE timestamp < ? LIMIT %d`, c.Table, limit),
				time.Now().Add(-1*c.Period).Unix())
			if err != nil {
				errs <- err
				return
			}
			affected, err := result.RowsAffected()

			if err != nil {
				errs <- err
				return
			}
			log.Printf("Rows affected in %s: %v", c.Table, affected)
			rowsAffected <- affected
		}()

		select {
		case affected:= <-rowsAffected:
			if affected < limit {
				redo = false
			}
			limit = c.Limit
		case <-time.After(time.Second):
			limit = limit/2
		case err := <-errs:
			return err
		}
	}
	return nil
}

func Cleanuprun(){
	db, err := sqlx.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")
	if err != nil {
		panic(err)
	}

	res, _ := db.Query(`SHOW TABLES`)

	var table string

	var schema_tables = map[string]CleanUpTable{}
	for res.Next() {
		res.Scan(&table)
		var tempPeriod, tempTick time.Duration
		var limit int64
		switch {
		case table == "test_table":
			tempPeriod, _ = time.ParseDuration("18000h")
			tempTick, _ = time.ParseDuration("2m")
			limit = 1000
		case table == "test_table2":
			tempPeriod, _ = time.ParseDuration("800h")
			tempTick, _ = time.ParseDuration("1.5m")
			limit = 1000
		default:
			tempPeriod, _ = time.ParseDuration("7000h")
			tempTick, _ = time.ParseDuration("1.5m")
			limit = 10000
		}

		tempSchema := CleanUpTable{
			Db: db,
			Table: table,
			Period: tempPeriod,
			Tick: tempTick,
			Limit: limit,
		}
		schema_tables[tempSchema.Table] = tempSchema
	}

	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())

	for _, CleanupStruct := range schema_tables {

		wg.Add(1)
		fmt.Println(CleanupStruct)
		go CleanupStruct.start(&wg)
	}
	wg.Wait()
}