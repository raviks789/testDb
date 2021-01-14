package cleanup

import (
	"context"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"runtime"
	"sync"
	"time"
)

type CleanUpTable struct {
	Db     *sqlx.DB
	Table  string
	Period time.Duration
	Tick   time.Duration
	Limit  int64
}


func (c CleanUpTable) start(wg *sync.WaitGroup, ctx context.Context) {
	go c.controller(wg, ctx)
}

func (c CleanUpTable) controller(wg *sync.WaitGroup, ctx context.Context) {
	subctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer wg.Done()
	errChan := make(chan error, 1)

	defer close(errChan)
	for {
		if err := c.cleanup(subctx); err != nil {
			errChan <- err
		}
		select {
		case <-time.Tick(c.Tick):
		case err := <-errChan:
			cancel()

			log.Println(err)
		case <-subctx.Done():
			log.Fatalln(subctx.Err())
		}
	}
}

func (c CleanUpTable) cleanup(ctx context.Context) error {

	redo := true

	rowsAffected := make(chan int64, 1)
	defer close(rowsAffected)

	errs := make(chan error, 1)
	defer close(errs)
	limit := c.Limit

	for redo {
		go func() {
			result, err := c.Db.ExecContext(
				ctx,
				fmt.Sprintf(`DELETE FROM %s WHERE timestamp < ? LIMIT %d`, c.Table, limit),
				time.Now().Add(-1*c.Period).Unix())
			if err != nil {
				fmt.Println(c.Table)

				errs <- err
				return
			}
			affected, err := result.RowsAffected()

			if c.Table == "test_table" {
				err = errors.New("Testing on test table")
			}
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
			tempPeriod, _ = time.ParseDuration("8000h")
			tempTick, _ = time.ParseDuration("2m")
			limit = 1000
		case table == "test_table2":
			tempPeriod, _ = time.ParseDuration("3.5h")
			tempTick, _ = time.ParseDuration("3m")
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

	ctx := context.TODO()
	for _, CleanupStruct := range schema_tables {

		wg.Add(1)
		go CleanupStruct.start(&wg, ctx)
	}
	wg.Wait()
}