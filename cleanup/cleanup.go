package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Cleanup struct {
	dbw          *sql.DB
	wg           *sync.WaitGroup
	tick         time.Duration
	limit        int64
}

type Tableconfig struct {
	Table     string
	Period    time.Duration
}


func NewCleanup(db *sql.DB) *Cleanup {
	return &Cleanup{
		dbw: db,
		wg:  &sync.WaitGroup{},
		tick: time.Second,
		limit: 5000,
	}
}


func (c *Cleanup) Start() error {
	configPath := flag.String("config", "cleanup/testdb.ini", "path to config")
	flag.Parse()

	cfg, err := ini.Load(*configPath)
	if err != nil {
		return err
	}

	tables := cfg.Section("housekeeping").KeysHash()
	ctx := context.Background()

	for table, period := range tables {
		tempPeriod, _ := time.ParseDuration(period)
		c.wg.Add(1)

		go c.controller(Tableconfig{table, tempPeriod}, ctx)
	}

	c.wg.Wait()
	return nil
}

func (c *Cleanup) controller(t Tableconfig, ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		err := c.cleanup(t, ctx)
		if err != nil {
			cancel()
		}
		select {
		case <-time.Tick(c.tick):
		case <-ctx.Done():
			log.Error(err)
			c.wg.Done()
			return
		}
	}
}

func (c *Cleanup) cleanup(t Tableconfig, ctx context.Context) error {

	redo := true

	rowsAffected := make(chan int64, 1)
	defer close(rowsAffected)

	errs := make(chan error, 1)
	defer close(errs)
	limit := c.limit
	tbl := t.Table
	prd := t.Period

	for redo {
		go func() {
			result, err := c.dbw.ExecContext(
				ctx,
				fmt.Sprintf(`DELETE FROM %s WHERE timestamp < ? LIMIT %d`, tbl, limit),
				time.Now().Add(-1*prd).Unix())
			if err != nil {
				fmt.Println(prd)

				errs <- err
				return
			}
			affected, err := result.RowsAffected()

			if err != nil {
				errs <- err
				return
			}
			if tbl == "test_table" {
				errs <- errors.New("test_table")
			}
			log.Printf("Rows affected in %s: %v", tbl, affected)
			rowsAffected <- affected
		}()

		select {
		case affected:= <-rowsAffected:
			if affected < limit {
				redo = false
			}
			limit = c.limit
		case <-time.After(time.Second):
			limit = limit/2
		case err := <-errs:
			return err
		}
	}
	return nil
}
//
//func Cleanuprun(db *sql.DB){
//	configPath := flag.String("config", "cleanup/testdb.ini", "path to config")
//	flag.Parse()
//
//	cfg, err := ini.Load(*configPath)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	wg := &sync.WaitGroup{}
//	tables := cfg.Section("housekeeping").KeysHash()
//	housekeeping := map[string]CleanUp{}
//
//	for table, period := range tables {
//		tempPeriod, _ := time.ParseDuration(period)
//		housekeeping[table] = CleanUp{table, tempPeriod}
//	}
//
//	ctx := context.Background()
//	for _, c := range housekeeping {
//		wg.Add(1)
//		go c.start(db, wg, ctx)
//	}
//	wg.Wait()
//}
