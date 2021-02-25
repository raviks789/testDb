package cleanup

import "C"
import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
	"sync"
	"time"
)

type Cleanup struct {
	dbw          *sql.DB
	wg           *sync.WaitGroup
	tick         time.Duration
	limit        int
}

type Cleanupfunc func(ctx context.Context, tbl Tableconfig, db *sql.DB, limit int) (sql.Result, error)

type Funcmap map[string]Cleanupfunc

type Tableconfig struct {
	Table     string
	Period    time.Duration
	Starttime time.Time
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

	var CuConfig = Funcmap{
		"test_table": CleanTestFunc,
		"test_table2": CleanTest2Func,
	}

	for table, period := range tables {
		tempPeriod, _ := time.ParseDuration(period)
		c.wg.Add(1)

		go c.controller(ctx, Tableconfig{table, tempPeriod, time.Now()}, CuConfig[table])
	}

	c.wg.Wait()
	return nil
}

func (c *Cleanup) controller(ctx context.Context, t Tableconfig, cleanupfunc Cleanupfunc) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		err, _ := c.cleanup(ctx, t, cleanupfunc)
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

func (c *Cleanup) cleanup(ctx context.Context, t Tableconfig, cleanupfunc Cleanupfunc) (error, int) {

	redo := true

	rowsAffected := make(chan int, 1)
	defer close(rowsAffected)

	errs := make(chan error, 1)
	defer close(errs)
	limit := c.limit
	tbl := t.Table
	prd := t.Period
	numdel := 0

	for redo {
		go func() {
			result, err := cleanupfunc(ctx, t, c.dbw, limit)

			if err != nil {
				fmt.Println(prd)

				errs <- err
				return
			}
			affected, err := result.RowsAffected()

			//if tbl == "test_table" {
			//	errs <- errors.New("test_table")
			//}
			log.Printf("Rows affected in %s: %v", tbl, affected)
			rowsAffected <- int(affected)
			return
		}()

		select {
		case affected:= <-rowsAffected:
			numdel ++
			if affected < limit {
				redo = false
			}
			limit = c.limit
		case <-time.After(1*time.Second):
			limit = limit/2
		case err := <-errs:
			return err, numdel
		}
	}

	return nil, numdel
}

func CleanTestFunc(ctx context.Context, tblcfg Tableconfig, db *sql.DB, limit int) (sql.Result, error){

	event_time := TimeToMillisecs(tblcfg.Starttime.Add(-1*tblcfg.Period))
	result, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE %s.timestamp < ? LIMIT %d`, tblcfg.Table, tblcfg.Table, limit),
		event_time)

	return result, err
}

func CleanTest2Func(ctx context.Context, tblcfg Tableconfig, db *sql.DB, limit int) (sql.Result, error){
	result, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE %s.timestamp < ? LIMIT %d`, tblcfg.Table, tblcfg.Table, limit),
		TimeToMillisecs(tblcfg.Starttime.Add(-1*tblcfg.Period)))

	return result, err
}


// TimeToMillisecs returns t as ms since *nix epoch.
func TimeToMillisecs(t time.Time) int64 {
	sec := t.Unix()
	return sec*1000 + int64(t.Sub(time.Unix(sec, 0))/time.Millisecond)
}
