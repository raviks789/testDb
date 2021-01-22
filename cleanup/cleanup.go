package cleanup

import (
	"context"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/raviks789/testdb/supervar"
	"gopkg.in/ini.v1"
	"log"
	"time"
)

type CleanUp struct {
	Table string
	Period time.Duration
}

const (
	Tick = time.Minute
	Limit = int64(5000)
)

func (c CleanUp) start(super *supervar.SuperVar, ctx context.Context) {

	go c.controller(super, ctx)
}

func (c CleanUp) controller(super *supervar.SuperVar, ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		err := c.cleanup(super, ctx)
		if err != nil {
			cancel()
		}
		select {
		case <-time.Tick(Tick):
		case <-ctx.Done():
			log.Println(err)
			super.Wg.Done()
			return
		}
	}
}

func (c CleanUp) cleanup(super *supervar.SuperVar, ctx context.Context) error {

	redo := true

	rowsAffected := make(chan int64, 1)
	defer close(rowsAffected)

	errs := make(chan error, 1)
	defer close(errs)
	limit := Limit

	for redo {
		go func() {
			result, err := super.Db.ExecContext(
				ctx,
				fmt.Sprintf(`DELETE FROM %s WHERE timestamp < ? LIMIT %d`, c.Table, limit),
				time.Now().Add(-1*c.Period).Unix())
			if err != nil {
				fmt.Println(c.Table)

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
			limit = Limit
		case <-time.After(time.Second):
			limit = limit/2
		case err := <-errs:
			return err
		}
	}
	return nil
}

func Cleanuprun(super *supervar.SuperVar){
	configPath := flag.String("config", "cleanup/testdb.ini", "path to config")
	flag.Parse()

	cfg, err := ini.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	tables := cfg.Section("housekeeping").KeysHash()
	housekeeping := map[string]CleanUp{}

	for table, period := range tables {
		tempPeriod, _ := time.ParseDuration(period)
		housekeeping[table] = CleanUp{table, tempPeriod}
	}

	ctx := context.Background()
	for _, c := range housekeeping {
		super.Wg.Add(1)
		go c.start(super, ctx)
	}
	super.Wg.Wait()
}