package insertworker

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"runtime"
	"sync"
	"time"
	"github.com/brianvoe/gofakeit"
)

type ProductModel struct {
	Db *sqlx.DB
}

func Workerrun() {
	var ch = make(chan string)
	var quit = make(chan bool)
	var wg sync.WaitGroup
	xthreads := 8

	runtime.GOMAXPROCS(runtime.NumCPU())

	// username:password@protocol(address)/dbname?param=value
	db, err := sqlx.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")

	if err != nil {
		fmt.Println(err)
	} else {

		productModel := ProductModel{
			Db: db,
		}

		j := make([]int, 8)

		now := time.Now()
		nowUnix := now.Unix()

		before1 := now.AddDate(-3, 0, 0).Unix()
		before2 := now.AddDate(-1, 0 , 0).Unix()
		//done := make(chan struct{})

		for i:=0; i<xthreads; i++ {
			jobNo := i

			wg.Add(1)

			go func() {
			loop: for {
				select {
				case a, ok := <-ch:
					if !ok { // if there is nothing to do and the channel has been closed then end the goroutine
						wg.Done()
						return
					}
					if before1 < nowUnix {
						before1 += 1
					} else {
						before1 =time.Now().Unix()
					}

					if before2 < nowUnix {
						before2 += 1
					} else {
						before2 =time.Now().Unix()
					}
					//InsertWorker(randData(), count) // insert row into test_table
					productModel.InsertWorker(a, jobNo, j[jobNo], before1, before2)
					j[jobNo] = j[jobNo] + 1

				//case <-done:
				//	fmt.Printf("Error occured")
				//	close(quit)
				//	break loop
				case <-time.After(time.Second):
					close(quit)
					break loop
				}
			}
			}()
		}
	}
	go func() {
		defer close(ch)
		for {
			time.Sleep(time.Millisecond * 1)

			select {
			case ch <-randData():
			case <-quit:
				close(ch)
			}

		}
	}()

	wg.Wait()
}

func randData() string {
	gofakeit.Seed(0)
	return gofakeit.Sentence(10)
}

func (productModel ProductModel) InsertWorker(a string, job, count int, before1, before2 int64) { // (productModel ProductModel) InsertWorker(a string)
	if count%500 == 0 {
		fmt.Printf("Insert Worker string: %v, job: %d, count: %d \n", a, job, count)
	}

	stmt1, err := productModel.Db.Prepare("INSERT INTO test_table(test_table.data, test_table.timestamp) VALUES(?, ?)")
	if err != nil {
		ErrorCheck(err)
	}
	_, inErr := stmt1.Exec(a, before1)
	if inErr != nil {
		ErrorCheck(inErr)
	}
	stmt2, err := productModel.Db.Prepare("INSERT INTO test_table2(test_table2.data2, test_table2.timestamp) VALUES(?, ?)")
	if err != nil {
		ErrorCheck(err)
	}
	_, inErr = stmt2.Exec(a, before2)
	if inErr != nil {
		ErrorCheck(inErr)
	}
}

func ErrorCheck(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func (productModel ProductModel) MinTimestamp() (int, error) {
	rows, err := productModel.Db.Query("SELECT MIN(timestamp) FROM test_table")
	if err != nil {
		return 0, err
	} else {
		var min_timestamp int
		for rows.Next() {
			rows.Scan(&min_timestamp)
		}
		return min_timestamp, nil
	}
}

func (productModel ProductModel) MaxTimestamp() (int, error) {
	rows, err := productModel.Db.Query("SELECT MAX(timestamp) FROM test_table")
	if err != nil {
		return 0, err
	} else {
		var max_timestamp int
		for rows.Next() {
			rows.Scan(&max_timestamp)
		}
		return max_timestamp, nil
	}
}

func (productModel ProductModel) CountRows() int {
	var count int

	_ = productModel.Db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	return count
}
