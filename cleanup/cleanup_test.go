package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/raviks789/testdb/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"sync"
	"testing"
	log "github.com/sirupsen/logrus"
	"time"
)

func NewTestCleanup(db *sql.DB) *Cleanup {
	return &Cleanup{
		dbw: db,
		wg:  &sync.WaitGroup{},
		tick: time.Second,
		limit: 10,
	}
}

func TestCleanup_Start(t *testing.T) {
	var m sync.Mutex
	config.Congigrun(&m)

	db, _ := sql.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")

	testclean := NewTestCleanup(db)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS testing123 (data text NOT NULL, timestamp bigint(20) NOT NULL, INDEX idx_event_time (timestamp))`))
	require.NoError(t, err)

	filename := "testdata.csv"
	mysql.RegisterLocalFile(filename)
	result, err := db.Exec(fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s' INTO TABLE testing123`, filename))
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	log.Printf("%d rows inserted\n", affected)
	assert.Equal(t, int64(100), affected)
	ctx := context.Background()
    if err := testclean.cleanup(Tableconfig{"testing123", time.Hour}, ctx); err !=nil {
    	require.NoError(t, err)
	}

	time.Sleep(1 * time.Millisecond)
	countres := db.QueryRow("SELECT COUNT(*) FROM  testing123")
	var count int
	if err:= countres.Scan(&count); err != nil {
		log.Error(err)
	}

	assert.Equal(t, 0, count)
	assert.Equal(t, 10, runtime.NumGoroutine())
	_, err = db.Exec("DROP TABLE testing123")
	require.NoError(t, err)
	db.Close()
}
