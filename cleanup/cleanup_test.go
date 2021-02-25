package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"sync"
	"testing"
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

func TestCleanupMore(t *testing.T) {

	db, _ := sql.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")

	testclean := NewTestCleanup(db)

	startTime := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS testing123 (data text NOT NULL, event_time bigint(20) NOT NULL, INDEX idx_event_time (event_time))`))
	require.NoError(t, err)

	filename := "testdatamore.csv"
	mysql.RegisterLocalFile(filename)

	result, err := db.Exec(fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s' INTO TABLE testing123 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'`, filename))
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	log.Printf("%d rows inserted\n", affected)
	assert.Equal(t, int64(200), affected)

	ctx := context.TODO()

	err, cnt := testclean.cleanup(ctx, Tableconfig{"testing123", time.Hour, startTime}, CleanTestingFunc)
	if err !=nil {
		require.NoError(t, err)
	}
	countres := db.QueryRow("SELECT COUNT(*) FROM  testing123")

	var count int
	if err:= countres.Scan(&count); err != nil {
		log.Error(err)
	}

	assert.Equal(t, 100, count)

	assert.Equal(t, 11, cnt)
	_, err = db.Exec("DROP TABLE testing123")
	require.NoError(t, err)

	db.Close()
	return
}

func TestCleanupLess(t *testing.T) {

	db, _ := sql.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")

	testclean := NewTestCleanup(db)

	startTime := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS testing123 (data text NOT NULL, event_time bigint(20) NOT NULL, INDEX idx_event_time (event_time))`))
	require.NoError(t, err)

	filename := "testdataless.csv"
	mysql.RegisterLocalFile(filename)
	result, err := db.Exec(fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s' INTO TABLE testing123 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'`, filename))
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	log.Printf("%d rows inserted\n", affected)
	assert.Equal(t, int64(8), affected)
	ctx := context.TODO()

	err, cnt := testclean.cleanup(ctx, Tableconfig{"testing123", time.Hour, startTime}, CleanTestingFunc)
	if err !=nil {
		require.NoError(t, err)
	}

	countres := db.QueryRow("SELECT COUNT(*) FROM  testing123")
	var count int
	if err:= countres.Scan(&count); err != nil {
		log.Error(err)
	}

	assert.Equal(t, 0, count)
	assert.Equal(t, 1, cnt)
	_, err = db.Exec("DROP TABLE testing123")
	require.NoError(t, err)
	db.Close()
}

func TestCleanupEqual(t *testing.T) {
	db, _ := sql.Open("mysql", "testdb:testdb@tcp(mysql:3306)/testdb")

	testclean := NewTestCleanup(db)

	startTime := time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS testing123 (data text NOT NULL, event_time bigint(20) NOT NULL, INDEX idx_event_time (event_time))`))
	require.NoError(t, err)

	filename := "testdataequal.csv"
	mysql.RegisterLocalFile(filename)
	result, err := db.Exec(fmt.Sprintf(`LOAD DATA LOCAL INFILE '%s' INTO TABLE testing123 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'`, filename))
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	log.Printf("%d rows inserted\n", affected)
	assert.Equal(t, int64(100), affected)
	ctx := context.TODO()

	err, cnt := testclean.cleanup(ctx, Tableconfig{"testing123", time.Hour, startTime}, CleanTestingFunc)
	if err !=nil {
		require.NoError(t, err)
	}

	countres := db.QueryRow("SELECT COUNT(*) FROM  testing123")
	var count int
	runtime.LockOSThread()
	if err:= countres.Scan(&count); err != nil {
		log.Error(err)
	}

	assert.Equal(t, 0, count)
	assert.Equal(t, 11, cnt)
	_, err = db.Exec("DROP TABLE testing123")
	require.NoError(t, err)
	db.Close()

	runtime.UnlockOSThread()
	return
}

func CleanTestingFunc(ctx context.Context, tblcfg Tableconfig, db *sql.DB, limit int) (sql.Result, error){

	event_time := TimeToMillisecs(tblcfg.Starttime.Add(-1*tblcfg.Period))
	result, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE %s.event_time < ? LIMIT %d`, tblcfg.Table, tblcfg.Table, limit),
		event_time)

	return result, err
}
