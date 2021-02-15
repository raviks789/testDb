package config

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)

const (
	username = "testdb"
	password = "testdb"
	schemaName = "testdb"
	hostname = "mysql:3306"
)

type ProductModel struct {
	Db *sqlx.DB
}

func Congigrun(m *sync.Mutex) {
	m.Lock()
	db, err := DBConfig()
	if err != nil {
		log.Fatalf("Error %s when opening DB", err)
	}

	productModel := ProductModel{db}
	productModel.Userconfig()
	productModel.Tablesconfig()
	log.Printf("testdb has been configured\n")
	m.Unlock()
	return
}

// username:password@protocol(address)/dbname?param=value
func DBConfig() (*sqlx.DB, error){
	fmt.Println("setting up testdb schema")
	db, err := sqlx.Open("mysql", dsn(""))
	if err != nil {
		err = errors.New(fmt.Sprintf("Error %s when opening DB\n", err))
		return nil, err
	}
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS "+schemaName)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error %s when creating DB", err))
		return nil, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		err = errors.New(fmt.Sprintf("Error %s when fetching rows", err))
		return nil, err
	}
	log.Printf("rows affected %d\n", n)

	db, err = sqlx.Open("mysql", dsn(schemaName))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		err = errors.New(fmt.Sprintf("Errors %s pinging DB", err))
		return nil, err
	}
	log.Printf("Connected to DB %s successfully\n", schemaName)
	return db, nil
}

func (productModel ProductModel) Userconfig() {
	//db, err := sqlx.Open("mysql", dsn(schemaName))
	//defer db.Close()

	db, err := sqlx.Open("mysql", dsn(""))
	productModel.Db = db
	if err != nil {
		err = errors.New(fmt.Sprintf("Error %s when opening DB\n", err))
		return
	}
	res, err := productModel.Db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY '%s'`, username, password))
	if err != nil {
		log.Printf("Error %s when creating User\n", err)
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected %d\n", n)

	if n > 0 {
		res, err = productModel.Db.Exec(fmt.Sprintf(`GRANT ALL PRIVILEGES ON %s.* TO %s@'%%' IDENTIFIED BY 'testdb'`, schemaName, username))
		if err != nil {
			log.Printf("Error %s when granting permissions User\n", err)
			return
		}
	}
}

func (productModel ProductModel) Tablesconfig() {

	query1 := `CREATE TABLE IF NOT EXISTS test_table(data text NOT NULl, 
               timestamp bigint(20) NOT NULL)ENGINE=InnoDB`
	res, err := productModel.Db.Exec(fmt.Sprintf(query1))
	if err != nil {
		log.Printf("Error %s when creating User\n", err)
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected %d\n", n)
	if n > 0 {
		res, err = productModel.Db.Exec(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_timestamp ON test_table(test_table.timestamp)`))
		if err != nil {
			log.Printf("Error %s when granting permissions User\n", err)
			return
		}
	}
	query2 := `CREATE TABLE IF NOT EXISTS test_table2(data2 text NOT NULl, 
               timestamp bigint(20) NOT NULL)ENGINE=InnoDB`
	res, err = productModel.Db.Exec(fmt.Sprintf(query2))
	if err != nil {
		log.Printf("Error %s when creating User\n", err)
		return
	}
	n, err = res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected %d\n", n)
	if n > 0 {
		res, err = productModel.Db.Exec(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_timestamp2 ON test_table2(test_table2.timestamp)`))
		if err != nil {
			log.Printf("Error %s when granting permissions User\n", err)
			return
		}
	}
}

func dsn(dbName string) string {
	if dbName == "" {
		return fmt.Sprintf("%s:%s@tcp(%s)/%s", "root", "password", hostname, dbName)
	}
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}
