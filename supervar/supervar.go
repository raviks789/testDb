package supervar

import (
	"github.com/jmoiron/sqlx"
	"sync"
)

type SuperVar struct {
	Db *sqlx.DB
	Wg *sync.WaitGroup
}
