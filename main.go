package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/common/log"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var address = flag.String("address", "127.0.0.1:8086", "mysql port")
var update_thread_num = flag.Int("update-thread", 16, "update thread")
var verify_thread_num = flag.Int("verify-thread", 8, "verify thread")
var replica = flag.Int("replica", 2, "tiflash replica num")
var schema = flag.String("schema", "", "schema file path")
var stable = flag.Bool("stable", false, "run stable workload")
var recreate_stable_table = flag.Bool("recreate-stable", false, "recreate stable table")
var insert_batch_count = flag.Int("batch-count", 50, "insert batch count")

// varchar(512)
// varchar(1000)
// bigint(11)
// double
// varchar(100)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(maxLength int) string {
	length := rand.Intn(maxLength) + 1
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func randBigInt() int64 {
	return rand.Int63()
}

func randInt(maxValue int) int {
	return rand.Intn(maxValue)
}

func randDouble() float64 {
	return rand.Float64()
}

type SQLBatchLoader struct {
	insertHint string
	db         *sql.DB
	buf        bytes.Buffer
	count      int
}

// NewSQLBatchLoader creates a batch loader for database connection
func NewSQLBatchLoader(db *sql.DB, hint string) *SQLBatchLoader {
	return &SQLBatchLoader{
		count:      0,
		insertHint: hint,
		db:         db,
	}
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *SQLBatchLoader) InsertValue(query []string) error {
	sep := ", "
	if b.count == 0 {
		b.buf.WriteString(b.insertHint)
		sep = " "
	}
	b.buf.WriteString(sep)
	b.buf.WriteString(query[0])

	b.count++

	if b.count >= *insert_batch_count {
		return b.Flush()
	}

	return nil
}

// Flush inserts all pending values
func (b *SQLBatchLoader) Flush() error {
	if b.buf.Len() == 0 {
		return nil
	}

	_, err := b.db.Exec(b.buf.String())
	if err != nil {
		panic(err)
	}
	b.count = 0
	b.buf.Reset()

	return nil
}

func createTable(db *sql.DB) error {
	data, err := ioutil.ReadFile(*schema)
	if err != nil {
		return err
	}
	_, err = db.Query(string(data))
	if err != nil {
		return err
	}

	_, err = db.Query(fmt.Sprintf("alter table rpt_sdb_account_agent_trans_d set tiflash replica %d", *replica))
	if err != nil {
		return err
	}

	return nil
}

func updateTable(wg *sync.WaitGroup) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/test", *address))
	//db, err := sql.Open("mysql", "root@tcp(127.0.0.1:8000)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer wg.Done()
	loader := NewSQLBatchLoader(db, "INSERT INTO rpt_sdb_account_agent_trans_d VALUES ")
	for {
		v := fmt.Sprintf("('%s','%s','%s','%s','%s',%d,%d,%f,%d,%d,%d,%d,%d,%f,%d,%f,%d,%f,%f,%d,%d,%d,'%s','%s','%s','%s')",
			randString(512),
			randString(512),
			randString(512),
			randString(1000),
			randString(1000),
			randBigInt(),
			randInt(10000),
			randDouble(),
			randBigInt(),
			randBigInt(),
			randBigInt(),
			randBigInt(),
			randBigInt(),
			randDouble(),
			randBigInt(),
			randDouble(),
			randBigInt(),
			randDouble(),
			randDouble(),
			randBigInt(),
			randBigInt(),
			randBigInt(),
			randString(100),
			randString(100),
			randString(100),
			randString(100))
		err := loader.InsertValue([]string{v})
		if err != nil {
			panic(err)
		}
	}
}

func getRow() string {
	return fmt.Sprintf("('%s','%s','%s','%s','%s',%d,%d,%f,%d,%d,%d,%d,%d,%f,%d,%f,%d,%f,%f,%d,%d,%d,'%s','%s','%s','%s') ",
		randString(512),
		randString(512),
		randString(512),
		randString(1000),
		randString(1000),
		randBigInt(),
		randInt(10000),
		randDouble(),
		randBigInt(),
		randBigInt(),
		randBigInt(),
		randBigInt(),
		randBigInt(),
		randDouble(),
		randBigInt(),
		randDouble(),
		randBigInt(),
		randDouble(),
		randDouble(),
		randBigInt(),
		randBigInt(),
		randBigInt(),
		randString(100),
		randString(100),
		randString(100),
		randString(100))
}

func createStableTable(db *sql.DB) {
	_, err := db.Query("Drop table if exists rpt_sdb_account_agent_trans_d2")
	if err != nil {
		panic(err)
	}
	// create rpt_sdb_account_agent_trans_d if need
	createTable(db)

	_, err = db.Query("Create table rpt_sdb_account_agent_trans_d2 like rpt_sdb_account_agent_trans_d")
	if err != nil {
		panic(err)
	}
	loader := NewSQLBatchLoader(db, "INSERT INTO rpt_sdb_account_agent_trans_d2 VALUES ")
	for i := 0; i < 600000; i += 1 {
		v := getRow()
		err := loader.InsertValue([]string{v})
		if err != nil {
			panic(err)
		}
	}
	err = loader.Flush()
	if err != nil {
		panic(err)
	}
}

func stableUpdateTable(wg *sync.WaitGroup) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/test", *address))
	//db, err := sql.Open("mysql", "root@tcp(127.0.0.1:8000)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer wg.Done()
	loader := NewSQLBatchLoader(db, "INSERT INTO rpt_sdb_account_agent_trans_d2 VALUES ")
	for {
		insertNum := rand.Intn(500)
		deleteNum := rand.Intn(500)
		for i := 0; i < insertNum; i += 1 {
			v := getRow()
			err := loader.InsertValue([]string{v})
			if err != nil {
				log.Warn(err)
				time.Sleep(1 * time.Second)
			}
		}
		loader.Flush()
		_, err := db.Exec(fmt.Sprintf("DELETE FROM rpt_sdb_account_agent_trans_d2 limit %d", deleteNum))
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
		}
	}
}

func checkConsistency(tx *sql.Tx, threadId int, tso uint64, query string) bool {
	var totalTiFlash = -1
	var totalTiKV = -1
	var meetError = false
	_, err := tx.Query("set @@session.tidb_isolation_read_engines='tikv'")
	if err != nil {
		log.Warn(err)
	}
	err = tx.QueryRow(query).Scan(&totalTiKV)
	if err != nil {
		tx.Rollback()
		log.Warn(err)
		meetError = true
	}

	if !meetError {
		_, err = tx.Query("set @@session.tidb_isolation_read_engines='tiflash'")
		if err != nil {
			log.Warn(err)
		}
		err = tx.QueryRow(query).Scan(&totalTiFlash)
		if err != nil {
			tx.Rollback()
			log.Warn(err)
			meetError = true
		}
	}

	if !meetError && totalTiFlash != totalTiKV {
		fmt.Printf("tiflash result %d, tikv result %d is not consistent thread %d tso %d.\n", totalTiFlash, totalTiKV, threadId, tso)
		return false
	} else {
		fmt.Printf("tiflash result %d, tikv result %d thread %d tso %d\n", totalTiFlash, totalTiKV, threadId, tso)
		if !meetError {
			tx.Commit()
		}
	}
	return true
}

func verify(wg *sync.WaitGroup, tableName string, threadId int) {
	defer wg.Done()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/test", *address))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// disable batch cop
	_, err = db.Exec("set @@tidb_allow_batch_cop = 0;")
	if err != nil {
		panic(err)
	}

	query := fmt.Sprintf("select count(*) from %s", tableName)
	for {
		tx, err := db.Begin()
		if err != nil {
			panic(err)
		}
		var tso uint64
		if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
			panic(err)
		}

		if threadId%3 == 0 {
			interval := rand.Intn(5000)
			fmt.Printf("thread %d sleep %d millisecond\n", threadId, interval)
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}

		correct := checkConsistency(tx, threadId, tso, query)
		if !correct {
			fmt.Printf("thread %d meet wrong result, try to check again\n", threadId)
			checkConsistency(tx, threadId, tso, query)
			fmt.Println(time.Now().UTC())
			panic("test meet error")
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func main() {
	flag.Parse()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/test", *address))
	//db, err := sql.Open("mysql", "root@tcp(127.0.0.1:8000)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(time.Minute * 30)

	if *stable {
		fmt.Println("Run stable workload")
		var wg sync.WaitGroup

		for i := 0; i < *verify_thread_num; i++ {
			fmt.Println("Main: Starting verify worker", i)
			wg.Add(1)
			go verify(&wg, "rpt_sdb_account_agent_trans_d2", i)
		}
		if *recreate_stable_table {
			createStableTable(db)
		}

		for i := 0; i < *update_thread_num; i++ {
			fmt.Println("Main: Starting update worker", i)
			wg.Add(1)
			go stableUpdateTable(&wg)
		}

		fmt.Println("Main: Waiting for workers to finish")
		wg.Wait()
		fmt.Println("Main: Completed")
	} else {
		fmt.Println("Run insert workload")
		err = createTable(db)
		if err != nil {
			panic(err)
		}

		var wg sync.WaitGroup

		for i := 0; i < *verify_thread_num; i++ {
			fmt.Println("Main: Starting verify worker", i)
			wg.Add(1)
			go verify(&wg, "rpt_sdb_account_agent_trans_d", i)
		}

		for i := 0; i < *update_thread_num; i++ {
			fmt.Println("Main: Starting upate worker", i)
			wg.Add(1)
			go updateTable(&wg)
		}

		fmt.Println("Main: Waiting for workers to finish")
		wg.Wait()
		fmt.Println("Main: Completed")

	}
}
