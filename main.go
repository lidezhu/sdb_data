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
var thread = flag.Int("thread", 32, "update thread")
var update = flag.Bool("update", true, "update or verify the data")
var replica = flag.Int("replica", 2, "tiflash replica num")
var schema = flag.String("schema", "", "schema file path")
var stable = flag.Bool("stable", false, "run stable workload")

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

const maxBatchCount = 512

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

	if b.count >= maxBatchCount {
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

func createStableTable(db *sql.DB) {
	_, err := db.Query("Drop table if exists rpt_sdb_account_agent_trans_d2")
	if err != nil {
		panic(err)
	}
	_, err = db.Query("Create table rpt_sdb_account_agent_trans_d2 like rpt_sdb_account_agent_trans_d")
	if err != nil {
		panic(err)
	}
	loader := NewSQLBatchLoader(db, "INSERT INTO rpt_sdb_account_agent_trans_d2 ")
	for i := 0; i < 600000; i += 1 {
		v := fmt.Sprintf("('%s','%s','%s','%s','%s',%d,%d,%f,%d,%d,%d,%d,%d,%f,%d,%f,%d,%f,%f,%d,%d,%d,'%s','%s','%s','%s') WHERE ",
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
	for {
		updateSql := fmt.Sprintf("update rpt_sdb_account_agent_trans_d2 set agent_name='%s',"+
			"channel='%s',"+
			"sub_channel='%s"+
			"advertiser_id='%s'"+
			"account='%s'"+
			"shows=%d"+
			"click=%d"+
			"cost=%f"+
			"landing_uv=%d"+
			"free_insur_user_num=%d"+
			"submit_user_num=%d"+
			"succ_user_num=%d"+
			"succ_order_num=%d"+
			"channel_origin=%f"+
			"new_user_num=%d"+
			"new_channel_origin=%f"+
			"repay_user_num=%d"+
			"repay_origin=%f"+
			"origin=%f"+
			"refund_order_num=%d"+
			"hand_pay_user_num=%d"+
			"follow_user_num=%d"+
			"types='%s'"+
			"biz='%s'"+
			"media_code='%s'"+
			"dt='%s'"+
			"where click=%d",
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
			randString(100),
			randInt(10000))
		_, err = db.Query(updateSql)
		if err != nil {
			panic(err)
		}
	}
}

func verify(wg *sync.WaitGroup) {
	defer wg.Done()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/test", *address))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// disable batch cop
	_, err = db.Query("set @@tidb_allow_batch_cop = 0;")
	if err != nil {
		panic(err)
	}

	query := "select count(*) from rpt_sdb_account_agent_trans_d"
	for {
		tx, err := db.Begin()
		if err != nil {
			panic(err)
		}
		var totalTiFlash = -1
		var totalTiKV = -2
		_, err = tx.Query("set @@session.tidb_isolation_read_engines='tiflash'")
		if err != nil {
			panic(err)
		}
		err = tx.QueryRow(query).Scan(&totalTiFlash)
		if err != nil {
			tx.Rollback()
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		_, err = tx.Query("set @@session.tidb_isolation_read_engines='tikv'")
		if err != nil {
			panic(err)
		}
		err = tx.QueryRow(query).Scan(&totalTiKV)
		if err != nil {
			tx.Rollback()
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		tx.Commit()
		fmt.Printf("tiflash result %d, tikv result %d\n", totalTiFlash, totalTiKV)
		if totalTiFlash != totalTiKV {
			fmt.Printf("tiflash result %d, tikv result %d is not consisten\n", totalTiFlash, totalTiKV)
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
		createStableTable(db)
		var wg sync.WaitGroup

		for i := 0; i < *thread; i++ {
			fmt.Println("Main: Starting worker", i)
			wg.Add(1)
			go stableUpdateTable(&wg)
		}
		fmt.Println("Main: Waiting for workers to finish")
		wg.Wait()
		fmt.Println("Main: Completed")
	} else {
		if *update {
			err = createTable(db)
			if err != nil {
				panic(err)
			}

			var wg sync.WaitGroup

			for i := 0; i < *thread; i++ {
				fmt.Println("Main: Starting worker", i)
				wg.Add(1)
				go updateTable(&wg)
			}
			fmt.Println("Main: Waiting for workers to finish")
			wg.Wait()
			fmt.Println("Main: Completed")
		} else {
			fmt.Println("begin to verify")
			var wg sync.WaitGroup

			for i := 0; i < *thread; i++ {
				fmt.Println("Main: Starting worker", i)
				wg.Add(1)
				go verify(&wg)
			}
			fmt.Println("Main: Waiting for workers to finish")
			wg.Wait()
			fmt.Println("Main: Completed")
		}
	}
}
