package dailytest

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

// test different data type of mysql
// mysql will change boolean to tinybit(1)
var case1 = []string{`
create table  binlog_case1 (
	id int auto_increment,
	t_boolean boolean,
	t_bigint BIGINT,
	t_double DOUBLE,
	t_decimal DECIMAL(38,19),
	t_bit BIT(64),
	t_date DATE,
	t_datetime DATETIME,
	t_timestamp TIMESTAMP NULL,
	t_time TIME,
	t_year YEAR,
	t_char CHAR,
	t_varchar VARCHAR(10),
	t_blob BLOB,
	t_text TEXT,
	t_enum ENUM('enum1', 'enum2', 'enum3'),
	t_set SET('a', 'b', 'c'),
	t_json JSON,
	primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
`,
	`
insert into binlog_case1(t_boolean, t_bigint, t_double, t_decimal, t_bit
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum
	,t_set, t_json) values
	(true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2'
	,'a,b', NULL);
`,
	`
insert into binlog_case1(t_boolean) values(TRUE);
`,
	`
insert into binlog_case1(t_boolean) values(FALSE);
`,
	// minmum value of bigint
	`
insert into binlog_case1(t_bigint) values(-9223372036854775808);
`,
	// maximum value of bigint
	`
insert into binlog_case1(t_bigint) values(9223372036854775807);
`,
	`
insert into binlog_case1(t_json) values('{"key1": "value1", "key2": "value2"}');
`,
}

var case1Clean = []string{`
	drop table binlog_case1`,
}

// https://internal.pingcap.net/jira/browse/TOOL-714
var case2 = []string{`
create table binlog_case2 (id int, a1 int, a3 int, unique key dex1(a1, a3));
`,
	`
insert into binlog_case2(id, a1, a3) values(1, 1, NULL);
`,
	`
insert into binlog_case2(id, a1, a3) values(2, 1, NULL);
`,
	`
update binlog_case2 set id = 10 where id = 1;
`,
	`
update binlog_case2 set id = 100 where id = 10;
`,
}

var case2Clean = []string{`
	drop table binlog_case2`,
}

var case3 = []string{`
create table a(id int primary key, a1 int);
`,
	`
insert into a(id, a1) values(1,1),(2,1);
`,
	`
alter table a add unique index aidx(a1);
`,
}

var case3Clean = []string{`
	drop table a`,
}

type testRunner struct {
	src    *sql.DB
	dst    *sql.DB
	schema string
}

func (tr *testRunner) run(test func(*sql.DB)) {
	RunTest(tr.src, tr.dst, tr.schema, test)
}

func (tr *testRunner) execSQLs(sqls []string) {
	RunTest(tr.src, tr.dst, tr.schema, func(src *sql.DB) {
		err := execSQLs(tr.src, sqls)
		if err != nil {
			log.Fatal(err)
		}
	})
}

// RunCase run some simple test case
func RunCase(src *sql.DB, dst *sql.DB, schema string) {
	tr := &testRunner{src: src, dst: dst, schema: schema}
	runPKcases(tr)

	tr.execSQLs(case1)
	tr.execSQLs(case1Clean)

	tr.execSQLs(case2)
	tr.execSQLs(case2Clean)

	// run case3
	tr.run(func(src *sql.DB) {
		err := execSQLs(src, case3)
		if err != nil && !strings.Contains(err.Error(), "Duplicate for key") {
			log.Fatal(err)
		}
	})
	tr.execSQLs(case3Clean)

	// random op on have both pk and uk table
	RunTest(src, dst, schema, func(src *sql.DB) {
		start := time.Now()

		err := updatePKUK(src, 1000)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		log.Info(" updatePKUK take: ", time.Since(start))
	})

	// swap unique index value
	RunTest(src, dst, schema, func(src *sql.DB) {
		_, err := src.Exec("create table uindex(id int primary key, a1 int unique)")
		if err != nil {
			log.Fatal(err)
		}

		_, err = src.Exec("insert into uindex(id, a1) values(1, 10),(2,20)")
		if err != nil {
			log.Fatal(err)
		}

		tx, err := src.Begin()
		if err != nil {
			log.Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 30 where id = 1")
		if err != nil {
			log.Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 10 where id = 2")
		if err != nil {
			log.Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 20 where id = 1")
		if err != nil {
			log.Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}

		_, err = src.Exec("drop table uindex")
		if err != nil {
			log.Fatal(err)
		}
	})

	// test big binlog msg
	RunTest(src, dst, schema, func(src *sql.DB) {
		_, err := src.Query("create table binlog_big(id int primary key, data longtext);")
		if err != nil {
			log.Fatal(err)
		}

		tx, err := src.Begin()
		if err != nil {
			log.Fatal(err)
		}
		// insert 50 * 1M
		// note limitation of TiDB: https://github.com/pingcap/docs/blob/733a5b0284e70c5b4d22b93a818210a3f6fbb5a0/FAQ.md#the-error-message-transaction-too-large-is-displayed
		var data = make([]byte, 1<<20)
		for i := 0; i < 50; i++ {
			_, err = tx.Query("insert into binlog_big(id, data) values(?, ?);", i, data)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
	})
	// clean table
	RunTest(src, dst, schema, func(src *sql.DB) {
		_, err := src.Query("drop table binlog_big;")
		if err != nil {
			log.Fatal(err)
		}
	})
}

// updatePKUK create a table with primary key and unique key
// then do opNum randomly DML
func updatePKUK(db *sql.DB, opNum int) error {
	maxKey := 20
	_, err := db.Exec("create table pkuk(pk int primary key, uk int, v int, unique key uk(uk));")
	if err != nil {
		return errors.Trace(err)
	}

	var pks []int
	addPK := func(pk int) {
		pks = append(pks, pk)
	}
	removePK := func(pk int) {
		var tmp []int
		for _, v := range pks {
			if v != pk {
				tmp = append(tmp, v)
			}
		}
		pks = tmp
	}
	hasPK := func(pk int) bool {
		for _, v := range pks {
			if v == pk {
				return true
			}
		}
		return false
	}

	for i := 0; i < opNum; {
		var sql string
		pk := rand.Intn(maxKey)
		uk := rand.Intn(maxKey)
		v := rand.Intn(10000)
		oldPK := rand.Intn(maxKey)

		// try randomly insert&update&delete
		op := rand.Intn(3)
		switch op {
		case 0:
			if len(pks) == maxKey {
				continue
			}
			for hasPK(pk) {
				log.Info(pks)
				pk = rand.Intn(maxKey)
			}
			sql = fmt.Sprintf("insert into pkuk(pk, uk, v) values(%d,%d,%d)", pk, uk, v)
		case 1:
			if len(pks) == 0 {
				continue
			}
			for !hasPK(oldPK) {
				log.Info(pks)
				oldPK = rand.Intn(maxKey)
			}
			sql = fmt.Sprintf("update pkuk set pk = %d, uk = %d, v = %d where pk = %d", pk, uk, v, oldPK)
		case 2:
			if len(pks) == 0 {
				continue
			}
			for !hasPK(pk) {
				log.Info(pks)
				pk = rand.Intn(maxKey)
			}
			sql = fmt.Sprintf("delete from pkuk where pk = %d", pk)
		}

		_, err := db.Exec(sql)
		if err != nil {
			// for insert and update, we didn't check for uk's duplicate
			if strings.Contains(err.Error(), "Duplicate entry") {
				continue
			}
			return errors.Trace(err)
		}

		switch op {
		case 0:
			addPK(pk)
		case 1:
			removePK(oldPK)
			addPK(pk)
		case 2:
			removePK(pk)
		}
		i++
	}

	_, err = db.Exec("drop table pkuk")
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// create a table with one column id with different type
// test the case whether it is primary key too, this can
// also help test when the column is handle or not.
func runPKcases(tr *testRunner) {
	cases := []struct {
		Tp     string
		Value  interface{}
		Update interface{}
	}{
		{
			Tp:     "BIGINT UNSIGNED",
			Value:  uint64(math.MaxUint64),
			Update: uint64(math.MaxUint64) - 1,
		},
		{
			Tp:     "BIGINT SIGNED",
			Value:  int64(math.MaxInt64),
			Update: int64(math.MaxInt64) - 1,
		},
		{
			Tp:     "INT UNSIGNED",
			Value:  uint32(math.MaxUint32),
			Update: uint32(math.MaxUint32) - 1,
		},
		{
			Tp:     "INT SIGNED",
			Value:  int32(math.MaxInt32),
			Update: int32(math.MaxInt32) - 1,
		},
		{
			Tp:     "SMALLINT UNSIGNED",
			Value:  uint16(math.MaxUint16),
			Update: uint16(math.MaxUint16) - 1,
		},
		{
			Tp:     "SMALLINT SIGNED",
			Value:  int16(math.MaxInt16),
			Update: int16(math.MaxInt16) - 1,
		},
		{
			Tp:     "TINYINT UNSIGNED",
			Value:  uint8(math.MaxUint8),
			Update: uint8(math.MaxUint8) - 1,
		},
		{
			Tp:     "TINYINT SIGNED",
			Value:  int8(math.MaxInt8),
			Update: int8(math.MaxInt8) - 1,
		},
	}

	for _, c := range cases {
		for _, ispk := range []string{"", "PRIMARY KEY"} {

			tr.run(func(src *sql.DB) {
				sql := fmt.Sprintf("CREATE TABLE pk(id %s %s)", c.Tp, ispk)
				mustExec(src, sql)

				sql = "INSERT INTO pk(id) values( ? )"
				mustExec(src, sql, c.Value)

				if len(ispk) == 0 {
					// insert a null value
					mustExec(src, sql, nil)
				}

				sql = "UPDATE pk set id = ? where id = ?"
				mustExec(src, sql, c.Update, c.Value)

				sql = "DELETE from pk where id = ?"
				mustExec(src, sql, c.Update)
			})

			tr.execSQLs([]string{"DROP TABLE pk"})
		}
	}
}

func mustExec(db *sql.DB, sql string, args ...interface{}) {
	_, err := db.Exec(sql, args...)
	if err != nil {
		log.Fatalf("exec failed, sql: %s args: %v, err: %+v", sql, args, err)
	}
}
