package executor

import (
	"database/sql"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// QueryHistogramVec get the sql query time
var QueryHistogramVec *prometheus.HistogramVec

type mysqlExecutor struct {
	db *sql.DB
	*baseError
}

func newMysql(cfg *DBConfig, sqlMode *string) (Executor, error) {
	db, err := pkgsql.OpenDBWithSQLMode("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password, sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mysqlExecutor{
		db:        db,
		baseError: newBaseError(),
	}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	// len(args[0]) > 0 -> isDDL
	if len(args) > 0 && len(args[0]) > 0 {
		schema := args[0][0]
		useSql := fmt.Sprintf("use %s;", schema)
		sqls = append([]string{useSql}, sqls...)
		args = append(args, args...)
	}
	return pkgsql.ExecuteSQLsWithHistogram(m.db, sqls, args, isDDL, QueryHistogramVec)
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
