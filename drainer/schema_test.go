package drainer

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func (t *testDrainerSuite) TestSchema(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	ignoreDBName := model.NewCIStr("ignoreTest")
	// db and ignoreDB info
	dbInfo := &model.DBInfo{
		ID:    1,
		Name:  dbName,
		State: model.StatePublic,
	}
	ingnoreDBInfo := &model.DBInfo{
		ID:    2,
		Name:  ignoreDBName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job := &model.Job{
		ID:       3,
		SchemaID: 1,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{123, dbInfo},
	}
	jobs = append(jobs, mustTranslateJob(c, job))
	// `createIgnoreSchema` job
	job1 := &model.Job{
		ID:       4,
		SchemaID: 2,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{123, ingnoreDBInfo},
	}
	jobs = append(jobs, mustTranslateJob(c, job1))
	// construct a cancelled job
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 5, State: model.JobCancelled}))
	// construct ignore db list
	ignoreNames := make(map[string]struct{})
	ignoreNames[ignoreDBName.L] = struct{}{}
	// reconstruct the local schema
	schema, err := NewSchema(jobs, ignoreNames)
	c.Assert(err, IsNil)
	// check ignore DB
	_, ok := schema.IgnoreSchemaByID(ingnoreDBInfo.ID)
	c.Assert(ok, IsTrue)
	// test drop schema and drop ignore schema
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 6, SchemaID: 1, Type: model.ActionDropSchema}))
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 7, SchemaID: 2, Type: model.ActionDropSchema}))
	_, err = NewSchema(jobs, ignoreNames)
	c.Assert(err, IsNil)
	// test create schema already exist error
	jobs = jobs[:0]
	jobs = append(jobs, mustTranslateJob(c, job))
	jobs = append(jobs, mustTranslateJob(c, job))
	_, err = NewSchema(jobs, ignoreNames)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)

	// test schema decodeArgs error
	jobs = jobs[:0]
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 8, SchemaID: 1, Args: []interface{}{123, 123}, Type: model.ActionCreateSchema}))
	_, err = NewSchema(jobs, ignoreNames)
	c.Assert(err, NotNil, Commentf("should return  schema decodeArgs error"))

	// test schema drop schema error
	jobs = jobs[:0]
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 9, SchemaID: 1, Type: model.ActionDropSchema}))
	_, err = NewSchema(jobs, ignoreNames)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (*testDrainerSuite) TestTable(c *C) {
	var jobs []*model.Job

	ignoreDBName := model.NewCIStr("ignoreTest")
	originjobs, dbInfo, tblInfo := testConstructJobs()
	for _, job := range originjobs {
		jobs = append(jobs, mustTranslateJob(c, job))
	}

	// construct ignore db list
	ignoreNames := make(map[string]struct{})
	ignoreNames[ignoreDBName.L] = struct{}{}
	// reconstruct the local schema
	schema, err := NewSchema(jobs, ignoreNames)
	c.Assert(err, IsNil)
	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := schema.SchemaByID(dbInfo.ID)
	c.Assert(ok, IsTrue)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(table.Columns, HasLen, 1)
	c.Assert(table.Indices, HasLen, 1)
	// check truncate table
	tblInfo.ID = 9
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 9, SchemaID: 3, TableID: 2, Type: model.ActionTruncateTable, Args: []interface{}{123, tblInfo}}))
	schema1, err := NewSchema(jobs, ignoreNames)
	c.Assert(err, IsNil)
	table, ok = schema1.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	table, ok = schema1.TableByID(2)
	c.Assert(ok, IsFalse)
	// check drop table
	jobs = append(jobs, mustTranslateJob(c, &model.Job{ID: 9, SchemaID: 3, TableID: 9, Type: model.ActionDropTable}))
	schema2, err := NewSchema(jobs, ignoreNames)
	c.Assert(err, IsNil)
	table, ok = schema2.TableByID(tblInfo.ID)
	c.Assert(ok, IsFalse)
	// test schemaAndTableName
	_, _, ok = schema1.SchemaAndTableName(9)
	c.Assert(ok, IsTrue)
	// drop schema
	_, err = schema1.DropSchema(3)
	c.Assert(err, IsNil)
	// test schema version
	c.Assert(schema.SchemaMetaVersion(), Equals, int64(0))
}

func testConstructJobs() ([]*model.Job, *model.DBInfo, *model.TableInfo) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")

	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}

	idxInfo := &model.IndexInfo{
		Name:  idxName,
		Table: tbName,
		Columns: []*model.IndexColumn{
			{
				Name:   colName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   model.StatePublic,
	}

	tblInfo := &model.TableInfo{
		ID:    2,
		Name:  tbName,
		State: model.StatePublic,
	}

	dbInfo := &model.DBInfo{
		ID:    3,
		Name:  dbName,
		State: model.StatePublic,
	}

	// `createSchema` job
	job := &model.Job{
		ID:       5,
		SchemaID: 3,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{123, dbInfo},
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &model.Job{
		ID:       6,
		SchemaID: 3,
		TableID:  2,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{123, tblInfo},
	}
	jobs = append(jobs, job)

	// `addColumn` job
	tblInfo.Columns = []*model.ColumnInfo{colInfo}
	job = &model.Job{
		ID:       7,
		SchemaID: 3,
		TableID:  2,
		Type:     model.ActionAddColumn,
		Args:     []interface{}{123, tblInfo},
	}
	jobs = append(jobs, job)

	// construct a historical `addIndex` job
	tblInfo.Indices = []*model.IndexInfo{idxInfo}
	job = &model.Job{
		ID:       8,
		SchemaID: 3,
		TableID:  2,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{123, tblInfo},
	}
	jobs = append(jobs, job)

	return jobs, dbInfo, tblInfo
}

func mustTranslateJob(c *C, job *model.Job) *model.Job {
	rawJob, err := job.Encode()
	c.Assert(err, IsNil)
	err = job.Decode(rawJob)
	c.Assert(err, IsNil)
	return job
}
