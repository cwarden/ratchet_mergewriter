package mergewriter

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/util"
	"github.com/golang-sql/sqlexp"
)

// SQLMergeWriter handles upserting data.JSON into a specified SQL table using
// MERGE INTO. If an error occurs while building or executing the MERGE INTO,
// the error will be sent to the killChan.
//
// Note that the data.JSON must be a valid JSON object or a slice of valid
// objects, where the keys are column names and the the values are the SQL
// values to be inserted into those columns.
type SQLMergeWriter struct {
	writeDB   *sql.DB
	TableName string
	KeyField  string
}

// NewSQLMergeWriter returns a new SQLMergeWriter
func NewSQLMergeWriter(db *sql.DB, tableName string, keyField string) *SQLMergeWriter {
	return &SQLMergeWriter{writeDB: db, TableName: tableName, KeyField: keyField}
}

// ProcessData defers to SQLMergeData
func (s *SQLMergeWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	logger.Info("SQLMergeWriter: Writing data...")
	err := SQLMergeData(s.writeDB, d, s.TableName, s.KeyField)
	util.KillPipelineIfErr(err, killChan)
	logger.Info("SQLMergeWriter: Write complete")
}

// Finish - see interface for documentation.
func (s *SQLMergeWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLMergeWriter) String() string {
	return "SQLMergeWriter"
}

func SQLMergeData(db *sql.DB, d data.JSON, tableName string, keyField string) error {
	objects, err := data.ObjectsFromJSON(d)
	if err != nil {
		return err
	}

	return mergeInto(db, objects, tableName, keyField)
}

func mergeInto(db *sql.DB, objects []map[string]interface{}, tableName string, keyField string) error {
	logger.Info("SQLMergeData: building MERGE INTO for len(objects) =", len(objects))

	cols := sortedColumns(objects)
	quoter, _ := sqlexp.QuoterFromDriver(db.Driver(), context.TODO())

	mergeSQL := buildMergeSQL(cols, len(objects), tableName, keyField, quoter)
	vals := buildVals(cols, objects)

	logger.Debug("SQLMergeData:", mergeSQL)
	logger.Debug("SQLMergeData: values", vals)

	stmt, err := db.Prepare(mergeSQL)
	if err != nil {
		logger.Debug("SQLMergeData: error preparing SQL")
		return err
	}
	defer stmt.Close()

	logger.Debug("SQLMergeData: executing")
	res, err := stmt.Exec(vals...)
	if err != nil {
		logger.Error("SQLMergeData: failed to execute prepared statement")
		return err
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("SQLMergeData: rows affected = %d", rowCnt))
	return nil
}

func buildMergeSQL(cols []string, rowCount int, tableName string, keyField string, quoter sqlexp.Quoter) (mergeSQL string) {
	quotedCols := quoteCols(cols, quoter)
	var updates, sourceCols []string
	for _, col := range quotedCols {
		updates = append(updates, fmt.Sprintf("target.%[1]s = source.%[1]s", col))
		sourceCols = append(sourceCols, fmt.Sprintf("source.%s", col))
	}
	mergeOn := fmt.Sprintf("target.%[1]s = source.%[1]s", quoter.ID(keyField))
	mergeSQL = fmt.Sprintf(`
		MERGE
			%s AS target
		USING (VALUES %s) AS source (%s)
		ON
			%s
		WHEN MATCHED THEN UPDATE SET
			%s
		WHEN NOT MATCHED THEN
			INSERT(%[3]s)
			VALUES(%[6]s);`,
		quoter.ID(tableName), buildParameters(len(cols), rowCount), strings.Join(quotedCols, ","), mergeOn, strings.Join(updates, ","), strings.Join(sourceCols, ","))
	return
}

func quoteCols(columns []string, quoter sqlexp.Quoter) []string {
	var quotedCols []string
	for _, col := range columns {
		quotedCols = append(quotedCols, quoter.ID(col))
	}
	return quotedCols
}

func sortedColumns(objects []map[string]interface{}) []string {
	// Since we don't know if all objects have the same keys, we need to
	// iterate over all the objects to gather all possible keys/columns
	// to use in the INSERT statement.
	colsMap := make(map[string]struct{})
	for _, o := range objects {
		for col := range o {
			colsMap[col] = struct{}{}
		}
	}

	cols := []string{}
	for col := range colsMap {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

// builds the (?,?) part
func buildParameters(columns int, rows int) string {
	var parametersSQL string
	qs := "("
	for i := 0; i < columns; i++ {
		if i > 0 {
			qs += ","
		}
		qs += "?"
	}
	qs += ")"
	// append as many (?,?) parts as there are objects to insert
	for i := 0; i < rows; i++ {
		if i > 0 {
			parametersSQL += ","
		}
		parametersSQL += qs
	}
	return parametersSQL
}

func buildVals(cols []string, objects []map[string]interface{}) []interface{} {
	vals := []interface{}{}
	for _, obj := range objects {
		for _, col := range cols {
			if val, ok := obj[col]; ok {
				vals = append(vals, val)
			} else {
				vals = append(vals, nil)
			}
		}
	}
	return vals
}
