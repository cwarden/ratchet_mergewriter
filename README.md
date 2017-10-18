SQLMergeWriter Ratchet Processor
================================

A [ratchet](https://github.com/dailyburn/ratchet) processor to upsert data to a
SQL database using MERGE.  When a row already exists in the `targetTable` with
a matching `keyField`, the row will be updated; otherwise, a new row will be
inserted.

It is based on the
[SQLWriter](https://github.com/dailyburn/ratchet/blob/develop/processors/sql_writer.go)
processor, but is for use with databases that support `MERGE` rather than `ON
DUPLICATE KEY UPDATE`.

Usage
=====

```
import (
	merge "github.com/cwarden/ratchet_mergewriter"
)


db, err := sql.Open("mssql", "sqlserver://username:password@server:port?database=dbname")
pipeline := ratchet.NewPipeline(..., merge.NewSQLMergeWriter(db, targetTable, keyField))
```
