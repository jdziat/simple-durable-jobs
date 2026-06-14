package storage

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

const nilUUIDString = "00000000-0000-0000-0000-000000000000"
const mysqlUUIDBinaryCompletionMarkerTable = "sdj_uuid_binary_migration"
const mysqlUUIDBinaryCompletionMarkerName = "uuid_binary_v3_complete"

type legacyUUIDColumn struct {
	table    string
	column   string
	nullable bool
	sentinel bool
	primary  bool
}

type mysqlConvertedCompositeIndex struct {
	model   any
	table   string
	name    string
	unique  bool
	columns []string
}

type mysqlCompositeIndexRecreateStats struct {
	dropped int
	created int
}

var legacyUUIDColumns = []legacyUUIDColumn{
	{table: "jobs", column: "id", primary: true},
	{table: "jobs", column: "parent_job_id", nullable: true},
	{table: "jobs", column: "root_job_id", nullable: true},
	{table: "jobs", column: "fan_out_id", nullable: true},
	{table: "checkpoints", column: "id", primary: true},
	{table: "checkpoints", column: "job_id"},
	{table: "signals", column: "id", primary: true},
	{table: "signals", column: "job_id"},
	{table: "fan_outs", column: "id", primary: true},
	{table: "fan_outs", column: "parent_job_id"},
	{table: "concurrency_slots", column: "job_id", sentinel: true, primary: true},
	{table: "unique_locks", column: "job_id"},
}

var mysqlConvertedCompositeIndexes = []mysqlConvertedCompositeIndex{
	{model: &core.Checkpoint{}, table: "checkpoints", name: "idx_checkpoints_job_call", unique: true, columns: []string{"job_id", "call_index", "call_type"}},
	{model: &core.Signal{}, table: "signals", name: "idx_signals_pending", columns: []string{"job_id", "name", "consumed_at", "created_at"}},
	{model: &core.Signal{}, table: "signals", name: "idx_signals_consumed_at", columns: []string{"consumed_at", "id"}},
	{model: &core.Job{}, table: "jobs", name: "idx_jobs_fan_out_status", columns: []string{"fan_out_id", "status"}},
	{model: &core.Job{}, table: "jobs", name: "idx_jobs_retention_terminal", columns: []string{"status", "completed_at", "id"}},
	{model: &core.FanOut{}, table: "fan_outs", name: "idx_fan_outs_parent_status", columns: []string{"parent_job_id", "status"}},
}

func convertLegacyStringUUIDColumns(ctx context.Context, db *gorm.DB, dialect string) error {
	if !db.Migrator().HasTable(&core.Job{}) {
		return nil
	}

	switch dialect {
	case dialectPostgres:
		return convertPostgresLegacyStringUUIDColumns(ctx, db)
	case dialectMySQL:
		return convertMySQLLegacyStringUUIDColumns(ctx, db)
	default:
		return convertSQLiteLegacyStringUUIDColumns(ctx, db)
	}
}

func convertPostgresLegacyStringUUIDColumns(ctx context.Context, db *gorm.DB) error {
	columnType, err := postgresColumnDataType(ctx, db, "jobs", "id")
	if err != nil {
		return fmt.Errorf("inspect jobs.id type: %w", err)
	}
	switch columnType {
	case "uuid":
		return nil
	case "character varying", "character", "text":
	default:
		return nil
	}

	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, stmt := range []string{
			"ALTER TABLE checkpoints DROP CONSTRAINT IF EXISTS fk_checkpoints_job",
			"ALTER TABLE fan_outs DROP CONSTRAINT IF EXISTS fk_fanouts_parent",
			"ALTER TABLE signals DROP CONSTRAINT IF EXISTS fk_signals_job",
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}

		for _, col := range legacyUUIDColumns {
			if !tx.Migrator().HasTable(col.table) || !tx.Migrator().HasColumn(col.table, col.column) {
				continue
			}
			expr := fmt.Sprintf("%s::uuid", col.column)
			if col.nullable {
				expr = fmt.Sprintf("NULLIF(%s, '')::uuid", col.column)
			}
			if col.sentinel {
				expr = fmt.Sprintf("(CASE WHEN %s = '' THEN '%s' ELSE %s END)::uuid", col.column, nilUUIDString, col.column)
			}
			stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE uuid USING %s", col.table, col.column, expr)
			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("convert %s.%s: %w", col.table, col.column, err)
			}
		}

		for _, stmt := range []string{
			"ALTER TABLE checkpoints ADD CONSTRAINT fk_checkpoints_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE",
			"ALTER TABLE fan_outs ADD CONSTRAINT fk_fanouts_parent FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE CASCADE",
			"ALTER TABLE signals ADD CONSTRAINT fk_signals_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE",
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func postgresColumnDataType(ctx context.Context, db *gorm.DB, table, column string) (string, error) {
	var dataType sql.NullString
	err := db.WithContext(ctx).Raw(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = CURRENT_SCHEMA()
		  AND table_name = ?
		  AND column_name = ?
	`, table, column).Scan(&dataType).Error
	if err != nil || !dataType.Valid {
		return "", err
	}
	return strings.ToLower(dataType.String), nil
}

func convertMySQLLegacyStringUUIDColumns(ctx context.Context, db *gorm.DB) error {
	needsRun, err := mysqlLegacyUUIDConversionNeedsRun(ctx, db)
	if err != nil {
		return err
	}
	if !needsRun {
		return nil
	}

	columnsNeedConversion, err := mysqlLegacyUUIDColumnsNeedConversion(ctx, db)
	if err != nil {
		return err
	}
	if columnsNeedConversion {
		for _, indexName := range []string{"idx_jobs_parent_nonterminal", "idx_jobs_root_nonterminal"} {
			if db.Migrator().HasIndex(&core.Job{}, indexName) {
				if err := db.WithContext(ctx).Exec("DROP INDEX " + indexName + " ON jobs").Error; err != nil && !isBenignDDLError(err) {
					return fmt.Errorf("drop %s: %w", indexName, err)
				}
			}
		}
		for _, name := range []string{"pending_parent_ref", "pending_root_ref"} {
			if db.Migrator().HasColumn(&core.Job{}, name) {
				if err := db.WithContext(ctx).Exec("ALTER TABLE jobs DROP COLUMN " + name).Error; err != nil && !isBenignDDLError(err) {
					return fmt.Errorf("drop %s: %w", name, err)
				}
			}
		}
		for _, fk := range []struct {
			table string
			name  string
		}{
			{table: "checkpoints", name: "fk_checkpoints_job"},
			{table: "fan_outs", name: "fk_fanouts_parent"},
			{table: "signals", name: "fk_signals_job"},
		} {
			if err := db.WithContext(ctx).Exec("ALTER TABLE " + fk.table + " DROP FOREIGN KEY " + fk.name).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop %s: %w", fk.name, err)
			}
		}

		for _, col := range mysqlLegacyUUIDColumnsInConversionOrder() {
			if !db.Migrator().HasTable(col.table) {
				continue
			}
			if err := convertMySQLLegacyUUIDColumn(ctx, db, col); err != nil {
				return err
			}
		}
	}

	if err := ensureMySQLRetentionWorkflowGeneratedColumns(ctx, db); err != nil {
		return err
	}
	if _, err := recreateMySQLConvertedCompositeIndexes(ctx, db); err != nil {
		return err
	}
	if err := ensureMySQLConvertedForeignKeys(ctx, db); err != nil {
		return err
	}
	if err := markMySQLUUIDBinaryConversionComplete(ctx, db); err != nil {
		return err
	}
	return nil
}

func mysqlLegacyUUIDColumnsInConversionOrder() []legacyUUIDColumn {
	ordered := make([]legacyUUIDColumn, 0, len(legacyUUIDColumns))
	var jobsID *legacyUUIDColumn
	for _, col := range legacyUUIDColumns {
		if col.table == "jobs" && col.column == "id" {
			c := col
			jobsID = &c
			continue
		}
		ordered = append(ordered, col)
	}
	if jobsID != nil {
		ordered = append(ordered, *jobsID)
	}
	return ordered
}

func mysqlLegacyUUIDConversionNeedsRun(ctx context.Context, db *gorm.DB) (bool, error) {
	complete, err := mysqlUUIDBinaryConversionComplete(ctx, db)
	if err != nil || complete {
		return false, err
	}
	return true, nil
}

func mysqlLegacyUUIDColumnsNeedConversion(ctx context.Context, db *gorm.DB) (bool, error) {
	for _, col := range legacyUUIDColumns {
		if !db.Migrator().HasTable(col.table) {
			continue
		}
		if db.Migrator().HasColumn(col.table, col.column+"_uuid_tmp") {
			return true, nil
		}
		if !db.Migrator().HasColumn(col.table, col.column) {
			continue
		}
		columnType, err := mysqlUUIDColumnDataType(ctx, db, col.table, col.column)
		if err != nil {
			return false, fmt.Errorf("inspect %s.%s type: %w", col.table, col.column, err)
		}
		if columnType == "varchar" || columnType == "char" {
			return true, nil
		}
	}
	return false, nil
}

func mysqlUUIDBinaryConversionComplete(ctx context.Context, db *gorm.DB) (bool, error) {
	if !db.Migrator().HasTable(mysqlUUIDBinaryCompletionMarkerTable) {
		return false, nil
	}
	var count int64
	if err := db.WithContext(ctx).Table(mysqlUUIDBinaryCompletionMarkerTable).
		Where("name = ?", mysqlUUIDBinaryCompletionMarkerName).
		Count(&count).Error; err != nil {
		return false, fmt.Errorf("read MySQL UUID binary completion marker: %w", err)
	}
	return count > 0, nil
}

func markMySQLUUIDBinaryConversionComplete(ctx context.Context, db *gorm.DB) error {
	if err := db.WithContext(ctx).Exec(
		"CREATE TABLE IF NOT EXISTS " + mysqlUUIDBinaryCompletionMarkerTable + " (" +
			"name varchar(64) NOT NULL PRIMARY KEY, " +
			"completed_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)" +
			")",
	).Error; err != nil {
		return fmt.Errorf("create MySQL UUID binary completion marker table: %w", err)
	}
	if err := db.WithContext(ctx).Exec(
		"INSERT IGNORE INTO "+mysqlUUIDBinaryCompletionMarkerTable+" (name) VALUES (?)",
		mysqlUUIDBinaryCompletionMarkerName,
	).Error; err != nil {
		return fmt.Errorf("write MySQL UUID binary completion marker: %w", err)
	}
	return nil
}

func recreateMySQLConvertedCompositeIndexes(ctx context.Context, db *gorm.DB) (mysqlCompositeIndexRecreateStats, error) {
	var stats mysqlCompositeIndexRecreateStats
	m := db.Migrator()
	for _, spec := range mysqlConvertedCompositeIndexes {
		matches, err := mysqlConvertedCompositeIndexMatches(db, spec)
		if err != nil {
			return stats, err
		}
		if matches {
			continue
		}
		if m.HasIndex(spec.model, spec.name) {
			if err := m.DropIndex(spec.model, spec.name); err != nil && !isBenignDDLError(err) {
				return stats, fmt.Errorf("drop %s: %w", spec.name, err)
			}
			stats.dropped++
		}
		if m.HasIndex(spec.model, spec.name) {
			continue
		}
		unique := ""
		if spec.unique {
			unique = "UNIQUE "
		}
		stmt := fmt.Sprintf(
			"CREATE %sINDEX %s ON %s (%s)",
			unique,
			spec.name,
			spec.table,
			strings.Join(spec.columns, ", "),
		)
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil && !isBenignDDLError(err) {
			return stats, fmt.Errorf("create %s: %w", spec.name, err)
		}
		stats.created++
	}
	return stats, nil
}

func mysqlConvertedCompositeIndexMatches(db *gorm.DB, spec mysqlConvertedCompositeIndex) (bool, error) {
	indexes, err := db.Migrator().GetIndexes(spec.model)
	if err != nil {
		return false, fmt.Errorf("inspect indexes for %s: %w", spec.table, err)
	}
	for _, index := range indexes {
		if index.Name() != spec.name {
			continue
		}
		unique, ok := index.Unique()
		return ok && unique == spec.unique && reflect.DeepEqual(index.Columns(), spec.columns), nil
	}
	return false, nil
}

func ensureMySQLConvertedForeignKeys(ctx context.Context, db *gorm.DB) error {
	for _, fk := range []struct {
		model any
		table string
		name  string
		col   string
	}{
		{model: &core.Checkpoint{}, table: "checkpoints", name: "fk_checkpoints_job", col: "job_id"},
		{model: &core.FanOut{}, table: "fan_outs", name: "fk_fanouts_parent", col: "parent_job_id"},
		{model: &core.Signal{}, table: "signals", name: "fk_signals_job", col: "job_id"},
	} {
		if db.Migrator().HasConstraint(fk.model, fk.name) {
			continue
		}
		stmt := fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES jobs(id) ON DELETE CASCADE",
			fk.table,
			fk.name,
			fk.col,
		)
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("add %s: %w", fk.name, err)
		}
	}
	return nil
}

func ensureMySQLRetentionWorkflowGeneratedColumns(ctx context.Context, db *gorm.DB) error {
	for _, spec := range []struct {
		name      string
		refColumn string
		indexName string
	}{
		{name: "pending_parent_ref", refColumn: "parent_job_id", indexName: "idx_jobs_parent_nonterminal"},
		{name: "pending_root_ref", refColumn: "root_job_id", indexName: "idx_jobs_root_nonterminal"},
	} {
		correct, err := mysqlRetentionGeneratedColumnCorrect(ctx, db, spec.name, spec.refColumn)
		if err != nil {
			return err
		}
		if correct {
			continue
		}
		if db.Migrator().HasIndex(&core.Job{}, spec.indexName) {
			if err := db.WithContext(ctx).Exec("DROP INDEX " + spec.indexName + " ON jobs").Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop %s: %w", spec.indexName, err)
			}
		}
		if db.Migrator().HasColumn(&core.Job{}, spec.name) {
			if err := db.WithContext(ctx).Exec("ALTER TABLE jobs DROP COLUMN " + spec.name).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop %s: %w", spec.name, err)
			}
		}
	}
	return migrateMySQLRetentionWorkflowGeneratedColumns(ctx, db)
}

func mysqlRetentionGeneratedColumnCorrect(ctx context.Context, db *gorm.DB, column, refColumn string) (bool, error) {
	if !db.Migrator().HasColumn(&core.Job{}, column) {
		return false, nil
	}
	var row struct {
		DataType             sql.NullString `gorm:"column:DATA_TYPE"`
		CharacterMaxLength   sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
		GenerationExpression sql.NullString `gorm:"column:GENERATION_EXPRESSION"`
		Extra                sql.NullString `gorm:"column:EXTRA"`
	}
	if err := db.WithContext(ctx).Raw(`
		SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, GENERATION_EXPRESSION, EXTRA
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'jobs'
		  AND COLUMN_NAME = ?
	`, column).Scan(&row).Error; err != nil {
		return false, fmt.Errorf("inspect jobs.%s generated column: %w", column, err)
	}
	if !row.DataType.Valid || strings.ToLower(row.DataType.String) != "binary" {
		return false, nil
	}
	if !row.CharacterMaxLength.Valid || row.CharacterMaxLength.Int64 != 16 {
		return false, nil
	}
	if !row.GenerationExpression.Valid || !row.Extra.Valid {
		return false, nil
	}
	expr := strings.ToLower(row.GenerationExpression.String)
	extra := strings.ToLower(row.Extra.String)
	return strings.Contains(expr, strings.ToLower(refColumn)) &&
		strings.Contains(expr, "status") &&
		strings.Contains(expr, "completed") &&
		strings.Contains(expr, "failed") &&
		strings.Contains(expr, "cancelled") &&
		strings.Contains(extra, "stored") &&
		strings.Contains(extra, "generated"), nil
}

func mysqlUUIDColumnDataType(ctx context.Context, db *gorm.DB, table, column string) (string, error) {
	var dataType sql.NullString
	err := db.WithContext(ctx).Raw(`
		SELECT DATA_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME = ?
	`, table, column).Scan(&dataType).Error
	if err != nil || !dataType.Valid {
		return "", err
	}
	return strings.ToLower(dataType.String), nil
}

func convertMySQLLegacyUUIDColumn(ctx context.Context, db *gorm.DB, col legacyUUIDColumn) error {
	tmp := col.column + "_uuid_tmp"
	hasColumn := db.Migrator().HasColumn(col.table, col.column)
	hasTemp := db.Migrator().HasColumn(col.table, tmp)
	if !hasColumn && !hasTemp {
		return nil
	}
	if !hasColumn {
		if err := renameMySQLUUIDTempColumn(ctx, db, col, tmp); err != nil {
			return err
		}
		return restoreMySQLUUIDColumnPrimaryKey(ctx, db, col)
	}

	currentType, err := mysqlUUIDColumnDataType(ctx, db, col.table, col.column)
	if err != nil {
		return fmt.Errorf("inspect %s.%s type: %w", col.table, col.column, err)
	}
	if currentType == "binary" {
		if hasTemp {
			if err := db.WithContext(ctx).Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", col.table, tmp)).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("drop %s.%s: %w", col.table, tmp, err)
			}
		}
		return restoreMySQLUUIDColumnPrimaryKey(ctx, db, col)
	}
	if currentType != "varchar" && currentType != "char" {
		return nil
	}
	if !hasTemp {
		if err := db.WithContext(ctx).Exec(fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s binary(16) NULL",
			col.table, tmp,
		)).Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("add %s.%s: %w", col.table, tmp, err)
		}
	}

	expr := fmt.Sprintf("CASE WHEN %s IS NULL THEN NULL WHEN %s = '' THEN UNHEX(REPEAT('00',16)) ELSE UUID_TO_BIN(%s) END", col.column, col.column, col.column)
	if col.nullable {
		expr = fmt.Sprintf("CASE WHEN %s IS NULL OR %s = '' THEN NULL ELSE UUID_TO_BIN(%s) END", col.column, col.column, col.column)
	}
	if err := db.WithContext(ctx).Exec(fmt.Sprintf("UPDATE %s SET %s = %s", col.table, tmp, expr)).Error; err != nil {
		return fmt.Errorf("backfill %s.%s: %w", col.table, tmp, err)
	}

	if col.primary {
		_ = db.WithContext(ctx).Exec("ALTER TABLE " + col.table + " DROP PRIMARY KEY").Error
	}
	if err := db.WithContext(ctx).Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", col.table, col.column)).Error; err != nil && !isBenignDDLError(err) {
		return fmt.Errorf("drop %s.%s: %w", col.table, col.column, err)
	}
	if err := renameMySQLUUIDTempColumn(ctx, db, col, tmp); err != nil {
		return err
	}
	return restoreMySQLUUIDColumnPrimaryKey(ctx, db, col)
}

func renameMySQLUUIDTempColumn(ctx context.Context, db *gorm.DB, col legacyUUIDColumn, tmp string) error {
	nullSpec := "NULL"
	if !col.nullable {
		nullSpec = "NOT NULL"
	}
	if err := db.WithContext(ctx).Exec(fmt.Sprintf(
		"ALTER TABLE %s CHANGE COLUMN %s %s binary(16) %s",
		col.table, tmp, col.column, nullSpec,
	)).Error; err != nil && !isBenignDDLError(err) {
		return fmt.Errorf("rename %s.%s: %w", col.table, tmp, err)
	}
	return nil
}

func restoreMySQLUUIDColumnPrimaryKey(ctx context.Context, db *gorm.DB, col legacyUUIDColumn) error {
	if col.primary {
		exists, err := mysqlPrimaryKeyExists(ctx, db, col.table)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		pk := col.column
		if col.table == "concurrency_slots" {
			pk = "slot_name, job_id"
		}
		if err := db.WithContext(ctx).Exec("ALTER TABLE " + col.table + " ADD PRIMARY KEY (" + pk + ")").Error; err != nil && !isBenignDDLError(err) {
			return fmt.Errorf("restore %s primary key: %w", col.table, err)
		}
	}
	return nil
}

func mysqlPrimaryKeyExists(ctx context.Context, db *gorm.DB, table string) (bool, error) {
	var count int64
	if err := db.WithContext(ctx).Raw(`
		SELECT COUNT(*)
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND CONSTRAINT_TYPE = 'PRIMARY KEY'
	`, table).Scan(&count).Error; err != nil {
		return false, fmt.Errorf("inspect %s primary key: %w", table, err)
	}
	return count > 0, nil
}

func convertSQLiteLegacyStringUUIDColumns(ctx context.Context, db *gorm.DB) error {
	needsRun, err := sqliteLegacyUUIDConversionNeedsRun(ctx, db)
	if err != nil || !needsRun {
		return err
	}
	for _, col := range legacyUUIDColumns {
		if !db.Migrator().HasTable(col.table) || !db.Migrator().HasColumn(col.table, col.column) {
			continue
		}
		legacy, err := sqliteUUIDColumnIsLegacyText(ctx, db, col)
		if err != nil {
			return err
		}
		if !legacy {
			continue
		}
		if err := rewriteSQLiteUUIDColumn(ctx, db, col); err != nil {
			return err
		}
	}
	return declareSQLiteUUIDColumnsAsBlob(ctx, db)
}

func sqliteLegacyUUIDConversionNeedsRun(ctx context.Context, db *gorm.DB) (bool, error) {
	for _, col := range legacyUUIDColumns {
		if !db.Migrator().HasTable(col.table) {
			continue
		}
		if db.Migrator().HasColumn(col.table, col.column+"_uuid_tmp") {
			return true, nil
		}
		if !db.Migrator().HasColumn(col.table, col.column) {
			continue
		}
		legacy, err := sqliteUUIDColumnIsLegacyText(ctx, db, col)
		if err != nil {
			return false, err
		}
		if legacy {
			return true, nil
		}
	}
	return false, nil
}

func sqliteJobsIDIsLegacyText(ctx context.Context, db *gorm.DB) (bool, error) {
	return sqliteUUIDColumnIsLegacyText(ctx, db, legacyUUIDColumn{table: "jobs", column: "id"})
}

func sqliteUUIDColumnIsLegacyText(ctx context.Context, db *gorm.DB, col legacyUUIDColumn) (bool, error) {
	var storedType sql.NullString
	if err := db.WithContext(ctx).Raw(fmt.Sprintf("SELECT typeof(%s) FROM %s WHERE %s IS NOT NULL LIMIT 1", col.column, col.table, col.column)).Scan(&storedType).Error; err != nil {
		return false, err
	}
	if storedType.Valid {
		switch strings.ToLower(storedType.String) {
		case "blob":
			return false, nil
		case "text":
			return true, nil
		}
	}
	var declaredType sql.NullString
	if err := db.WithContext(ctx).Raw("SELECT type FROM pragma_table_info(?) WHERE name = ?", col.table, col.column).Scan(&declaredType).Error; err != nil {
		return false, err
	}
	return declaredType.Valid && sqliteTypeIsLegacyString(declaredType.String), nil
}

func sqliteTypeIsLegacyString(typeName string) bool {
	t := strings.ToLower(typeName)
	return strings.Contains(t, "text") || strings.Contains(t, "char") || strings.Contains(t, "varchar")
}

func rewriteSQLiteUUIDColumn(ctx context.Context, db *gorm.DB, col legacyUUIDColumn) error {
	type row struct {
		RowID int64          `gorm:"column:rowid"`
		Value sql.NullString `gorm:"column:value"`
	}
	var rows []row
	if err := db.WithContext(ctx).Raw(
		fmt.Sprintf("SELECT rowid, %s AS value FROM %s", col.column, col.table),
	).Scan(&rows).Error; err != nil {
		return fmt.Errorf("read %s.%s: %w", col.table, col.column, err)
	}
	for _, r := range rows {
		if !r.Value.Valid {
			continue
		}
		if r.Value.String == "" && col.nullable {
			if err := db.WithContext(ctx).Exec(
				fmt.Sprintf("UPDATE %s SET %s = NULL WHERE rowid = ?", col.table, col.column),
				r.RowID,
			).Error; err != nil {
				return fmt.Errorf("null %s.%s: %w", col.table, col.column, err)
			}
			continue
		}
		value := r.Value.String
		if value == "" && col.sentinel {
			value = nilUUIDString
		}
		parsed, err := core.ParseUUID(value)
		if err != nil {
			return fmt.Errorf("parse %s.%s rowid %d: %w", col.table, col.column, r.RowID, err)
		}
		blob, err := parsed.Value()
		if err != nil {
			return fmt.Errorf("encode %s.%s rowid %d: %w", col.table, col.column, r.RowID, err)
		}
		b, ok := blob.([]byte)
		if !ok || len(b) != 16 {
			return fmt.Errorf("encode %s.%s rowid %d: expected 16-byte UUID", col.table, col.column, r.RowID)
		}
		if col.sentinel && value == nilUUIDString {
			b = bytes.Repeat([]byte{0}, 16)
		}
		if err := db.WithContext(ctx).Exec(
			fmt.Sprintf("UPDATE %s SET %s = ? WHERE rowid = ?", col.table, col.column),
			b, r.RowID,
		).Error; err != nil {
			return fmt.Errorf("write %s.%s rowid %d: %w", col.table, col.column, r.RowID, err)
		}
	}
	return nil
}

func declareSQLiteUUIDColumnsAsBlob(ctx context.Context, db *gorm.DB) error {
	byTable := make(map[string][]string)
	for _, col := range legacyUUIDColumns {
		byTable[col.table] = append(byTable[col.table], col.column)
	}
	var schemaVersion int
	if err := db.WithContext(ctx).Raw("PRAGMA schema_version").Scan(&schemaVersion).Error; err != nil {
		return err
	}
	if err := db.WithContext(ctx).Exec("PRAGMA writable_schema = ON").Error; err != nil {
		return err
	}
	defer func() {
		_ = db.WithContext(context.WithoutCancel(ctx)).Exec("PRAGMA writable_schema = OFF").Error
	}()
	for table, columns := range byTable {
		if !db.Migrator().HasTable(table) {
			continue
		}
		var createSQL sql.NullString
		if err := db.WithContext(ctx).Raw(
			"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
			table,
		).Scan(&createSQL).Error; err != nil {
			return err
		}
		if !createSQL.Valid || createSQL.String == "" {
			continue
		}
		updated := createSQL.String
		for _, column := range columns {
			updated = sqliteDeclareColumnAsBlob(updated, column)
		}
		if updated == createSQL.String {
			continue
		}
		if err := db.WithContext(ctx).Exec(
			"UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = ?",
			updated, table,
		).Error; err != nil {
			return fmt.Errorf("declare %s UUID columns as blob: %w", table, err)
		}
	}
	return db.WithContext(ctx).Exec(fmt.Sprintf("PRAGMA schema_version = %d", schemaVersion+1)).Error
}

func sqliteDeclareColumnAsBlob(createSQL, column string) string {
	quoted := regexp.QuoteMeta(column)
	pattern := regexp.MustCompile(`(?i)(^|[\s,(])((?:` + "`" + `|"` + `|\[)?` + quoted + `(?:` + "`" + `|"|\])?\s+)(text|varchar\s*\(\s*36\s*\)|char\s*\(\s*36\s*\)|character\s+varying\s*\(\s*36\s*\)|character\s*\(\s*36\s*\))`)
	return pattern.ReplaceAllString(createSQL, "${1}${2}BLOB")
}
