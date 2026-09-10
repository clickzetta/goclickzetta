package goclickzetta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// The GORM layer had a single integration test that needed a live instance, so
// none of it ran unattended. Everything here runs against a fake gorm.ConnPool
// instead: Dialector.Create builds the whole INSERT itself and hands the text to
// ConnPool.ExecContext, which is exactly the seam a fake can sit in.
//
// The tests pin the SQL byte for byte. That is deliberate for a statement built
// by string concatenation: the encoding of every value type, the quoting of
// identifiers and the spacing are the contract with the server, and a silent
// change to any of them is a bug that only shows up as a server-side parse
// error.

// fakeConnPool records the statements Create sends and returns a canned result.
type fakeConnPool struct {
	queries []string
	args    [][]interface{}
	result  sql.Result
	err     error
}

func (p *fakeConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	p.queries = append(p.queries, query)
	p.args = append(p.args, args)
	if p.err != nil {
		return nil, p.err
	}
	if p.result != nil {
		return p.result, nil
	}
	return fakeSQLResult{}, nil
}

// The remaining three methods of gorm.ConnPool are never reached by the create
// path. They fail loudly rather than returning zero values, so a test that grows
// past what this fake covers says so instead of quietly reading nothing.
func (p *fakeConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, errors.New("fakeConnPool: PrepareContext is not implemented")
}

func (p *fakeConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("fakeConnPool: QueryContext is not implemented")
}

func (p *fakeConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}

func (p *fakeConnPool) onlyQuery(t *testing.T) string {
	t.Helper()
	if len(p.queries) != 1 {
		t.Fatalf("pool saw %d statements, want 1: %q", len(p.queries), p.queries)
	}
	return p.queries[0]
}

type fakeSQLResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r fakeSQLResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r fakeSQLResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// gormWidget has no primary key on purpose: a primary key would be an
// autoincrement column, which sends ConvertToCreateValues down the
// default-value path exercised separately by
// TestCreatePanicsOnBatchesWithDefaultValueColumns.
type gormWidget struct {
	Name  string
	Count int
}

func (gormWidget) TableName() string { return "widgets" }

// openGORM builds a *gorm.DB on top of pool. SkipDefaultTransaction is required,
// not a convenience: the default callbacks wrap Create in a transaction, and a
// gorm.ConnPool that is not also a TxBeginner cannot start one.
func openGORM(t *testing.T, pool gorm.ConnPool) *gorm.DB {
	t.Helper()
	dialector := &Dialector{ClickZettaConfig: &ClickZettaConfig{Conn: pool}}
	db, err := gorm.Open(dialector, &gorm.Config{
		SkipDefaultTransaction: true,
		DisableAutomaticPing:   true,
		Logger:                 gormlogger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	return db
}

func TestCreateBuildsASingleRowInsert(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	if err := db.Create(&gormWidget{Name: "one", Count: 7}).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Two spaces after VALUES: clause.Values leaves a trailing one and Create
	// adds another when it joins the rows on. Harmless, and pinned here so the
	// expectation matches what actually goes over the wire.
	want := "INSERT INTO `widgets` (`name`,`count`) VALUES  (\"one\",7)"
	if got := pool.onlyQuery(t); got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
	// The values are inlined, so nothing is left to bind.
	if args := pool.args[0]; len(args) != 0 {
		t.Errorf("statement carried %d bind arguments, want none: %v", len(args), args)
	}
}

func TestCreateBuildsOneStatementForABatch(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	rows := []gormWidget{{Name: "a", Count: 1}, {Name: "b", Count: 2}}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	want := "INSERT INTO `widgets` (`name`,`count`) VALUES  (\"a\",1),(\"b\",2)"
	if got := pool.onlyQuery(t); got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

// Every value is rendered with encoding/json, which is not an SQL encoder. This
// test is the record of what that means, because none of it is obvious from the
// call site:
//
//   - strings arrive double quoted, with JSON escapes, so the server has to read
//     "..." as a string literal that honours backslash escapes;
//   - a time.Time arrives as an RFC 3339 string and relies on an implicit cast,
//     not as the timestamp '...' literal the driver's own binding path produces;
//   - a nil pointer arrives as the JSON null literal.
//
// Contrast this with replacePlaceholders in util.go, which quotes with ' and
// declares the escape mode it used to the server. The two paths do not agree.
func TestCreateEncodesValuesWithJSON(t *testing.T) {
	type jsonWidget struct {
		Text  string
		When  time.Time
		Maybe *string
		Flag  bool
		Ratio float64
	}

	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	row := &jsonWidget{
		Text:  `o'brien "x" \y`,
		When:  time.Date(2026, 9, 10, 1, 2, 3, 456000000, time.UTC),
		Flag:  true,
		Ratio: 1.5,
	}
	if err := db.Table("widgets").Create(row).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	want := "INSERT INTO `widgets` (`text`,`when`,`maybe`,`flag`,`ratio`) VALUES " +
		` ("o'brien \"x\" \\y","2026-09-10T01:02:03.456Z",null,true,1.5)`
	if got := pool.onlyQuery(t); got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

func TestCreatePropagatesExecErrors(t *testing.T) {
	wantErr := errors.New("server said no")
	pool := &fakeConnPool{err: wantErr}
	db := openGORM(t, pool)

	err := db.Create(&gormWidget{Name: "one", Count: 1}).Error
	if !errors.Is(err, wantErr) {
		t.Fatalf("Create() error = %v, want %v", err, wantErr)
	}
}

// Create is registered as the gorm:create callback, so it runs with whatever
// error the earlier callbacks left on the statement. It has to stay off the wire
// in that case.
func TestCreateSkipsAStatementThatAlreadyFailed(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)
	dialector := &Dialector{ClickZettaConfig: &ClickZettaConfig{Conn: pool}}

	tx := db.Session(&gorm.Session{NewDB: true})
	tx.AddError(errors.New("an earlier callback failed"))
	dialector.Create(tx)

	if len(pool.queries) != 0 {
		t.Errorf("pool saw %d statements, want none: %q", len(pool.queries), pool.queries)
	}
}

// gorm.WithResult is the only way to reach the db.Statement.Result branch: the
// field is set by a statement modifier and is unexported otherwise.
func TestCreateHandsBackTheDriverResult(t *testing.T) {
	pool := &fakeConnPool{result: fakeSQLResult{lastInsertID: 0, rowsAffected: 1}}
	db := openGORM(t, pool)

	result := gorm.WithResult()
	if err := db.Clauses(result).Create(&gormWidget{Name: "one", Count: 1}).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if result.Result == nil {
		t.Fatal("Create() left the sql.Result unset")
	}
	affected, err := result.Result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("RowsAffected() = %d, want 1", affected)
	}
}

// An empty slice is rejected by GORM before the callback runs, so no statement
// is built for it.
func TestCreateRejectsAnEmptySlice(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	err := db.Create(&[]gormWidget{}).Error
	if !errors.Is(err, gorm.ErrEmptySlice) {
		t.Fatalf("Create() error = %v, want %v", err, gorm.ErrEmptySlice)
	}
	if len(pool.queries) != 0 {
		t.Errorf("pool saw %d statements, want none: %q", len(pool.queries), pool.queries)
	}
}

// autoWidget's primary key is an autoincrement column, which GORM records as a
// field with a database-side default value.
type autoWidget struct {
	ID   int64 `gorm:"primaryKey"`
	Name string
}

func (autoWidget) TableName() string { return "autos" }

// A single insert of a model with an autoincrement key is fine: GORM only omits
// the column.
func TestCreateOmitsAnUnsetAutoincrementKey(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	if err := db.Create(&autoWidget{Name: "a"}).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	want := "INSERT INTO `autos` (`name`) VALUES  (\"a\")"
	if got := pool.onlyQuery(t); got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

// A batch is not fine. When some rows set a default-value column and others
// leave it zero, ConvertToCreateValues fills the gaps with the dialector's
// DefaultValueOf, which panics with "unimplemented". The panic happens inside
// GORM, before Create sees anything, so there is nothing the driver could
// recover from at that point.
//
// This is a real limitation of the GORM support, not a quirk of the test: any
// model with an autoincrement primary key or a `default:` tag hits it as soon as
// a batch mixes set and unset values. It is pinned so that implementing
// DefaultValueOf shows up here as a failing expectation rather than going
// unnoticed.
func TestCreatePanicsOnBatchesWithMixedDefaultValues(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Create() did not panic; DefaultValueOf may now be implemented")
		}
		if msg, ok := r.(string); !ok || msg != "unimplemented" {
			t.Fatalf("panic = %v, want \"unimplemented\"", r)
		}
		if len(pool.queries) != 0 {
			t.Errorf("pool saw %d statements, want none: %q", len(pool.queries), pool.queries)
		}
	}()

	rows := []autoWidget{{ID: 5, Name: "a"}, {Name: "b"}}
	_ = db.Create(&rows)
}

func TestDialectorName(t *testing.T) {
	if got := (Dialector{}).Name(); got != DefaultDriverName {
		t.Errorf("Name() = %q, want %q", got, DefaultDriverName)
	}
}

// The driver has no server-side parameter binding, so the placeholder GORM is
// told to write is only ever consumed by replacePlaceholders on the way out.
func TestDialectorBindVarTo(t *testing.T) {
	var w strings.Builder
	(Dialector{}).BindVarTo(&w, nil, 42)
	if got := w.String(); got != "?" {
		t.Errorf("BindVarTo() wrote %q, want %q", got, "?")
	}
}

func TestDialectorQuoteTo(t *testing.T) {
	// Backticks, matching the identifier quoting splitSQL recognises.
	for _, name := range []string{"widgets", "some column", ""} {
		var w strings.Builder
		(Dialector{}).QuoteTo(&w, name)
		if want := "`" + name + "`"; w.String() != want {
			t.Errorf("QuoteTo(%q) wrote %q, want %q", name, w.String(), want)
		}
	}
}

func TestDialectorExplain(t *testing.T) {
	got := (Dialector{}).Explain("SELECT ?", 1, "two")
	want := "SQL: SELECT ?, Vars: [1 two]"
	if got != want {
		t.Errorf("Explain() = %q, want %q", got, want)
	}
}

// NowFunc's argument is a number of decimal digits, so it rounds rather than
// truncates: three digits round to the nearest millisecond.
func TestDialectorNowFuncRoundsToThePrecision(t *testing.T) {
	for _, tc := range []struct {
		digits int
		round  time.Duration
	}{
		{0, time.Second},
		{3, time.Millisecond},
		{6, time.Microsecond},
	} {
		now := (Dialector{}).NowFunc(tc.digits)()
		if got := now.Truncate(tc.round); !got.Equal(now) {
			t.Errorf("NowFunc(%d) returned %v, which is not a multiple of %v", tc.digits, now, tc.round)
		}
	}
}

func TestDialectorApplySetsTheClockOnce(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		cfg := &ClickZettaConfig{}
		dialector := Dialector{ClickZettaConfig: cfg}
		config := &gorm.Config{}

		if err := dialector.Apply(config); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if config.NowFunc == nil {
			t.Fatal("Apply() left NowFunc unset")
		}
		// Apply takes a value receiver, but ClickZettaConfig is a pointer, so the
		// default precision it fills in is visible to the caller.
		if cfg.DefaultDatetimePrecision == nil {
			t.Fatal("Apply() left DefaultDatetimePrecision unset")
		}
		if got := *cfg.DefaultDatetimePrecision; got != defaultDatetimePrecision {
			t.Errorf("DefaultDatetimePrecision = %d, want %d", got, defaultDatetimePrecision)
		}
		now := config.NowFunc()
		if got := now.Truncate(time.Millisecond); !got.Equal(now) {
			t.Errorf("NowFunc() = %v, which is not a whole millisecond", now)
		}
	})

	t.Run("already set", func(t *testing.T) {
		cfg := &ClickZettaConfig{}
		dialector := Dialector{ClickZettaConfig: cfg}
		fixed := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
		config := &gorm.Config{NowFunc: func() time.Time { return fixed }}

		if err := dialector.Apply(config); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if got := config.NowFunc(); !got.Equal(fixed) {
			t.Errorf("Apply() replaced NowFunc: got %v, want %v", got, fixed)
		}
		// The early return skips the precision default as well.
		if cfg.DefaultDatetimePrecision != nil {
			t.Errorf("DefaultDatetimePrecision = %d, want it left unset", *cfg.DefaultDatetimePrecision)
		}
	})
}

func TestInitializeUsesTheInjectedConn(t *testing.T) {
	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	if db.ConnPool != gorm.ConnPool(pool) {
		t.Errorf("ConnPool = %T, want the injected pool", db.ConnPool)
	}
	// Initialize replaces gorm:create, so a Create goes through create.go rather
	// than through GORM's own callback, which would try to bind parameters.
	if err := db.Create(&gormWidget{Name: "one", Count: 1}).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !strings.HasPrefix(pool.onlyQuery(t), "INSERT INTO `widgets`") {
		t.Errorf("unexpected statement %q", pool.queries[0])
	}
}

// Without an injected pool the dialector opens the DSN itself. sql.Open does not
// connect, so this reaches no server; DisableAutomaticPing keeps gorm.Open from
// making it connect.
func TestInitializeOpensTheDSN(t *testing.T) {
	cfg := &ClickZettaConfig{DSN: "username:password@instance.example.com/workspace?virtualCluster=vc"}
	db, err := gorm.Open(&Dialector{ClickZettaConfig: cfg}, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               gormlogger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	sqlDB, ok := db.ConnPool.(*sql.DB)
	if !ok {
		t.Fatalf("ConnPool = %T, want *sql.DB", db.ConnPool)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	// Initialize takes a value receiver, but Dialector embeds the config by
	// pointer, so the defaults it fills in are written through to the config the
	// caller still holds.
	if cfg.DriverName != DefaultDriverName {
		t.Errorf("DriverName = %q, want %q", cfg.DriverName, DefaultDriverName)
	}
	if cfg.DefaultDatetimePrecision == nil || *cfg.DefaultDatetimePrecision != defaultDatetimePrecision {
		t.Errorf("DefaultDatetimePrecision = %v, want %d", cfg.DefaultDatetimePrecision, defaultDatetimePrecision)
	}
}

func TestInitializeReportsAnUnknownDriver(t *testing.T) {
	_, err := gorm.Open(&Dialector{ClickZettaConfig: &ClickZettaConfig{
		DriverName: "not-a-registered-driver",
		DSN:        "ignored",
	}}, &gorm.Config{DisableAutomaticPing: true, Logger: gormlogger.Discard})
	if err == nil {
		t.Fatal("gorm.Open() succeeded, want an unknown driver error")
	}
	if !strings.Contains(err.Error(), "unknown driver") {
		t.Errorf("error = %v, want it to mention the unknown driver", err)
	}
}

func TestOpenCarriesTheDSN(t *testing.T) {
	dialector, ok := Open("some-dsn").(*Dialector)
	if !ok {
		t.Fatalf("Open() returned %T, want *Dialector", Open("some-dsn"))
	}
	if dialector.DSN != "some-dsn" {
		t.Errorf("DSN = %q, want %q", dialector.DSN, "some-dsn")
	}
	if dialector.Conn != nil {
		t.Error("Open() set a Conn")
	}
}

// The three Dialector methods GORM needs for migrations are not implemented.
// Nothing in the create or query path calls them, which is why the driver works
// at all, but AutoMigrate and anything else that inspects column types will take
// down the process. Pinned so that implementing them is a deliberate change.
func TestUnimplementedDialectorMethodsPanic(t *testing.T) {
	dialector := Dialector{ClickZettaConfig: &ClickZettaConfig{}}
	field := &schema.Field{Name: "Name", DBName: "name"}

	for _, tc := range []struct {
		name string
		call func()
	}{
		{"DataTypeOf", func() { _ = dialector.DataTypeOf(field) }},
		{"DefaultValueOf", func() { _ = dialector.DefaultValueOf(field) }},
		{"Migrator", func() { _ = dialector.Migrator(&gorm.DB{}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("%s() did not panic", tc.name)
				}
				if msg, ok := r.(string); !ok || msg != "unimplemented" {
					t.Fatalf("panic = %v, want \"unimplemented\"", r)
				}
			}()
			tc.call()
		})
	}
}

// clause is imported for the compile-time check that the dialector still
// satisfies gorm.Dialector; the assertion below is what keeps the import honest.
var (
	_ gorm.Dialector    = Dialector{}
	_ clause.Expression = clause.Insert{}
)

// gormClauseFlag is a field type that contributes a model-level create clause,
// the way gorm.io/plugin/soft_delete does. Create copies those clauses onto the
// statement before building the INSERT.
type gormClauseFlag int64

func (gormClauseFlag) CreateClauses(f *schema.Field) []clause.Interface {
	return []clause.Interface{clause.Insert{Modifier: "OVERWRITE"}}
}

func TestCreateAppliesModelCreateClauses(t *testing.T) {
	type clauseWidget struct {
		Name string
		Flag gormClauseFlag
	}

	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	if err := db.Table("widgets").Create(&clauseWidget{Name: "one", Flag: 2}).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// The modifier from the model-supplied clause is what proves it was applied:
	// AddClauseIfNotExists would otherwise have installed a bare clause.Insert.
	want := "INSERT OVERWRITE INTO `widgets` (`name`,`flag`) VALUES  (\"one\",2)"
	if got := pool.onlyQuery(t); got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

// A value encoding/json cannot marshal aborts Create halfway through, and it
// does so silently: the function just returns, so no statement is sent and
// nothing is recorded on db.Error. The caller is told the insert succeeded.
//
// This is a defect, pinned here as it stands. NaN reaches it through a plain
// float64 column, so a caller does not have to do anything exotic to hit it.
func TestCreateSilentlyDropsUnmarshalableValues(t *testing.T) {
	type ratioWidget struct {
		Name  string
		Ratio float64
	}

	pool := &fakeConnPool{}
	db := openGORM(t, pool)

	err := db.Table("widgets").Create(&ratioWidget{Name: "one", Ratio: math.NaN()}).Error
	if err != nil {
		t.Fatalf("Create() error = %v; if this now reports the encoding failure, the test should assert that instead", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("pool saw %d statements, want none: %q", len(pool.queries), pool.queries)
	}
}

// Initialize fills in the datetime precision too. gorm.Open normally gets there
// first through Apply, so the branch is only reachable when the caller supplied
// its own NowFunc and Apply returned early.
func TestInitializeSetsThePrecisionWhenApplySkippedIt(t *testing.T) {
	cfg := &ClickZettaConfig{Conn: &fakeConnPool{}}
	_, err := gorm.Open(&Dialector{ClickZettaConfig: cfg}, &gorm.Config{
		SkipDefaultTransaction: true,
		DisableAutomaticPing:   true,
		Logger:                 gormlogger.Discard,
		NowFunc:                func() time.Time { return time.Unix(0, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	if cfg.DefaultDatetimePrecision == nil {
		t.Fatal("Initialize() left DefaultDatetimePrecision unset")
	}
	if got := *cfg.DefaultDatetimePrecision; got != defaultDatetimePrecision {
		t.Errorf("DefaultDatetimePrecision = %d, want %d", got, defaultDatetimePrecision)
	}
}

// gormIntegrationRow avoids a field literally named ID: GORM would take it for
// the primary key and treat it as an autoincrement column, which changes the
// column list Create builds. The column names are set explicitly so the DDL
// below and the struct cannot drift apart.
type gormIntegrationRow struct {
	Num  int64  `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

// The unit tests above pin the SQL text. This one answers the question they
// cannot: whether the server accepts it. The JSON encoding leaves string values
// in double quotes with backslash escapes, which is not the form the driver's own
// binding path produces, so it is worth a round trip against a real instance.
//
// A backslash and a double quote survive that encoding. A single quote does not:
// see TestGORMCreateLosesSingleQuotes.
func TestGORMCreateRoundTrip(t *testing.T) {
	db, table, ctx := setUpGORMIntegrationTable(t)

	single := gormIntegrationRow{Num: 1, Name: `back\slash and "quotes"`}
	if err := db.WithContext(ctx).Table(table).Create(&single).Error; err != nil {
		t.Fatalf("Create() single row: %v", err)
	}

	batch := []gormIntegrationRow{{Num: 2, Name: "bob"}, {Num: 3, Name: "carol"}}
	if err := db.WithContext(ctx).Table(table).Create(&batch).Error; err != nil {
		t.Fatalf("Create() batch: %v", err)
	}

	got := readGORMIntegrationTable(t, db, ctx, table)
	want := []gormIntegrationRow{single, batch[0], batch[1]}
	if len(got) != len(want) {
		t.Fatalf("read back %d rows, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// A string holding a single quote is corrupted on the way in: the character is
// silently dropped and the row lands one byte shorter than it went out.
//
// The cause is create.go encoding values with encoding/json, which produces a
// double-quoted literal. Inside "..." the server only resolves a quote that is
// escaped, either doubled or preceded by a backslash, and swallows a lone one;
// the behaviour is the same under every cz.sql.string.literal.escape.mode, so the
// hint cannot rescue it. The
// driver's own binding path does not have the problem, because it quotes with '
// and escapes accordingly.
//
// This test records the defect rather than the intent. Fixing create.go to quote
// the way util.go does will make it fail, which is the point: the assertion is
// where the fix should be confirmed.
func TestGORMCreateLosesSingleQuotes(t *testing.T) {
	db, table, ctx := setUpGORMIntegrationTable(t)

	row := gormIntegrationRow{Num: 1, Name: "o'brien"}
	if err := db.WithContext(ctx).Table(table).Create(&row).Error; err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got := readGORMIntegrationTable(t, db, ctx, table)
	if len(got) != 1 {
		t.Fatalf("read back %d rows, want 1: %+v", len(got), got)
	}
	if got[0].Name == row.Name {
		t.Fatalf("single quotes now survive Create(); create.go was fixed, so this test should be replaced by a round-trip assertion")
	}
	if got[0].Name != "obrien" {
		t.Errorf("name = %q, want %q, the value with the quote dropped", got[0].Name, "obrien")
	}
}

// setUpGORMIntegrationTable opens a GORM handle on the live instance and creates
// a table that is dropped when the test ends.
func setUpGORMIntegrationTable(t *testing.T) (*gorm.DB, string, context.Context) {
	t.Helper()
	dsn := integrationDSN(t)

	db, err := gorm.Open(Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 gormlogger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB() error = %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	table := fmt.Sprintf("goclickzetta_gorm_it_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	if err := db.WithContext(ctx).Exec(fmt.Sprintf("CREATE TABLE %s (id BIGINT, name STRING)", table)).Error; err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if err := db.WithContext(cleanupCtx).Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Error; err != nil {
			t.Errorf("drop integration table: %v", err)
		}
	})
	return db, table, ctx
}

func readGORMIntegrationTable(t *testing.T, db *gorm.DB, ctx context.Context, table string) []gormIntegrationRow {
	t.Helper()
	var rows []gormIntegrationRow
	query := fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", table)
	if err := db.WithContext(ctx).Raw(query).Scan(&rows).Error; err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	return rows
}
