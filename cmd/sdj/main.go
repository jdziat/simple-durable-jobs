// Command sdj provides operational tools for Simple Durable Jobs storage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	mysqlcfg "github.com/go-sql-driver/mysql"
	jobs "github.com/jdziat/simple-durable-jobs/v4"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var version = "dev"

const (
	exitOK      = 0
	exitError   = 1
	exitHandled = 2
	// exitPartial exists so `dlq requeue --queue "$Q" && clear-alert`
	// means what an operator thinks it means. A bulk requeue used to exit 0 whether
	// it drained the queue, matched nothing at all (a typo'd or already-empty
	// queue), or requeued some rows and skipped others — three outcomes an
	// automation must act on differently, collapsed into one code.
	exitNoMatch = 3
	exitPartial = 4
)

const usageText = `sdj is the Simple Durable Jobs operations CLI.

Usage:
  sdj [--driver sqlite|postgres|mysql] --dsn <dsn> <command> [options]
  sdj --version
  sdj version

Global flags:
  --driver string   Database driver: sqlite, postgres, mysql (default "sqlite")
  --dsn string      Database connection string (required for storage commands)

Commands:
  migrate           Run storage migrations and print a success line
  queues            Print queue pending depth, DLQ depth, oldest pending time, and backlog age
  dlq list          List dead-lettered jobs as a table, JSON, or IDs only
  dlq requeue       Requeue one dead-lettered job by id
  health            Ping storage and print OK on success
  version           Print sdj version
  help              Print this help
`

const dlqUsageText = `Manage dead-lettered jobs and print list/requeue results.

Usage:
  sdj [--driver sqlite|postgres|mysql] --dsn <dsn> dlq list [options]
  sdj [--driver sqlite|postgres|mysql] --dsn <dsn> dlq requeue <jobID>
  sdj [--driver sqlite|postgres|mysql] --dsn <dsn> dlq requeue --queue q [--tenant t]

Commands:
  list              List dead-lettered jobs as a table, JSON, or IDs only
  requeue           Requeue one dead-lettered job by id or a filtered batch

Bulk requeue exit codes (--queue/--tenant):
  0                 every matching job was requeued
  3                 no dead-lettered job matched the filter
  4                 some matching jobs were skipped and are still dead-lettered
`

type globalOptions struct {
	driver string
	dsn    string
}

type openedStore struct {
	store *jobs.GormStorage
	close func() error
}

type metadataFlags map[string]string

func (m *metadataFlags) String() string {
	if m == nil || len(*m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*m))
	for key := range *m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+(*m)[key])
	}
	return strings.Join(parts, ",")
}

func (m *metadataFlags) Set(value string) error {
	key, val, ok := strings.Cut(value, "=")
	if !ok || key == "" {
		return fmt.Errorf("metadata filter must be key=value")
	}
	if *m == nil {
		*m = make(metadataFlags)
	}
	(*m)[key] = val
	return nil
}

func (m metadataFlags) metadataMap() *jobs.MetadataMap {
	if len(m) == 0 {
		return nil
	}
	out := make(jobs.MetadataMap, len(m))
	for key, value := range m {
		out[key] = value
	}
	return &out
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	a := app{stdout: stdout, stderr: stderr}
	return a.run(context.Background(), args)
}

type app struct {
	stdout io.Writer
	stderr io.Writer
}

func (a app) run(ctx context.Context, args []string) int {
	globals, rest, code := a.parseGlobals(args)
	if code == exitHandled {
		return exitOK
	}
	if code != exitOK || len(rest) == 0 {
		if len(rest) == 0 && code == exitOK {
			_, _ = fmt.Fprint(a.stderr, usageText)
			_, _ = fmt.Fprintln(a.stderr, "\nmissing subcommand; run `sdj help` for usage")
			return exitError
		}
		return code
	}

	switch rest[0] {
	case "migrate":
		return a.runMigrate(ctx, globals, rest[1:])
	case "queues":
		return a.runQueues(ctx, globals, rest[1:])
	case "dlq":
		return a.runDLQ(ctx, globals, rest[1:])
	case "health":
		return a.runHealth(ctx, globals, rest[1:])
	case "version":
		if len(rest) != 1 {
			return a.fail("version accepts no arguments")
		}
		_, _ = fmt.Fprintf(a.stdout, "sdj %s\n", version)
		return exitOK
	case "help", "-h", "--help":
		_, _ = fmt.Fprint(a.stdout, usageText)
		return exitOK
	default:
		return a.fail("unknown command %q; valid commands: migrate, queues, dlq, health, version, help", rest[0])
	}
}

func (a app) parseGlobals(args []string) (globalOptions, []string, int) {
	var opts globalOptions
	fs := flag.NewFlagSet("sdj", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	fs.StringVar(&opts.driver, "driver", "sqlite", "Database driver: sqlite, postgres, mysql")
	fs.StringVar(&opts.dsn, "dsn", "", "Database connection string")
	showVersion := fs.Bool("version", false, "Print sdj version")
	fs.Usage = func() { _, _ = fmt.Fprint(fs.Output(), usageText) }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return opts, nil, exitHandled
		}
		return opts, nil, exitError
	}
	if *showVersion {
		_, _ = fmt.Fprintf(a.stdout, "sdj %s\n", version)
		return opts, nil, exitHandled
	}
	return opts, fs.Args(), exitOK
}

func (a app) runMigrate(ctx context.Context, globals globalOptions, args []string) int {
	fs := a.newFlagSet("migrate", `Run idempotent storage migrations and print a success line.

Usage:
  sdj --driver sqlite --dsn ./jobs.db migrate
  sdj --driver postgres --dsn postgres://user:pass@localhost/db migrate
  sdj --driver mysql --dsn 'user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27' migrate
`)
	if code := a.parseSubcommand(fs, args); code != exitOK {
		if code == exitHandled {
			return exitOK
		}
		return code
	}
	if fs.NArg() != 0 {
		return a.fail("migrate accepts no arguments")
	}
	opened, ok := a.openStore(globals)
	if !ok {
		return exitError
	}
	defer closeStore(opened)
	if err := opened.store.Migrate(ctx); err != nil {
		return a.fail("migrate failed; verify the DSN, database permissions, and schema migration access: %s", userError(err))
	}
	_, _ = fmt.Fprintln(a.stdout, "migrations applied successfully")
	return exitOK
}

func (a app) runQueues(ctx context.Context, globals globalOptions, args []string) int {
	fs := a.newFlagSet("queues", `Print an aligned table of queue pending depth, DLQ depth, and oldest pending timestamp.

Usage:
  sdj --driver sqlite --dsn ./jobs.db queues
  sdj --driver postgres --dsn postgres://user:pass@localhost/db queues
  sdj --driver mysql --dsn 'user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27' queues
`)
	if code := a.parseSubcommand(fs, args); code != exitOK {
		if code == exitHandled {
			return exitOK
		}
		return code
	}
	if fs.NArg() != 0 {
		return a.fail("queues accepts no arguments")
	}
	opened, ok := a.openStore(globals)
	if !ok {
		return exitError
	}
	defer closeStore(opened)

	pending, err := opened.store.QueuePendingCounts(ctx)
	if err != nil {
		return a.fail("could not read pending queue counts; verify migrations have run and the DSN is correct: %s", userError(err))
	}
	dlq, err := opened.store.QueueDeadLetterCounts(ctx)
	if err != nil {
		return a.fail("could not read dead-letter counts; verify migrations have run and the DSN is correct: %s", userError(err))
	}
	oldest, err := opened.store.QueueOldestPendingAt(ctx)
	if err != nil {
		return a.fail("could not read oldest pending timestamps; verify migrations have run and the DSN is correct: %s", userError(err))
	}

	queues := collectQueueNames(pending, dlq, oldest)
	if len(queues) == 0 {
		_, _ = fmt.Fprintln(a.stdout, "No queues found. Pending and dead-letter counts are both zero.")
		return exitOK
	}

	tw := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "QUEUE\tPENDING\tDLQ\tOLDEST_PENDING\tBACKLOG_AGE")
	now := time.Now()
	for _, q := range queues {
		oldestText := "-"
		ageText := "-"
		if ts, ok := oldest[q]; ok && !ts.IsZero() {
			oldestText = ts.UTC().Format(time.RFC3339)
			ageText = roundDuration(now.Sub(ts))
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\n", q, pending[q], dlq[q], oldestText, ageText)
	}
	if err := tw.Flush(); err != nil {
		return a.fail("could not write queues output: %s", userError(err))
	}
	return exitOK
}

func (a app) runDLQ(ctx context.Context, globals globalOptions, args []string) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(a.stderr, dlqUsageText)
		_, _ = fmt.Fprintln(a.stderr, "\nmissing dlq subcommand; valid commands: list, requeue")
		return exitError
	}
	switch args[0] {
	case "list":
		return a.runDLQList(ctx, globals, args[1:])
	case "requeue":
		return a.runDLQRequeue(ctx, globals, args[1:])
	case "help", "-h", "--help":
		_, _ = fmt.Fprint(a.stdout, dlqUsageText)
		return exitOK
	default:
		return a.fail("unknown dlq command %q; valid dlq commands: list, requeue", args[0])
	}
}

func (a app) runDLQList(ctx context.Context, globals globalOptions, args []string) int {
	var filter jobs.DeadLetterFilter
	var metadata metadataFlags
	var jsonOutput, idsOnly bool
	fs := a.newFlagSet("dlq list", `List dead-lettered jobs and print a table, JSON array, or one job ID per line.

Usage:
  sdj --driver sqlite --dsn ./jobs.db dlq list [--queue q] [--tenant t] [--metadata k=v] [--type t] [--limit n] [--json|--ids-only]
  sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --queue default --tenant acme
`)
	fs.StringVar(&filter.Queue, "queue", "", "Filter by queue")
	fs.StringVar(&filter.Type, "type", "", "Filter by job type")
	fs.StringVar(&filter.Tenant, "tenant", "", "Filter by tenant")
	fs.Var(&metadata, "metadata", "Filter by metadata key=value; repeat for multiple pairs")
	fs.IntVar(&filter.Limit, "limit", 50, "Maximum jobs to list")
	fs.IntVar(&filter.Offset, "offset", 0, "Jobs to skip")
	fs.BoolVar(&jsonOutput, "json", false, "Print jobs as a JSON array")
	fs.BoolVar(&idsOnly, "ids-only", false, "Print one job ID per line")
	if code := a.parseSubcommand(fs, args); code != exitOK {
		if code == exitHandled {
			return exitOK
		}
		return code
	}
	if fs.NArg() != 0 {
		return a.fail("dlq list accepts no positional arguments")
	}
	if filter.Limit <= 0 {
		return a.fail("dlq list --limit must be greater than zero")
	}
	if filter.Offset < 0 {
		return a.fail("dlq list --offset must be zero or greater")
	}
	if jsonOutput && idsOnly {
		return a.fail("dlq list accepts only one machine-readable mode; choose --json or --ids-only")
	}
	filter.MetaContains = metadata.metadataMap()

	opened, ok := a.openStore(globals)
	if !ok {
		return exitError
	}
	defer closeStore(opened)
	dead, err := opened.store.ListDeadLettered(ctx, filter)
	if err != nil {
		return a.fail("could not list dead-lettered jobs; verify migrations have run and filters are valid: %s", userError(err))
	}

	if idsOnly {
		for _, job := range dead {
			_, _ = fmt.Fprintln(a.stdout, job.ID)
		}
		return exitOK
	}
	if jsonOutput {
		if err := json.NewEncoder(a.stdout).Encode(dead); err != nil {
			return a.fail("could not write JSON output: %s", userError(err))
		}
		return exitOK
	}
	if len(dead) == 0 {
		_, _ = fmt.Fprintln(a.stdout, "No dead-lettered jobs found.")
		return exitOK
	}

	tw := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ID\tQUEUE\tTYPE\tPRIORITY\tATTEMPT\tMAX_RETRIES\tDEAD_LETTERED_AT\tLAST_ERROR\tREASON")
	for _, job := range dead {
		deadAt := "-"
		if job.DeadLetteredAt != nil {
			deadAt = job.DeadLetteredAt.UTC().Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\n",
			job.ID,
			job.Queue,
			job.Type,
			job.Priority,
			job.Attempt,
			job.MaxRetries,
			deadAt,
			oneLine(job.LastError),
			oneLine(job.DeadLetterReason),
		)
	}
	if err := tw.Flush(); err != nil {
		return a.fail("could not write dlq list output: %s", userError(err))
	}
	return exitOK
}

func (a app) runDLQRequeue(ctx context.Context, globals globalOptions, args []string) int {
	var filter jobs.DeadLetterFilter
	fs := a.newFlagSet("dlq requeue", `Requeue one dead-lettered job by id, or requeue a filtered dead-lettered batch.

Usage:
  sdj --driver sqlite --dsn ./jobs.db dlq requeue <jobID>
  sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq requeue <jobID>
  sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq requeue --queue emails --tenant acme

Bulk exit codes (--queue/--tenant):
  0   every matching job was requeued
  3   no dead-lettered job matched the filter (typo'd filter, or already drained)
  4   some matching jobs were skipped and are still dead-lettered
  1   a storage error stopped the run (the partial counts are still printed)
`)
	fs.StringVar(&filter.Queue, "queue", "", "Bulk requeue dead-lettered jobs in this queue")
	fs.StringVar(&filter.Tenant, "tenant", "", "Bulk requeue dead-lettered jobs for this tenant")
	if code := a.parseSubcommand(fs, args); code != exitOK {
		if code == exitHandled {
			return exitOK
		}
		return code
	}
	if fs.NArg() > 1 {
		return a.fail("dlq requeue accepts at most one job id")
	}
	if fs.NArg() == 1 && (filter.Queue != "" || filter.Tenant != "") {
		return a.fail("dlq requeue accepts either one job id or --queue/--tenant filters, not both")
	}
	if fs.NArg() == 0 && filter.Queue == "" && filter.Tenant == "" {
		return a.fail("dlq requeue requires one job id or at least one bulk filter: --queue or --tenant")
	}
	opened, ok := a.openStore(globals)
	if !ok {
		return exitError
	}
	defer closeStore(opened)
	if fs.NArg() == 0 {
		return a.runDLQRequeueBulk(ctx, opened.store, filter)
	}
	jobID := fs.Arg(0)
	parsedJobID, err := jobs.ParseUUID(jobID)
	if err != nil {
		return a.fail("invalid job id %q: %s", jobID, userError(err))
	}
	requeued, err := opened.store.Requeue(ctx, parsedJobID)
	if err != nil {
		return a.fail("could not requeue job %q; verify the job is not a fan-out sub-job and storage is healthy: %s", jobID, userError(err))
	}
	if !requeued {
		return a.fail("job %q was not requeued; verify the ID exists and the job is failed or cancelled", jobID)
	}
	_, _ = fmt.Fprintf(a.stdout, "requeued %s\n", jobID)
	return exitOK
}

// runDLQRequeueBulk requeues every dead-lettered job matching the filter that
// CAN be requeued, and reports the ones that cannot.
//
// Two rules make it finish the run instead of wedging on one row:
//
//   - A fan-out sub-job is rejected by Requeue (ErrCannotRequeueSubJob) BY
//     DESIGN — replay goes through the parent. That is a property of one row, not
//     a failure of the command, so it is counted and skipped. Aborting on it made
//     the whole subcommand permanently useless: ListDeadLettered orders by
//     (dead_lettered_at DESC, id DESC), so the same sub-job was hit at the same
//     position on every re-run and no job sorting after it could EVER be bulk
//     requeued.
//   - Rows that stay dead-lettered advance the page offset. Requeued rows leave
//     the result set, so paging can only make progress on the ones that do not —
//     without this a full page of unrequeueable rows re-reads the same page
//     forever.
//
// The summary line is printed on EVERY exit, including the error exit: a run that
// requeued 900 of 1000 jobs and then hit a storage error used to print nothing at
// all, so the operator could not tell what had already happened.
// countStillDeadLettered reports how many of the given jobs are still in the
// dead-letter result set, which is what the paging cursor must step over.
func countStillDeadLettered(ctx context.Context, store *jobs.GormStorage, page []*jobs.Job) (int, error) {
	if len(page) == 0 {
		return 0, nil
	}
	ids := make([]jobs.UUID, 0, len(page))
	for _, job := range page {
		ids = append(ids, job.ID)
	}
	var n int64
	if err := store.DB().WithContext(ctx).Model(&jobs.Job{}).
		Where("id IN ?", ids).
		Where("dead_lettered_at IS NOT NULL").
		Count(&n).Error; err != nil {
		return 0, err
	}
	return int(n), nil
}

func (a app) runDLQRequeueBulk(ctx context.Context, store *jobs.GormStorage, filter jobs.DeadLetterFilter) int {
	const batchLimit = 1000
	filter.Limit = batchLimit
	filter.Offset = 0

	// matched counts the rows the LISTING handed back across all pages. Only its
	// zero-ness is used, so the paging cursor's step-over semantics cannot skew it.
	var requeued, skippedSubJobs, skippedOther, matched int
	report := func() {
		_, _ = fmt.Fprintf(a.stdout, "requeued %d jobs\n", requeued)
		if skippedSubJobs > 0 {
			_, _ = fmt.Fprintf(a.stdout, "skipped %d fan-out sub-jobs (requeue their parent instead)\n", skippedSubJobs)
		}
		if skippedOther > 0 {
			_, _ = fmt.Fprintf(a.stdout, "skipped %d jobs that could not be requeued (no longer failed or cancelled, or already gone)\n", skippedOther)
		}
	}

	for {
		dead, err := store.ListDeadLettered(ctx, filter)
		if err != nil {
			report()
			return a.fail("could not list dead-lettered jobs for bulk requeue; verify migrations have run and filters are valid: %s", userError(err))
		}
		if len(dead) == 0 {
			break
		}
		matched += len(dead)
		for _, job := range dead {
			ok, err := store.Requeue(ctx, job.ID)
			switch {
			case errors.Is(err, jobs.ErrCannotRequeueSubJob):
				skippedSubJobs++
			case err != nil:
				report()
				return a.fail("could not requeue job %q; verify storage is healthy: %s", job.ID, userError(err))
			case ok:
				requeued++
			default:
				skippedOther++
			}
		}
		// Advance the cursor by exactly the rows from this page that are STILL in
		// the dead-letter set, because those are the rows the next page will hand
		// back. Deriving that from the Requeue outcome is not possible: it returns
		// (false, nil) BOTH for a row that is gone and for a row that is present but
		// not in a requeuable state, and those need opposite treatment.
		//
		// Counting every non-requeued row (the original) over-advances past rows
		// that no longer exist, silently skipping exactly that many genuine
		// dead-lettered jobs on the next page — with exit 0 and a success message.
		// Counting none of them under-advances and re-reads the same page forever
		// whenever a still-present row cannot be requeued. Both are wrong, so ask
		// the database instead of inferring.
		remaining, err := countStillDeadLettered(ctx, store, dead)
		if err != nil {
			report()
			return a.fail("could not confirm dead-letter progress: %s", userError(err))
		}
		if len(dead) < batchLimit {
			break
		}
		filter.Offset += remaining
	}

	report()
	// Three outcomes, three exit codes — see exitNoMatch/exitPartial. The
	// distinction is made from what the LISTING returned, not from what Requeue
	// reported: a filter that matched nothing at all and a filter whose every match
	// was unrequeueable need opposite operator responses (check the filter vs.
	// investigate the rows), and both used to exit 0 alongside a clean drain.
	switch {
	case matched == 0:
		// EXIT 0, not a distinct code. "Nothing matched" includes the ordinary
		// already-drained queue, and the runbook's own example is
		// `sdj dlq requeue --queue "$Q" && clear-alert` — returning non-zero there
		// would stop clearing the alert on a healthy queue and would abort a
		// `set -e` cron. A skeptic caught that this is a user-visible CLI break
		// shipping in a PATCH release. Distinguishability comes from a line on
		// STDERR, which scripts can capture and humans can read, without changing
		// what every existing invocation returns.
		// report() has already printed "requeued 0 jobs"; do not repeat it.
		_, _ = fmt.Fprintln(a.stderr, "note: no dead-lettered jobs matched the filter — check --queue/--tenant, or the queue is already drained")
		return exitOK
	case skippedSubJobs > 0 || skippedOther > 0:
		_, _ = fmt.Fprintln(a.stdout, "some matching jobs were not requeued; see the skipped counts above")
		return exitPartial
	}
	return exitOK
}

func (a app) runHealth(ctx context.Context, globals globalOptions, args []string) int {
	fs := a.newFlagSet("health", `Ping storage and print OK on success.

Usage:
  sdj --driver sqlite --dsn ./jobs.db health
  sdj --driver postgres --dsn postgres://user:pass@localhost/db health
  sdj --driver mysql --dsn 'user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27' health
`)
	if code := a.parseSubcommand(fs, args); code != exitOK {
		if code == exitHandled {
			return exitOK
		}
		return code
	}
	if fs.NArg() != 0 {
		return a.fail("health accepts no arguments")
	}
	opened, ok := a.openStore(globals)
	if !ok {
		return exitError
	}
	defer closeStore(opened)
	if err := opened.store.Ping(ctx); err != nil {
		return a.fail("storage health check failed; verify the database is reachable and credentials are valid: %s", userError(err))
	}
	_, _ = fmt.Fprintln(a.stdout, "OK")
	return exitOK
}

func (a app) newFlagSet(name, description string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), description)
		_, _ = fmt.Fprintln(fs.Output(), "\nOptions:")
		fs.PrintDefaults()
	}
	return fs
}

func (a app) parseSubcommand(fs *flag.FlagSet, args []string) int {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitHandled
		}
		return exitError
	}
	return exitOK
}

func (a app) openStore(opts globalOptions) (openedStore, bool) {
	opened, err := openStore(opts)
	if err != nil {
		_, _ = fmt.Fprintln(a.stderr, err)
		return openedStore{}, false
	}
	return opened, true
}

func (a app) fail(format string, args ...any) int {
	_, _ = fmt.Fprintf(a.stderr, format+"\n", args...)
	return exitError
}

func openStore(opts globalOptions) (openedStore, error) {
	if strings.TrimSpace(opts.dsn) == "" {
		return openedStore{}, fmt.Errorf("--dsn is required for driver %q; expected %s", opts.driver, dsnHint(opts.driver))
	}

	var dialector gorm.Dialector
	switch opts.driver {
	case "sqlite":
		dialector = sqlite.Open(jobs.SafeSQLiteDSN(opts.dsn))
	case "postgres":
		dialector = postgres.Open(opts.dsn)
	case "mysql":
		cfg, err := mysqlcfg.ParseDSN(opts.dsn)
		if err != nil {
			return openedStore{}, fmt.Errorf("could not parse MySQL DSN; expected %s; %s", dsnHint(opts.driver), userError(err))
		}
		cfg.Loc = time.UTC
		if cfg.Params == nil {
			cfg.Params = make(map[string]string)
		}
		cfg.Params["time_zone"] = "'+00:00'"
		dialector = gormmysql.Open(cfg.FormatDSN())
	default:
		return openedStore{}, fmt.Errorf("unknown --driver %q; valid drivers: sqlite, postgres, mysql", opts.driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return openedStore{}, fmt.Errorf("could not connect using driver %q; expected %s; %s", opts.driver, dsnHint(opts.driver), userError(err))
	}
	sqlDB, err := db.DB()
	if err != nil {
		return openedStore{}, fmt.Errorf("could not initialize database handle; verify the DSN uses %s: %s", dsnHint(opts.driver), userError(err))
	}
	if err := jobs.ConfigurePool(db); err != nil {
		_ = sqlDB.Close()
		return openedStore{}, fmt.Errorf("could not configure database pool; verify the database connection supports pooling: %s", userError(err))
	}
	store, err := jobs.NewGormStorageWithPool(db)
	if err != nil {
		_ = sqlDB.Close()
		return openedStore{}, fmt.Errorf("could not initialize job storage; verify migrations and database permissions: %s", userError(err))
	}
	return openedStore{store: store, close: sqlDB.Close}, nil
}

func closeStore(opened openedStore) {
	if opened.close != nil {
		_ = opened.close()
	}
}

func dsnHint(driver string) string {
	switch driver {
	case "sqlite":
		return "a file path like ./jobs.db"
	case "postgres":
		return "postgres://user:pass@host:5432/db"
	case "mysql":
		return "user:pass@tcp(host:3306)/db?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27"
	default:
		return "one of: sqlite file path like ./jobs.db; postgres postgres://user:pass@host:5432/db; mysql user:pass@tcp(host:3306)/db?parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27"
	}
}

func userError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	msg = strings.ReplaceAll(msg, "\n", " ")
	msg = strings.Join(strings.Fields(msg), " ")
	if len(msg) > 240 {
		msg = msg[:240] + "..."
	}
	return msg
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if s == "" {
		return "-"
	}
	return s
}

func collectQueueNames(pending, dlq map[string]int, oldest map[string]time.Time) []string {
	seen := make(map[string]bool, len(pending)+len(dlq)+len(oldest))
	for q := range pending {
		seen[q] = true
	}
	for q := range dlq {
		seen[q] = true
	}
	for q := range oldest {
		seen[q] = true
	}
	queues := make([]string, 0, len(seen))
	for q := range seen {
		queues = append(queues, q)
	}
	sort.Strings(queues)
	return queues
}

func roundDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return "0s"
	}
	if d < time.Minute {
		return d.Truncate(time.Second).String()
	}
	if d < time.Hour {
		return d.Truncate(time.Minute).String()
	}
	return d.Truncate(time.Hour).String()
}
