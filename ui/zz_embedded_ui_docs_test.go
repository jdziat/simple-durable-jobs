package ui

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1/jobsv1connect"
)

// This file guards docs/content/docs/embedded-ui.md — the page an operator lands
// on to wire up the dashboard — against the four ways round 40 found it lying:
//
//  1. The "UIStorage Interface" listing was not the shipped interface. It gave
//     RetryJob/DeleteJob a `jobID string` (they take core.UUID) and omitted
//     GetWorkflowRoots entirely. Because UIStorage is discovered by a TYPE
//     ASSERTION, a storage written from that listing produces no compile error
//     anywhere — it just silently loses the enhanced path, so ListJobs returns
//     Unimplemented and the job browser is permanently empty.
//  2. "…a Connect-RPC service (jobs.v1.JobsService) with 12 methods" plus a
//     12-row table. There are 19; the 7 unlisted included CancelJob, PauseQueue
//     and ResumeQueue — every pause/cancel mutation the endpoint exposes.
//  3. "The history can be viewed over three periods" while the shipped
//     dashboard ships four and the same page called 30d "the dashboard's
//     longest throughput window" nine sections earlier.
//  4. The copy-paste "## Setup" example configured neither WithAuthorizer nor
//     WithInsecureAllowUnauthenticated, and ui.Handler fails closed — so the
//     quickstart produced a dashboard on which every RPC is PermissionDenied.
//
// Each check below derives the truth mechanically (reflection, the generated
// proto descriptor, the parsed doc source, an actual served RPC) rather than by
// eye, because reading is what failed here for many releases.
//
// docs/ and ui/frontend/ are outside the published module zip, so these are
// repo-checkout guards; CI runs them (test.Dockerfile COPYs the whole tree and
// the package list includes ./ui).

const (
	embeddedUIDocPath   = "../docs/content/docs/embedded-ui.md"
	authorizationDoc    = "../docs/content/docs/advanced/authorization.md"
	dashboardSveltePath = "frontend/src/routes/Dashboard.svelte"
)

func readEmbeddedUIDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(embeddedUIDocPath)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it rather than deleting it", embeddedUIDocPath)
	return string(b)
}

// goBlockAfter returns the contents of the first ```go fence that follows anchor.
func goBlockAfter(t *testing.T, md, anchor string) string {
	t.Helper()
	i := strings.Index(md, anchor)
	require.GreaterOrEqualf(t, i, 0, "anchor %q not found in %s", anchor, embeddedUIDocPath)
	const open = "```go\n"
	j := strings.Index(md[i:], open)
	require.GreaterOrEqualf(t, j, 0, "no ```go block after %q", anchor)
	rest := md[i+j+len(open):]
	k := strings.Index(rest, "```")
	require.GreaterOrEqualf(t, k, 0, "unterminated ```go block after %q", anchor)
	return rest[:k]
}

// stripQualifiers removes package qualifiers ("core.UUID" -> "UUID") so a doc
// that writes jobsv1.QueueStats and a reflect rendering that writes
// v1.QueueStats compare equal, while string-vs-UUID still differs.
var qualifier = regexp.MustCompile(`\b[A-Za-z_][A-Za-z0-9_]*\.`)

func stripQualifiers(s string) string { return qualifier.ReplaceAllString(s, "") }

// TestEmbeddedUIDocUIStorageBlockIsTheShippedInterface parses the interface the
// page prints and compares its method set, method by method and type by type,
// with reflection over the real ui.UIStorage. Methods inherited from the
// embedded core.Storage are excluded from both sides, since the doc writes that
// embedding as `core.Storage` rather than expanding it.
//
// Protects: docs/content/docs/embedded-ui.md, "### UIStorage Interface".
func TestEmbeddedUIDocUIStorageBlockIsTheShippedInterface(t *testing.T) {
	block := goBlockAfter(t, readEmbeddedUIDoc(t), "### UIStorage Interface")
	require.Contains(t, block, "core.Storage",
		"the documented interface must still embed core.Storage")

	documented := parseDocumentedInterface(t, block, "UIStorage")
	shipped := reflectedInterfaceMethods(t,
		reflect.TypeOf((*UIStorage)(nil)).Elem(),
		reflect.TypeOf((*core.Storage)(nil)).Elem())

	require.Equal(t, shipped, documented,
		"the UIStorage listing on embedded-ui.md does not match the shipped interface. "+
			"UIStorage is discovered by a type assertion, so a storage written from a wrong "+
			"listing fails NO compile check — it silently falls back and the job browser dies. "+
			"Update the page in the same commit as the interface.")
}

// parseDocumentedInterface renders the interface named in a documented Go block
// as "Name(argType, …) (resultType, …)" per method, qualifiers stripped, sorted.
func parseDocumentedInterface(t *testing.T, block, name string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "doc.go", "package doc\n"+block, parser.SkipObjectResolution)
	require.NoErrorf(t, err, "the ```go block is not parseable Go; it is printed as a Go interface, so it must be")

	var iface *ast.InterfaceType
	ast.Inspect(file, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != name {
			return true
		}
		iface, _ = ts.Type.(*ast.InterfaceType)
		return false
	})
	require.NotNilf(t, iface, "the documented block declares no interface named %s", name)

	var out []string
	for _, field := range iface.Methods.List {
		fn, ok := field.Type.(*ast.FuncType)
		if !ok || len(field.Names) != 1 {
			continue // an embedded interface (core.Storage), handled separately
		}
		out = append(out, field.Names[0].Name+renderFuncType(fn))
	}
	sort.Strings(out)
	return out
}

func renderFuncType(fn *ast.FuncType) string {
	render := func(fl *ast.FieldList) []string {
		var ts []string
		if fl == nil {
			return ts
		}
		for _, f := range fl.List {
			n := len(f.Names)
			if n == 0 {
				n = 1
			}
			for i := 0; i < n; i++ {
				ts = append(ts, stripQualifiers(types.ExprString(f.Type)))
			}
		}
		return ts
	}
	return "(" + strings.Join(render(fn.Params), ", ") + ") (" + strings.Join(render(fn.Results), ", ") + ")"
}

// reflectedInterfaceMethods renders iface's methods the same way, dropping any
// method also present on embedded.
func reflectedInterfaceMethods(t *testing.T, iface, embedded reflect.Type) []string {
	t.Helper()
	skip := make(map[string]struct{}, embedded.NumMethod())
	for i := 0; i < embedded.NumMethod(); i++ {
		skip[embedded.Method(i).Name] = struct{}{}
	}

	var out []string
	for i := 0; i < iface.NumMethod(); i++ {
		m := iface.Method(i)
		if _, ok := skip[m.Name]; ok {
			continue
		}
		ft := m.Type
		var in, res []string
		for j := 0; j < ft.NumIn(); j++ {
			in = append(in, stripQualifiers(ft.In(j).String()))
		}
		for j := 0; j < ft.NumOut(); j++ {
			res = append(res, stripQualifiers(ft.Out(j).String()))
		}
		out = append(out, m.Name+"("+strings.Join(in, ", ")+") ("+strings.Join(res, ", ")+")")
	}
	require.NotEmpty(t, out, "reflection found no UIStorage-specific methods; the guard would be vacuous")
	sort.Strings(out)
	return out
}

// TestEmbeddedUIDocRPCTableMatchesServiceDescriptor derives the RPC list from the
// generated proto descriptor and requires the page's stated count and its table
// rows to agree with it. Round 40: the page said 12 and listed 12; there are 19,
// and the 7 it omitted were every pause/cancel/resume mutation.
//
// Protects: docs/content/docs/embedded-ui.md, "## Connect-RPC API".
func TestEmbeddedUIDocRPCTableMatchesServiceDescriptor(t *testing.T) {
	md := readEmbeddedUIDoc(t)
	real := serviceMethodNames(t)

	sentence := regexp.MustCompile(`service \(` + "`" + `jobs\.v1\.JobsService` + "`" + `\) with (\d+) methods`)
	m := sentence.FindStringSubmatch(md)
	require.Lenf(t, m, 2, "embedded-ui.md no longer states the method count in the expected form")
	require.Equalf(t, len(real), atoiOrFail(t, m[1]),
		"embedded-ui.md states %s methods; jobs.proto declares %d (%v)", m[1], len(real), real)

	// Every declared RPC must appear as a `| `Name` |` row in the API table.
	section := md[strings.Index(md, "## Connect-RPC API"):]
	if end := strings.Index(section[len("## Connect-RPC API"):], "\n## "); end >= 0 {
		section = section[:end+len("## Connect-RPC API")]
	}
	rows := regexp.MustCompile("(?m)^\\| `([A-Za-z]+)` +\\|").FindAllStringSubmatch(section, -1)
	documented := make([]string, 0, len(rows))
	for _, r := range rows {
		documented = append(documented, r[1])
	}
	sort.Strings(documented)
	require.Equal(t, real, documented,
		"the Connect-RPC table on embedded-ui.md does not enumerate exactly the RPCs jobs.proto "+
			"declares. Mounting ui.Handler exposes all of them, so an operator auditing the "+
			"endpoint's mutating surface reads this table.")
}

func atoiOrFail(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		require.True(t, c >= '0' && c <= '9')
		n = n*10 + int(c-'0')
	}
	return n
}

func serviceMethodNames(t *testing.T) []string {
	t.Helper()
	services := jobsv1.File_jobs_v1_jobs_proto.Services()
	require.Equal(t, 1, services.Len(), "jobs.proto is expected to declare exactly one service")
	methods := services.Get(0).Methods()
	out := make([]string, 0, methods.Len())
	for i := 0; i < methods.Len(); i++ {
		out = append(out, string(methods.Get(i).Name()))
	}
	sort.Strings(out)
	return out
}

// TestEmbeddedUIDocThroughputPeriodsMatchTheDashboard derives the period set from
// the shipped Svelte dashboard, proves the server treats each as a first-class
// window, and then requires the page to name all of them.
//
// Protects: docs/content/docs/embedded-ui.md, "### Historical Charts" and the
// GetStatsHistory row of the Connect-RPC table.
func TestEmbeddedUIDocThroughputPeriodsMatchTheDashboard(t *testing.T) {
	svelte, err := os.ReadFile(dashboardSveltePath)
	require.NoErrorf(t, err, "cannot read %s", dashboardSveltePath)

	decl := regexp.MustCompile(`const periods: Period\[\] = \[([^\]]*)\]`).FindSubmatch(svelte)
	require.Lenf(t, decl, 2, "%s no longer declares `const periods: Period[] = [...]`", dashboardSveltePath)

	var periods []string
	for _, raw := range strings.Split(string(decl[1]), ",") {
		if p := strings.Trim(strings.TrimSpace(raw), "'\""); p != "" {
			periods = append(periods, p)
		}
	}
	require.NotEmpty(t, periods)

	// Each period the UI offers must be a distinct server-side window, or the
	// selector would be decoration. This is what makes 30d "first-class".
	until := time.Now()
	seen := map[time.Duration]string{}
	for _, p := range periods {
		since, _ := parsePeriod(p)
		width := until.Sub(since).Round(time.Minute)
		prev, dup := seen[width]
		require.Falsef(t, dup, "periods %q and %q resolve to the same window (%s)", prev, p, width)
		seen[width] = p
		require.Positivef(t, periodBucket(p), "period %q has no bucket width", p)
	}

	md := readEmbeddedUIDoc(t)
	charts := docSectionOf(t, md, "### Historical Charts")

	// Parse the enumerating SENTENCE, not the whole section: the section also
	// discusses 30d in prose, so a section-wide substring check would still
	// pass with 30d dropped from the list — which is exactly the drift that
	// shipped ("three periods: 1 hour, 24 hours, and 7 days").
	sentence := regexp.MustCompile(`can be viewed over ([a-z]+) periods: ([^.]*)\.`).FindStringSubmatch(charts)
	require.Lenf(t, sentence, 3,
		"the Historical Charts section must enumerate the periods as "+
			"\"can be viewed over <count> periods: `1h` …, `24h` ….\"; found:\n%s", charts)

	require.Equalf(t, len(periods), numberWords[sentence[1]],
		"the sentence says %q periods; the dashboard ships %d (%v)", sentence[1], len(periods), periods)

	var listed []string
	for _, m := range regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(sentence[2], -1) {
		listed = append(listed, m[1])
	}
	sortedPeriods := append([]string(nil), periods...)
	sort.Strings(sortedPeriods)
	sort.Strings(listed)
	require.Equalf(t, sortedPeriods, listed,
		"the Historical Charts sentence must list exactly the periods %s ships", dashboardSveltePath)

	// The GetStatsHistory table row is the other place a programmatic client reads.
	row := regexp.MustCompile("(?m)^\\| `GetStatsHistory` .*$").FindString(md)
	require.NotEmpty(t, row, "embedded-ui.md no longer has a GetStatsHistory table row")
	for _, p := range periods {
		require.Containsf(t, row, "`"+p+"`", "the GetStatsHistory row must list period %q", p)
	}
}

// TestEmbeddedUIDocOptionsSectionListsEveryHandlerOption derives the option set
// from ui/options.go — every exported function whose sole result is an Option —
// and requires the page's "## Options" section both to name each one and to give
// each its own "### With…" subsection.
//
// The section used to enumerate five options and omit WithAuthorizer,
// WithInsecureAllowUnauthenticated, WithAllowedOrigins, WithMetadataRedaction
// and WithScheduleOverdueThreshold, so nothing on the dedicated dashboard page
// pointed at the fail-closed gate its own quickstart tripped over.
//
// Protects: docs/content/docs/embedded-ui.md, "## Options".
func TestEmbeddedUIDocOptionsSectionListsEveryHandlerOption(t *testing.T) {
	options := parseOptionConstructors(t)
	require.NotEmpty(t, options, "found no Option constructors; the guard would be vacuous")

	md := readEmbeddedUIDoc(t)
	section := docSectionOf(t, md, "## Options")

	for _, name := range options {
		if name == "WithInsecureAllowUnauthenticatedWrites" {
			// Deprecated alias; the page mentions it as such rather than giving
			// it a subsection of its own.
			require.Containsf(t, section, name, "the deprecated alias %s must still be mentioned", name)
			continue
		}
		require.Containsf(t, section, "### "+name+"\n",
			"the Options section must document ui.%s with its own subsection; an option that is "+
				"not on this page is an option nobody finds", name)
	}

	// And the documented default for the one option whose default is printed as
	// a number here.
	require.Equal(t, time.Minute, DefaultScheduleOverdueThreshold,
		"embedded-ui.md prints this default as \"1 minute\"; change both together")
	require.Contains(t, section, "The default is 1 minute")
}

// parseOptionConstructors returns the names of exported ui functions whose only
// result is an Option.
func parseOptionConstructors(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "options.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "cannot parse ui/options.go")

	var out []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !fn.Name.IsExported() {
			continue
		}
		res := fn.Type.Results
		if res == nil || len(res.List) != 1 {
			continue
		}
		if ident, ok := res.List[0].Type.(*ast.Ident); ok && ident.Name == "Option" {
			out = append(out, fn.Name.Name)
		}
	}
	sort.Strings(out)
	return out
}

// numberWords lets the guard compare the doc's spelled-out count ("four
// periods") against a derived length. Zero for an unknown word, which fails.
var numberWords = map[string]int{
	"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
	"six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}

func docSectionOf(t *testing.T, md, heading string) string {
	t.Helper()
	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	lines := strings.Split(md, "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == heading {
			require.Equalf(t, -1, start, "heading %q is not unique", heading)
			start = i
		}
	}
	require.GreaterOrEqualf(t, start, 0, "heading %q not found", heading)
	for i := start + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		if h := len(trimmed) - len(strings.TrimLeft(trimmed, "#")); h <= level {
			return strings.Join(lines[start:i], "\n")
		}
	}
	return strings.Join(lines[start:], "\n")
}

// TestEmbeddedUIDocSetupExampleProducesAWorkingDashboard is the behavioural half:
// it proves ui.Handler fails closed, and that the option the page's quickstart
// now passes is what lifts the gate. The text half requires the Setup block to
// actually pass one of the two opt-ins.
//
// Protects: docs/content/docs/embedded-ui.md, "## Setup".
func TestEmbeddedUIDocSetupExampleProducesAWorkingDashboard(t *testing.T) {
	setup := goBlockAfter(t, readEmbeddedUIDoc(t), "## Setup")
	require.Truef(t,
		strings.Contains(setup, "ui.WithAuthorizer(") || strings.Contains(setup, "ui.WithInsecureAllowUnauthenticated()"),
		"the Setup quickstart must configure ui.WithAuthorizer or ui.WithInsecureAllowUnauthenticated; "+
			"ui.Handler fails closed, so without one the copied example yields a dashboard on which "+
			"EVERY RPC returns PermissionDenied and the SPA renders empty")

	require.Equal(t, connect.CodePermissionDenied, getStatsCodeFor(t /* no auth option */),
		"ui.Handler must fail closed; if this changed, the Setup callout and authorization.md are both stale")
	require.NotEqual(t, connect.CodePermissionDenied, getStatsCodeFor(t, WithInsecureAllowUnauthenticated()),
		"the option the Setup example passes must actually lift the gate")
}

// getStatsCodeFor mounts ui.Handler exactly as the docs do (under a StripPrefix
// subpath) and returns the Connect code GetStats answers with.
func getStatsCodeFor(t *testing.T, opts ...Option) connect.Code {
	t.Helper()
	store := setupGormStorage(t)
	mux := http.NewServeMux()
	mux.Handle("/jobs/", http.StripPrefix("/jobs", Handler(store, opts...)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := jobsv1connect.NewJobsServiceClient(srv.Client(), srv.URL+"/jobs")
	_, err := client.GetStats(context.Background(), connect.NewRequest(&jobsv1.GetStatsRequest{}))
	if err == nil {
		return 0
	}
	return connect.CodeOf(err)
}

// TestAuthorizationDocDoesNotPromiseADefaultActionForUnknownRPCs pins the
// fail-closed classification the Dashboard Authorization page describes. The
// page used to say "Unknown read RPCs default to ui.ActionViewJobs so new
// procedures are not left ungated" — the code does the opposite and denies them
// before the authorizer and before the insecure-allow branch, so an operator
// debugging a PermissionDenied looked in their own ActionViewJobs branch, where
// no fix can go.
//
// Protects: docs/content/docs/advanced/authorization.md, "## Actions".
func TestAuthorizationDocDoesNotPromiseADefaultActionForUnknownRPCs(t *testing.T) {
	_, known := actionForProcedure("/jobs.v1.JobsService/SomeFutureRPC")
	require.False(t, known, "an unmapped procedure must not be classified")

	b, err := os.ReadFile(authorizationDoc)
	require.NoErrorf(t, err, "cannot read %s", authorizationDoc)
	doc := string(b)

	require.NotContains(t, doc, "default to `ui.ActionViewJobs`",
		"unmapped procedures are denied outright, not defaulted to a read action")
	require.Contains(t, doc, "There is **no default action**",
		"the page must state the fail-closed rule an operator will hit")
}

// TestAuthorizationDocActionTablesMatchTheClassifier derives BOTH sides
// mechanically — the RPC list from the generated service descriptor, the
// Action constant names from ui/authorization.go's const block, and the
// RPC-to-action mapping from actionForProcedure — and requires the two action
// tables on authorization.md to agree with all three.
//
// It replaces eyeballing in two directions at once. An RPC missing from the
// tables is not cosmetic: an unclassified procedure is hard-denied on EVERY
// path, including under WithInsecureAllowUnauthenticated, so the operator's
// only lead is this page.
//
// Protects: docs/content/docs/advanced/authorization.md, both "## Actions" tables.
func TestAuthorizationDocActionTablesMatchTheClassifier(t *testing.T) {
	b, err := os.ReadFile(authorizationDoc)
	require.NoErrorf(t, err, "cannot read %s", authorizationDoc)
	doc := string(b)

	constValue := parseActionConstants(t)
	require.NotEmpty(t, constValue)

	// Parse rows of the form: | `ui.ActionViewStats` | `GetStats`, `GetJob` |
	row := regexp.MustCompile("(?m)^\\| `ui\\.(Action[A-Za-z]+)` +\\| (.+?) \\|\\s*$")
	documented := map[string]Action{} // RPC name -> action value
	matches := row.FindAllStringSubmatch(doc, -1)
	require.NotEmpty(t, matches, "authorization.md no longer has parseable action tables")
	for _, m := range matches {
		value, ok := constValue[m[1]]
		require.Truef(t, ok, "authorization.md names ui.%s, which is not an Action constant in ui/authorization.go", m[1])
		for _, rpc := range regexp.MustCompile("`([A-Za-z]+)`").FindAllStringSubmatch(m[2], -1) {
			prev, dup := documented[rpc[1]]
			require.Falsef(t, dup, "RPC %s is listed twice (as %s and %s)", rpc[1], prev, value)
			documented[rpc[1]] = value
		}
	}

	services := jobsv1.File_jobs_v1_jobs_proto.Services()
	require.Equal(t, 1, services.Len())
	svc := services.Get(0)
	methods := svc.Methods()
	require.Positive(t, methods.Len())

	real := map[string]Action{}
	for i := 0; i < methods.Len(); i++ {
		name := string(methods.Get(i).Name())
		action, known := actionForProcedure("/" + string(svc.FullName()) + "/" + name)
		require.Truef(t, known,
			"procedure %s is not classified; it would be hard-denied on every path, "+
				"including under WithInsecureAllowUnauthenticated", name)
		real[name] = action
	}

	require.Equal(t, real, documented,
		"authorization.md's action tables must map exactly the RPCs jobs.proto declares, "+
			"to exactly the actions actionForProcedure assigns them")
}

// parseActionConstants reads ui/authorization.go's const block and returns
// constant name -> value, so the doc can name Go identifiers while the code
// compares string values.
func parseActionConstants(t *testing.T) map[string]Action {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "authorization.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "cannot parse ui/authorization.go")

	out := map[string]Action{}
	ast.Inspect(file, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
			return true
		}
		lit, ok := vs.Values[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if !strings.HasPrefix(vs.Names[0].Name, "Action") {
			return true
		}
		out[vs.Names[0].Name] = Action(strings.Trim(lit.Value, `"`))
		return true
	})
	return out
}

var _ protoreflect.MethodDescriptor // keeps the protoreflect import honest for readers
