package exec

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gotable "github.com/jedib0t/go-pretty/v6/table"

	"github.com/alajmo/mani/core"
	"github.com/alajmo/mani/core/dao"
	"github.com/alajmo/mani/core/print"
)

func ptr[T any](v T) *T { return &v }

// shCommand builds a task sub-command executed through the system POSIX
// shell, available on both Linux (CI) and macOS.
func shCommand(name, cmd string) dao.Command {
	return dao.Command{
		Name:         name,
		Cmd:          cmd,
		ShellProgram: "sh",
		CmdArg:       []string{"-c", cmd},
	}
}

func newTableExec(t *testing.T, projectNames []string, taskFor func(name string) dao.Task) *Exec {
	t.Helper()

	clients := make([]Client, 0, len(projectNames))
	projects := make([]dao.Project, 0, len(projectNames))
	tasks := make([]dao.Task, 0, len(projectNames))

	base := t.TempDir()
	for _, name := range projectNames {
		dir := filepath.Join(base, name)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}

		clients = append(clients, Client{Name: name, Path: dir})
		projects = append(projects, dao.Project{Name: name, Path: name})
		tasks = append(tasks, taskFor(name))
	}

	return &Exec{Clients: clients, Projects: projects, Tasks: tasks}
}

// runTable executes a table capture and fails the test if it does not finish
// in time (a pipe-drain deadlock surfaces here as a timeout).
func runTable(t *testing.T, e *Exec, budgets *runBudgets) dao.TableOutput {
	t.Helper()

	if budgets == nil {
		budgets = newRunBudgets(defaultCellOutputBudget, defaultBatchOutputBudget)
	}

	flags := &core.RunFlags{Silent: true}
	done := make(chan dao.TableOutput, 1)
	go func() {
		done <- e.tableWithBudgets(flags, budgets)
	}()

	select {
	case data := <-done:
		return data
	case <-time.After(45 * time.Second):
		t.Fatal("table run timed out: likely deadlocked while draining stdout/stderr pipes")
		return dao.TableOutput{}
	}
}

// largeStreamCmd emits far more than one OS pipe buffer (64 KiB) on BOTH
// streams, interleaved. This is the output pattern that deadlocked when the
// table lock was held while reading stdout and stderr was never drained.
func largeStreamCmd(token string, lines int) string {
	return fmt.Sprintf(
		`i=0; while [ $i -lt %d ]; do echo %s-stdout-$i; echo %s-stderr-$i >&2; i=$((i+1)); done`,
		lines, token, token,
	)
}

// Multiple projects writing megabytes to both pipes concurrently must
// complete: streams drain independently without holding the table lock,
// output stays within per-cell/batch budgets, and rows never cross-talk.
func TestTableLargeDualStreamMultipleProjects(t *testing.T) {
	const lines = 100000 // ~3.6 MiB offered per project, well over the 1 MiB cell budget
	names := []string{"p0", "p1", "p2", "p3"}

	e := newTableExec(t, names, func(name string) dao.Task {
		return dao.Task{
			Name:     "heavy",
			SpecData: dao.Spec{Parallel: true, Forks: 4, IgnoreErrors: true},
			Commands: []dao.Command{shCommand("heavy", largeStreamCmd(name, lines))},
		}
	})

	data := runTable(t, e, nil)

	if got, want := len(data.Headers), 2; got != want {
		t.Fatalf("headers = %v, want %d columns", data.Headers, want)
	}
	if got, want := data.Headers[1], "heavy"; got != want {
		t.Fatalf("command header = %q, want %q", got, want)
	}
	if got, want := len(data.Rows), len(names); got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}

	totalCellBytes := 0
	for i, name := range names {
		row := data.Rows[i]
		if got := row.Columns[0]; got != name {
			t.Fatalf("row %d project cell = %q, want %q", i, got, name)
		}

		cell := row.Columns[1]
		token := name + "-"

		// The head of both streams is retained.
		if !strings.Contains(cell, token+"stdout-0") {
			t.Errorf("cell %s missing retained stdout head:\n%s", name, truncate(cell, 400))
		}
		if !strings.Contains(cell, token+"stderr-0") {
			t.Errorf("cell %s missing retained stderr head:\n%s", name, truncate(cell, 400))
		}

		// Stable merge rule: stdout precedes stderr.
		outAt := strings.Index(cell, token+"stdout-0")
		errAt := strings.Index(cell, token+"stderr-0")
		if outAt < 0 || errAt < 0 || outAt > errAt {
			t.Errorf("cell %s merge order wrong (stdout at %d, stderr at %d)", name, outAt, errAt)
		}

		// Truncation is explicitly marked.
		if !strings.Contains(cell, "mani: output truncated") {
			t.Errorf("cell %s missing truncation marker", name)
		}

		// No leakage from other projects/commands into this cell.
		for j, other := range names {
			if j != i && strings.Contains(cell, other+"-") {
				t.Errorf("cell %s contains output from project %s", name, other)
			}
		}

		// Retained memory stays within the cell budget plus marker slack.
		if len(cell) > int(defaultCellOutputBudget)+1024 {
			t.Errorf("cell %s retained %d bytes, want <= %d", name, len(cell), int(defaultCellOutputBudget)+1024)
		}
		totalCellBytes += len(cell)
	}

	// Batch-wide retained memory is bounded by forks*cell budget (the batch
	// budget here is larger, the cell budget is the effective constraint).
	if totalCellBytes > len(names)*int(defaultCellOutputBudget)+len(names)*1024 {
		t.Errorf("total retained bytes = %d, exceeds batch bound", totalCellBytes)
	}
}

// A small per-cell budget: the command still runs to completion (the sink
// keeps draining), only a budgeted prefix is retained and the marker names
// the cell limit.
func TestTableCellBudgetTruncation(t *testing.T) {
	const cellBytes int64 = 64 << 10 // 64 KiB
	cmd := `i=0; while [ $i -lt 20000 ]; do echo stdout-$i; echo stderr-$i >&2; i=$((i+1)); done`

	e := newTableExec(t, []string{"p"}, func(string) dao.Task {
		return dao.Task{
			Name:     "big",
			SpecData: dao.Spec{Parallel: false, Forks: 1},
			Commands: []dao.Command{shCommand("big", cmd)},
		}
	})

	data := runTable(t, e, newRunBudgets(cellBytes, defaultBatchOutputBudget))
	cell := data.Rows[0].Columns[1]

	if !strings.Contains(cell, "stdout-0") {
		t.Errorf("cell missing retained prefix:\n%s", truncate(cell, 400))
	}
	if !strings.Contains(cell, "cell limit of 65536 bytes exceeded") {
		t.Errorf("cell missing cell-limit marker:\n%s", truncate(cell, 400))
	}
	if len(cell) > int(cellBytes)+1024 {
		t.Errorf("cell retained %d bytes, want <= %d", len(cell), int(cellBytes)+1024)
	}
}

// The shared batch budget bounds total retained bytes across all projects;
// every over-budget cell is marked as batch-truncated while draining
// continues (no project hangs even though it retains almost nothing).
func TestTableBatchBudgetTruncation(t *testing.T) {
	const (
		cellBytes  int64 = 4 << 20 // 4 MiB per cell (not the active constraint)
		batchBytes int64 = 128 << 10
	)
	names := []string{"a", "b", "c"}

	e := newTableExec(t, names, func(name string) dao.Task {
		return dao.Task{
			Name:     "big",
			SpecData: dao.Spec{Parallel: true, Forks: 3},
			Commands: []dao.Command{shCommand("big", largeStreamCmd(name, 30000))},
		}
	})

	data := runTable(t, e, newRunBudgets(cellBytes, batchBytes))

	total := 0
	marked := 0
	for i, name := range names {
		cell := data.Rows[i].Columns[1]
		if data.Rows[i].Columns[0] != name {
			t.Fatalf("project column mismatch: %q != %q", data.Rows[i].Columns[0], name)
		}
		if strings.Contains(cell, "batch limit of 131072 bytes exceeded") {
			marked++
		}
		if strings.Contains(cell, "cell limit") {
			t.Errorf("project %s incorrectly marked as cell-limited", name)
		}
		total += len(cell)
	}

	if marked == 0 {
		t.Errorf("no cell marked batch-truncated")
	}
	// Only marker text/newline slack is retained beyond the shared batch bytes.
	if total > int(batchBytes)+len(names)*1024 {
		t.Errorf("batch retained %d bytes, want <= %d", total, int(batchBytes)+len(names)*1024)
	}
}

// Small-output merge behavior stays stable and trims exactly one trailing
// newline per stream, matching historical table cell contents.
func TestTableCellMergeStable(t *testing.T) {
	cases := []struct {
		cmd  string
		want string
	}{
		{`printf 'first\n'; printf 'second\n' >&2`, "firstsecond"}, // stdout before stderr, no separator
		{`printf 'only-out\n'`, "only-out"},
		{`printf 'only-err\n' >&2`, "only-err"},
		{`printf 'a\nb\n'`, "a\nb"}, // only the single trailing newline is trimmed
		{`true`, ""},                // no output at all
	}

	names := make([]string, len(cases))
	for i := range cases {
		names[i] = fmt.Sprintf("p%d", i)
	}

	e := newTableExec(t, names, func(name string) dao.Task {
		var cmd string
		for i, n := range names {
			if n == name {
				cmd = cases[i].cmd
			}
		}
		return dao.Task{
			Name:     "m",
			SpecData: dao.Spec{Parallel: true, Forks: uint32(len(names))},
			Commands: []dao.Command{shCommand("m", cmd)},
		}
	})

	data := runTable(t, e, nil)

	for i, c := range cases {
		if got := data.Rows[i].Columns[1]; got != c.want {
			t.Errorf("cmd %q: cell = %q, want %q", c.cmd, got, c.want)
		}
	}
}

// Failed commands keep their captured output in their column; with
// ignore_errors the following sub-command still executes into the next
// column, and without it the following column stays untouched.
func TestTableErrorFollowUp(t *testing.T) {
	cmd1 := `echo c1-out; echo c1-err >&2; exit 7`
	cmd2 := `echo c2-out`

	build := func(ignore bool) *Exec {
		return newTableExec(t, []string{"p"}, func(string) dao.Task {
			return dao.Task{
				Name: "two",
				SpecData: dao.Spec{
					Parallel:     false,
					Forks:        1,
					IgnoreErrors: ignore,
				},
				Commands: []dao.Command{
					shCommand("c1", cmd1),
					shCommand("c2", cmd2),
				},
			}
		})
	}

	// ignore_errors: both columns populated, exit status text never leaks
	// into the cell (only the command's own stderr).
	data := runTable(t, build(true), nil)
	row := data.Rows[0].Columns
	if got, want := row[1], "c1-outc1-err"; got != want {
		t.Errorf("ignore_errors c1 cell = %q, want %q", got, want)
	}
	if strings.Contains(row[1], "exit status") {
		t.Errorf("exit status leaked into cell: %q", row[1])
	}
	if got, want := row[2], "c2-out"; got != want {
		t.Errorf("ignore_errors c2 cell = %q, want %q (follow-up command must run)", got, want)
	}

	// Default: the failing command aborts remaining sub-commands for the row.
	data = runTable(t, build(false), nil)
	row = data.Rows[0].Columns
	if got, want := row[1], "c1-outc1-err"; got != want {
		t.Errorf("c1 cell = %q, want %q", got, want)
	}
	if got := row[2]; got != "" {
		t.Errorf("c2 cell = %q, want empty (follow-up must not run)", got)
	}

	// A command that cannot even start leaves its cell empty but does not
	// block later commands when errors are ignored.
	e := newTableExec(t, []string{"p"}, func(string) dao.Task {
		return dao.Task{
			Name:     "startfail",
			SpecData: dao.Spec{IgnoreErrors: true, Forks: 1},
			Commands: []dao.Command{
				{Name: "missing", ShellProgram: "/nonexistent/missing-shell", CmdArg: []string{"-c", "echo noop"}},
				shCommand("after", `echo after-ok`),
			},
		}
	})
	data = runTable(t, e, nil)
	row = data.Rows[0].Columns
	if got := row[1]; got != "" {
		t.Errorf("unstartable command cell = %q, want empty", got)
	}
	if got, want := row[2], "after-ok"; got != want {
		t.Errorf("follow-up cell = %q, want %q", got, want)
	}
}

// Per-project rows keep their per-command columns aligned when running in
// parallel: a cell only ever contains its own project's command output.
func TestTableColumnsDoNotCross(t *testing.T) {
	names := []string{"p0", "p1", "p2"}

	e := newTableExec(t, names, func(name string) dao.Task {
		return dao.Task{
			Name:     "two",
			SpecData: dao.Spec{Parallel: true, Forks: 3},
			Commands: []dao.Command{
				shCommand("alpha", fmt.Sprintf(`echo %s-alpha`, name)),
				shCommand("bravo", fmt.Sprintf(`echo %s-bravo; echo %s-bravo-err >&2`, name, name)),
			},
		}
	})

	data := runTable(t, e, nil)

	wantHeaders := []string{"project", "alpha", "bravo"}
	if strings.Join(data.Headers, ",") != strings.Join(wantHeaders, ",") {
		t.Fatalf("headers = %v, want %v", data.Headers, wantHeaders)
	}

	for i, name := range names {
		want := []string{name, name + "-alpha", name + "-bravo" + name + "-bravo-err"}
		got := data.Rows[i].Columns
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("row %d = %v, want %v", i, got, want)
		}

		for j, other := range names {
			if j != i {
				for _, cell := range got[1:] {
					if strings.Contains(cell, other+"-") {
						t.Errorf("cell of %s contains output of %s: %q", name, other, cell)
					}
				}
			}
		}
	}
}

func testTheme() dao.Theme {
	return dao.Theme{
		Name:  "test",
		Color: ptr(false),
		Table: dao.Table{
			Box: gotable.StyleBoxDefault,
			Border: &dao.Border{
				Around:  ptr(false),
				Columns: ptr(true),
				Header:  ptr(true),
				Rows:    ptr(true),
			},
			Header:      &dao.ColorOptions{},
			TitleColumn: &dao.ColorOptions{},
		},
	}
}

// Truncation markers and empty-row hiding behave identically for table,
// markdown and html rendering: the marker is plain text in every format and
// rows with only empty cells disappear when omit-empty-rows is set.
func TestTableRenderFormatsAndOmitEmptyRows(t *testing.T) {
	marker := "[mani: output truncated: 42 bytes omitted (cell limit of 64 bytes exceeded)]"
	rows := []dao.Row{
		{Columns: []string{"p1", "alpha-body\n" + marker, "bravo-body"}},
		{Columns: []string{"p2", "", ""}}, // empty row
		{Columns: []string{"p3", "charlie-body", ""}},
	}
	headers := []string{"project", "alpha", "bravo"}

	render := func(output string, omitEmptyRows bool) string {
		var buf bytes.Buffer
		options := print.PrintTableOptions{
			Theme:         testTheme(),
			Output:        output,
			Color:         false,
			AutoWrap:      false,
			OmitEmptyRows: omitEmptyRows,
		}
		print.PrintTable(rows, options, headers[0:1], headers[1:], &buf)
		return buf.String()
	}

	for _, output := range []string{"table", "markdown", "html"} {
		out := render(output, true)

		for _, want := range []string{"p1", "p3", "alpha-body", "bravo-body", "charlie-body", marker} {
			if !strings.Contains(out, want) {
				t.Errorf("%s output missing %q:\n%s", output, want, out)
			}
		}
		if strings.Contains(out, "p2") {
			t.Errorf("%s output should omit the empty p2 row:\n%s", output, out)
		}
	}

	if out := render("table", false); !strings.Contains(out, "p2") {
		t.Errorf("without omit-empty-rows the empty row should still render:\n%s", out)
	}
}

// streamSink enforces both budgets and keeps consuming (and counting) input
// after the budget is exhausted.
func TestStreamSinkBudgets(t *testing.T) {
	batch := newOutputBudget(10)
	cell := newOutputBudget(4)
	sink := &streamSink{cell: cell, batch: batch}

	if n, err := sink.Write([]byte("abcde")); err != nil || n != 5 {
		t.Fatalf("write = (%d, %v), want (5, nil)", n, err)
	}
	if got := string(sink.buf); got != "abcd" {
		t.Fatalf("retained = %q, want %q", got, "abcd")
	}
	if got := sink.dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}

	// Budgets exhausted: further writes are fully consumed but not retained,
	// which is what keeps the child pipe drained.
	if n, err := sink.Write([]byte("fghij")); err != nil || n != 5 {
		t.Fatalf("post-budget write = (%d, %v), want (5, nil)", n, err)
	}
	if got := string(sink.buf); got != "abcd" {
		t.Fatalf("retained changed after exhaustion: %q", got)
	}
	if got := sink.dropped(); got != 6 {
		t.Fatalf("dropped = %d, want 6", got)
	}

	// The unused batch byte was refunded to the shared batch budget.
	if got := atomic.LoadInt64(&batch.remaining); got != 6 {
		t.Fatalf("batch remaining = %d, want 6", got)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
