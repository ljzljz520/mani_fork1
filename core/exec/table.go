package exec

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/theckman/yacspin"

	"github.com/alajmo/mani/core"
	"github.com/alajmo/mani/core/dao"
)

const (
	// defaultCellOutputBudget bounds the combined stdout/stderr bytes retained
	// in a single table cell. Output beyond the budget is still drained but
	// discarded.
	defaultCellOutputBudget int64 = 1 << 20 // 1 MiB

	// defaultBatchOutputBudget bounds the total output bytes retained across
	// all cells of a single table run.
	defaultBatchOutputBudget int64 = 32 << 20 // 32 MiB
)

// outputBudget is a concurrency-safe, decrement-only byte budget that can be
// shared by several output streams (the stdout/stderr pair of one cell, or
// every cell in a batch).
type outputBudget struct {
	remaining int64 // atomic
	limit     int64
}

func newOutputBudget(limit int64) *outputBudget {
	return &outputBudget{remaining: limit, limit: limit}
}

// take reserves up to n bytes. It never blocks and never reserves more than
// what is still available, so concurrent streams sharing a budget together
// never exceed it.
func (b *outputBudget) take(n int) int {
	if n <= 0 {
		return 0
	}

	for {
		cur := atomic.LoadInt64(&b.remaining)
		if cur <= 0 {
			return 0
		}

		grant := cur
		if int64(n) < grant {
			grant = int64(n)
		}

		if atomic.CompareAndSwapInt64(&b.remaining, cur, cur-grant) {
			return int(grant)
		}
	}
}

// refund gives back bytes reserved with take that ended up unused.
func (b *outputBudget) refund(n int64) {
	if n > 0 {
		atomic.AddInt64(&b.remaining, n)
	}
}

// runBudgets wires the per-cell limit together with the budget shared by an
// entire table run.
type runBudgets struct {
	cellBytes  int64
	batchBytes int64
	batch      *outputBudget
}

func newRunBudgets(cellBytes, batchBytes int64) *runBudgets {
	return &runBudgets{
		cellBytes:  cellBytes,
		batchBytes: batchBytes,
		batch:      newOutputBudget(batchBytes),
	}
}

// streamSink drains a single pipe (stdout or stderr). Bytes covered by both
// the per-cell and the per-batch budget are retained, everything else is
// counted and dropped. Write always consumes the full chunk, so io.Copy keeps
// draining the pipe even after every budget is exhausted: the child process
// can never block on a full OS pipe buffer and retained memory stays bounded.
type streamSink struct {
	cell  *outputBudget
	batch *outputBudget

	buf      []byte
	offered  int64
	retained int64
}

func (s *streamSink) Write(p []byte) (int, error) {
	n := len(p)
	s.offered += int64(n)

	// A byte is retained only while both budgets can cover it. Reserve from
	// the batch first and return anything the cell budget cannot take.
	batchGrant := int64(s.batch.take(n))
	cellGrant := int64(s.cell.take(int(batchGrant)))
	if cellGrant < batchGrant {
		s.batch.refund(batchGrant - cellGrant)
	}

	if cellGrant > 0 {
		s.buf = append(s.buf, p[:int(cellGrant)]...)
		s.retained += cellGrant
	}

	return n, nil
}

func (s *streamSink) dropped() int64 {
	return s.offered - s.retained
}

func (exec *Exec) Table(runFlags *core.RunFlags) dao.TableOutput {
	return exec.tableWithBudgets(runFlags, newRunBudgets(defaultCellOutputBudget, defaultBatchOutputBudget))
}

func (exec *Exec) tableWithBudgets(runFlags *core.RunFlags, budgets *runBudgets) dao.TableOutput {
	task := exec.Tasks[0]
	clients := exec.Clients
	projects := exec.Projects

	var spinner *yacspin.Spinner
	var spinnerErr error
	go func() {
		if !runFlags.Silent {
			time.Sleep(500 * time.Millisecond)
			spinner, spinnerErr = initSpinner()
		}
	}()

	// In-case user interrupts, make sure spinner is stopped
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan

		if !runFlags.Silent && spinner != nil && spinnerErr == nil {
			_ = spinner.Stop()
		}
		os.Exit(0)
	}()

	var data dao.TableOutput
	var dataMutex = sync.RWMutex{}

	/**
	** Headers
	**/
	data.Headers = append(data.Headers, "project")

	// Append Command names if set
	for _, subTask := range task.Commands {
		if subTask.Name != "" {
			data.Headers = append(data.Headers, subTask.Name)
		} else {
			data.Headers = append(data.Headers, "output")
		}
	}

	// Append Command name if set
	if task.Cmd != "" {
		if task.Name != "" {
			data.Headers = append(data.Headers, task.Name)
		} else {
			data.Headers = append(data.Headers, "output")
		}
	}

	// Populate the rows (project name is first cell, then commands and cmd output is set to empty string)
	for i, p := range projects {
		data.Rows = append(data.Rows, dao.Row{Columns: []string{p.Name}})

		for range task.Commands {
			data.Rows[i].Columns = append(data.Rows[i].Columns, "")
		}

		if task.Cmd != "" {
			data.Rows[i].Columns = append(data.Rows[i].Columns, "")
		}
	}

	wg := core.NewSizedWaitGroup(task.SpecData.Forks)
	/**
	** Values
	**/
	for i, c := range clients {
		wg.Add()
		if task.SpecData.Parallel {
			go func(i int, c Client, wg *core.SizedWaitGroup) {
				defer wg.Done()
				_ = exec.TableWork(i, runFlags.DryRun, data, &dataMutex, budgets)
			}(i, c, &wg)
		} else {
			func(i int, c Client, wg *core.SizedWaitGroup) {
				defer wg.Done()
				_ = exec.TableWork(i, runFlags.DryRun, data, &dataMutex, budgets)
			}(i, c, &wg)
		}
	}
	wg.Wait()

	if !runFlags.Silent && spinner != nil && spinnerErr == nil {
		_ = spinner.Stop()
	}

	return data
}

func (exec *Exec) TableWork(
	rIndex int,
	dryRun bool,
	data dao.TableOutput,
	dataMutex *sync.RWMutex,
	budgets *runBudgets,
) error {
	client := exec.Clients[rIndex]
	task := exec.Tasks[rIndex]

	for j, cmd := range task.Commands {
		args := TableCmd{
			rIndex: rIndex,
			cIndex: j + 1,
			client: client,
			dryRun: dryRun,
			shell:  cmd.ShellProgram,
			env:    cmd.EnvList,
			cmd:    cmd.Cmd,
			cmdArr: cmd.CmdArg,
		}

		if cmd.TTY {
			return ExecTTY(cmd.Cmd, cmd.EnvList)
		}

		err := RunTableCmd(args, data, dataMutex, budgets)
		if err != nil && !task.SpecData.IgnoreErrors {
			return err
		}
	}

	if task.Cmd != "" {
		args := TableCmd{
			rIndex: rIndex,
			cIndex: len(task.Commands) + 1,
			client: client,
			dryRun: dryRun,
			shell:  task.ShellProgram,
			env:    task.EnvList,
			cmd:    task.Cmd,
			cmdArr: task.CmdArg,
		}

		if task.TTY {
			return ExecTTY(task.Cmd, task.EnvList)
		}

		err := RunTableCmd(args, data, dataMutex, budgets)
		if err != nil && !task.SpecData.IgnoreErrors {
			return err
		}
	}

	return nil
}

func RunTableCmd(
	t TableCmd,
	data dao.TableOutput,
	dataMutex *sync.RWMutex,
	budgets *runBudgets,
) error {
	combinedEnvs := dao.MergeEnvs(t.client.Env, t.env)

	if t.dryRun {
		dataMutex.Lock()
		data.Rows[t.rIndex].Columns[t.cIndex] = t.cmd
		dataMutex.Unlock()
		return nil
	}

	if err := t.client.Run(t.shell, combinedEnvs, t.cmdArr); err != nil {
		return err
	}

	// One per-cell budget shared by both streams so the combined retained
	// output never exceeds the cell limit.
	cellBudget := newOutputBudget(budgets.cellBytes)
	stdoutSink := &streamSink{cell: cellBudget, batch: budgets.batch}
	stderrSink := &streamSink{cell: cellBudget, batch: budgets.batch}

	// Drain stdout and stderr concurrently and independently. Neither
	// goroutine touches the table lock while reading, so a child writing
	// heavily to one stream can never stall the other stream's drain (holding
	// the table lock across io.ReadAll used to deadlock the whole batch).
	var drainWg sync.WaitGroup
	drain := func(reader io.Reader, sink *streamSink) {
		defer drainWg.Done()
		if _, err := io.Copy(sink, reader); err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
	}

	drainWg.Add(2)
	go drain(t.client.Stdout(), stdoutSink)
	go drain(t.client.Stderr(), stderrSink)
	drainWg.Wait()

	// Wait only after both pipes reached EOF (see os/exec StdoutPipe docs).
	waitErr := t.client.Wait()

	cell := buildCellContent(stdoutSink, stderrSink, waitErr, budgets)

	// Touch shared table state once, after capture is done. The lock is held
	// for a string assignment, never while waiting for pipe data, and each
	// command owns a unique (row, column) cell.
	dataMutex.Lock()
	data.Rows[t.rIndex].Columns[t.cIndex] = cell
	dataMutex.Unlock()

	if waitErr != nil {
		return waitErr
	}

	return nil
}

// buildCellContent merges captured streams into one table cell.
//
// Merge order is stable: stdout first, stderr second, with a single trailing
// newline trimmed from each stream (historical cell layout). When output
// exceeded a budget, the pipes were still drained to completion and the cell
// ends with an explicit truncation marker. Non-exit plumbing errors are
// surfaced in the cell like before.
func buildCellContent(
	stdoutSink *streamSink,
	stderrSink *streamSink,
	waitErr error,
	budgets *runBudgets,
) string {
	cell := fmt.Sprintf("%s%s",
		strings.TrimSuffix(string(stdoutSink.buf), "\n"),
		strings.TrimSuffix(string(stderrSink.buf), "\n"),
	)

	dropped := stdoutSink.dropped() + stderrSink.dropped()
	if dropped > 0 {
		// If this cell filled its own share of the budget, the cell limit was
		// hit; otherwise the shared batch budget ran out first.
		scope := "batch"
		limit := budgets.batchBytes
		if stdoutSink.retained+stderrSink.retained >= budgets.cellBytes {
			scope = "cell"
			limit = budgets.cellBytes
		}

		marker := fmt.Sprintf(
			"[mani: output truncated: %d bytes omitted (%s limit of %d bytes exceeded)]",
			dropped, scope, limit,
		)
		if cell != "" {
			cell += "\n"
		}
		cell += marker
	}

	if waitErr != nil {
		// Suppress Go's *exec.ExitError ("exit status N") — it's a stdlib
		// stringification of the process exit code that adds no information
		// the command's own stderr (already captured above) doesn't convey,
		// and it breaks --omit-empty-rows for commands that intentionally
		// exit non-zero (e.g. `grep -v` with no matches). Real mani-level
		// errors (I/O, plumbing) still surface in the cell.
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			if cell != "" {
				cell += "\n"
			}
			cell += waitErr.Error()
		}
	}

	return cell
}

func initSpinner() (*yacspin.Spinner, error) {
	spinner, err := dao.TaskSpinner()
	if err != nil {
		return &spinner, err
	}

	err = spinner.Start()
	if err != nil {
		return &spinner, err
	}

	spinner.Message(" Running")

	return &spinner, nil
}
