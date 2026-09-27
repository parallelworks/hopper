// Package hopperui is a web UI for hopper: an http.Handler that browses
// queues, jobs, history, workflows, subscriptions, stream consumers and
// clients, and can retry, cancel, pause, resume and seek when the
// application allows it.
//
//	mux.Handle("/hopper/", http.StripPrefix("/hopper", hopperui.New(client, &hopperui.Config{
//	    Prefix:    "/hopper",
//	    Authorize: func(r *http.Request, action hopperui.Action) error { return requireAdmin(r) },
//	})))
//
// The UI is server-rendered HTML with no scripts and no external assets.
// Without an Authorize hook it is read-only.
package hopperui

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/parallelworks/hopper"
)

//go:embed templates/ui.html
var templateFS embed.FS

//go:embed static/style.css
var styleCSS []byte

// Action is something the UI does to the cluster on the operator's behalf.
type Action string

// The actions Config.Authorize is asked about.
const (
	ActionRetry  Action = "retry"
	ActionCancel Action = "cancel"
	ActionPause  Action = "pause"
	ActionResume Action = "resume"
	ActionSeek   Action = "seek"
)

// Config tunes the UI. A nil *Config means the defaults.
type Config struct {
	// Prefix is the path the handler is mounted under ("/hopper"), used to
	// build links. Mount with http.StripPrefix so the handler sees paths
	// without it.
	Prefix string
	// Authorize is asked before every action; returning an error refuses
	// it with 403. With no hook the UI is read-only.
	Authorize func(r *http.Request, action Action) error
	// Title is shown in the header. Defaults to "hopper".
	Title string
	// PageSize is how many jobs a page lists. Defaults to 50.
	PageSize int
}

// New returns the UI handler for a client. The client need not be started;
// an insert-only client works.
func New[TTx any](client *hopper.Client[TTx], cfg *Config) http.Handler {
	u := &ui[TTx]{client: client}
	if cfg != nil {
		u.cfg = *cfg
	}
	u.cfg.Prefix = strings.TrimSuffix(u.cfg.Prefix, "/")
	if u.cfg.Title == "" {
		u.cfg.Title = "hopper"
	}
	if u.cfg.PageSize <= 0 {
		u.cfg.PageSize = 50
	}
	u.tmpl = template.Must(template.New("ui").Funcs(template.FuncMap{
		"since":  since,
		"ts":     stamp,
		"pretty": prettyJSON,
		"dur":    func(d time.Duration) string { return d.Round(time.Second).String() },
		"now":    time.Now,
		"short":  shorten,
	}).ParseFS(templateFS, "templates/ui.html"))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", u.index)
	mux.HandleFunc("GET /queues", u.queues)
	mux.HandleFunc("POST /queues/{name}/{action}", u.queueAction)
	mux.HandleFunc("GET /jobs", u.jobs)
	mux.HandleFunc("GET /jobs/{id}", u.job)
	mux.HandleFunc("POST /jobs/{id}/{action}", u.jobAction)
	mux.HandleFunc("GET /workflows/{id}", u.workflow)
	mux.HandleFunc("GET /subscriptions", u.subscriptions)
	mux.HandleFunc("GET /consumers", u.consumers)
	mux.HandleFunc("POST /consumers/{name}/seek", u.seek)
	mux.HandleFunc("GET /clients", u.clients)
	mux.HandleFunc("GET /static/style.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		http.ServeContent(w, r, "style.css", time.Time{}, bytes.NewReader(styleCSS))
	})
	u.mux = mux
	return u
}

type ui[TTx any] struct {
	client *hopper.Client[TTx]
	cfg    Config
	tmpl   *template.Template
	mux    *http.ServeMux
}

func (u *ui[TTx]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mux.ServeHTTP(w, r)
}

// page is what every template receives.
type page struct {
	Prefix  string
	Title   string
	Page    string
	CanAct  bool
	Data    any
	Query   url.Values
	Message string
}

func (u *ui[TTx]) render(w http.ResponseWriter, r *http.Request, name string, data any) {
	p := page{Prefix: u.cfg.Prefix, Title: u.cfg.Title, Page: name, CanAct: u.cfg.Authorize != nil, Data: data, Query: r.URL.Query(), Message: r.URL.Query().Get("msg")}
	var buf bytes.Buffer
	if err := u.tmpl.ExecuteTemplate(&buf, "layout", p); err != nil {
		http.Error(w, "hopperui: render "+name+": "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (u *ui[TTx]) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, hopper.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, context.Canceled):
		http.Error(w, "cancelled", 499)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// authorize refuses an action unless the application allows it.
func (u *ui[TTx]) authorize(w http.ResponseWriter, r *http.Request, action Action) bool {
	if u.cfg.Authorize == nil {
		http.Error(w, "hopperui is read-only: no Authorize hook", http.StatusForbidden)
		return false
	}
	if err := u.cfg.Authorize(r, action); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return false
	}
	return true
}

// back redirects to a page with a message.
func (u *ui[TTx]) back(w http.ResponseWriter, r *http.Request, path, msg string) {
	// The target is the configured prefix plus a path built from route
	// constants and parsed IDs, never a request value.
	http.Redirect(w, r, u.cfg.Prefix+path+"?msg="+url.QueryEscape(msg), http.StatusSeeOther) //nolint:gosec // see above
}

// Pages.

type indexData struct {
	Stats  *hopper.Stats
	Queues []queueView
}

type queueView struct {
	Name   string
	Stats  *hopper.QueueStats
	Row    *hopper.QueueRow
	Paused bool
}

func (u *ui[TTx]) queueViews(ctx context.Context) (*hopper.Stats, []queueView, error) {
	stats, err := u.client.Stats(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows, err := u.client.Queues().List(ctx)
	if err != nil {
		return nil, nil, err
	}
	byName := map[string]*hopper.QueueRow{}
	for _, q := range rows {
		byName[q.Name] = q
	}
	names := map[string]struct{}{}
	for n := range stats.Queues {
		names[n] = struct{}{}
	}
	for n := range byName {
		names[n] = struct{}{}
	}
	views := make([]queueView, 0, len(names))
	for n := range names {
		v := queueView{Name: n, Stats: stats.Queues[n], Row: byName[n]}
		if v.Stats == nil {
			v.Stats = &hopper.QueueStats{}
		}
		v.Paused = v.Stats.Paused || (v.Row != nil && !v.Row.PausedAt.IsZero())
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return stats, views, nil
}

func (u *ui[TTx]) index(w http.ResponseWriter, r *http.Request) {
	stats, queues, err := u.queueViews(r.Context())
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "index", indexData{Stats: stats, Queues: queues})
}

func (u *ui[TTx]) queues(w http.ResponseWriter, r *http.Request) {
	stats, queues, err := u.queueViews(r.Context())
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "queues", indexData{Stats: stats, Queues: queues})
}

func (u *ui[TTx]) queueAction(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var err error
	switch r.PathValue("action") {
	case "pause":
		if !u.authorize(w, r, ActionPause) {
			return
		}
		err = u.client.Queues().Pause(r.Context(), name)
	case "resume":
		if !u.authorize(w, r, ActionResume) {
			return
		}
		err = u.client.Queues().Resume(r.Context(), name)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		u.fail(w, err)
		return
	}
	u.back(w, r, "/queues", "queue "+name+" "+r.PathValue("action")+"d")
}

type jobsData struct {
	Jobs   []*hopper.JobRow
	Queues []string
	States []hopper.JobState
	Filter hopper.JobFilter
	Kind   string
	State  string
	Next   string
}

var allStates = []hopper.JobState{
	hopper.JobStatePending, hopper.JobStateAvailable, hopper.JobStateScheduled, hopper.JobStateRunning,
	hopper.JobStateRetryable, hopper.JobStateCompleted, hopper.JobStateCancelled, hopper.JobStateDiscarded,
}

func (u *ui[TTx]) jobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	filter := hopper.JobFilter{Queue: q.Get("queue")}
	if k := q.Get("kind"); k != "" {
		filter.Kinds = []string{k}
	}
	if s := q.Get("state"); s != "" {
		filter.States = []hopper.JobState{hopper.JobState(s)}
	}
	if a := q.Get("after"); a != "" {
		id, err := hopper.ParseJobID(a)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		filter.After = id
	}
	data := jobsData{Filter: filter, Kind: q.Get("kind"), State: q.Get("state"), States: allStates}
	for job, err := range u.client.Jobs(ctx, filter) {
		if err != nil {
			u.fail(w, err)
			return
		}
		data.Jobs = append(data.Jobs, job)
		if len(data.Jobs) == u.cfg.PageSize {
			break
		}
	}
	if len(data.Jobs) == u.cfg.PageSize {
		q.Set("after", data.Jobs[len(data.Jobs)-1].ID.String())
		data.Next = u.cfg.Prefix + "/jobs?" + q.Encode()
	}
	if _, views, err := u.queueViews(ctx); err == nil {
		for _, v := range views {
			data.Queues = append(data.Queues, v.Name)
		}
	}
	u.render(w, r, "jobs", data)
}

type jobData struct {
	Job    *hopper.JobRow
	Live   bool
	Errors []hopper.AttemptError
}

func (u *ui[TTx]) job(w http.ResponseWriter, r *http.Request) {
	id, err := hopper.ParseJobID(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	job, err := u.client.JobGet(r.Context(), id)
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "job", jobData{Job: job, Live: !job.State.Terminal(), Errors: job.Errors})
}

func (u *ui[TTx]) jobAction(w http.ResponseWriter, r *http.Request) {
	id, err := hopper.ParseJobID(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var job *hopper.JobRow
	switch r.PathValue("action") {
	case "retry":
		if !u.authorize(w, r, ActionRetry) {
			return
		}
		job, err = u.client.JobRetry(r.Context(), id)
	case "cancel":
		if !u.authorize(w, r, ActionCancel) {
			return
		}
		job, err = u.client.JobCancel(r.Context(), id)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		u.fail(w, err)
		return
	}
	u.back(w, r, "/jobs/"+id.String(), fmt.Sprintf("job is now %s", job.State))
}

// workflowData lays a workflow out as a DAG: each job goes in the column
// after its furthest dependency, and edges are drawn between boxes.
type workflowData struct {
	Workflow *hopper.WorkflowRow
	Nodes    []node
	Edges    []edge
	Width    int
	Height   int
}

type node struct {
	Job  *hopper.JobRow
	X, Y int
}

type edge struct {
	X1, Y1, X2, Y2 int
}

const (
	nodeW, nodeH = 220, 44
	gapX, gapY   = 60, 20
)

func (u *ui[TTx]) workflow(w http.ResponseWriter, r *http.Request) {
	id, err := hopper.ParseJobID(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	wf, err := u.client.WorkflowGet(r.Context(), id)
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "workflow", layout(wf))
}

func layout(wf *hopper.WorkflowRow) workflowData {
	index := map[hopper.JobID]int{}
	for i, j := range wf.Jobs {
		index[j.ID] = i
	}
	deps := map[hopper.JobID][]hopper.JobID{}
	for _, e := range wf.Edges {
		deps[e.Job] = append(deps[e.Job], e.DependsOn)
	}
	// Level = longest path from a root. Jobs are in insertion order and a
	// step only depends on earlier steps, but iterate to a fixed point in
	// case a graph came from elsewhere.
	level := make([]int, len(wf.Jobs))
	for changed := true; changed; {
		changed = false
		for i, j := range wf.Jobs {
			for _, d := range deps[j.ID] {
				if k, ok := index[d]; ok && level[k]+1 > level[i] {
					level[i] = level[k] + 1
					changed = true
				}
			}
		}
	}
	rows := map[int]int{}
	data := workflowData{Workflow: wf, Nodes: make([]node, len(wf.Jobs))}
	for i, j := range wf.Jobs {
		x := gapX + level[i]*(nodeW+gapX)
		y := gapY + rows[level[i]]*(nodeH+gapY)
		rows[level[i]]++
		data.Nodes[i] = node{Job: j, X: x, Y: y}
		data.Width = max(data.Width, x+nodeW+gapX)
		data.Height = max(data.Height, y+nodeH+gapY)
	}
	for _, e := range wf.Edges {
		from, to := index[e.DependsOn], index[e.Job]
		f, t := data.Nodes[from], data.Nodes[to]
		data.Edges = append(data.Edges, edge{X1: f.X + nodeW, Y1: f.Y + nodeH/2, X2: t.X, Y2: t.Y + nodeH/2})
	}
	return data
}

func (u *ui[TTx]) subscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := u.client.Subscriptions(r.Context())
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "subscriptions", subs)
}

func (u *ui[TTx]) consumers(w http.ResponseWriter, r *http.Request) {
	consumers, err := u.client.Streams().Consumers(r.Context())
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "consumers", consumers)
}

func (u *ui[TTx]) seek(w http.ResponseWriter, r *http.Request) {
	if !u.authorize(w, r, ActionSeek) {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := r.PathValue("name")
	var opts hopper.SeekOpts
	switch r.Form.Get("to") {
	case "earliest":
		opts.Earliest = true
	case "latest":
		opts.Latest = true
	case "time":
		t, err := time.ParseInLocation("2006-01-02T15:04", r.Form.Get("time"), time.Local)
		if err != nil {
			http.Error(w, "time: "+err.Error(), http.StatusBadRequest)
			return
		}
		opts.Time = t
	case "position":
		p, err := hopper.ParseStreamPosition(r.Form.Get("position"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		opts.Position = p
	default:
		http.Error(w, "to: one of earliest, latest, time or position", http.StatusBadRequest)
		return
	}
	if err := u.client.Streams().Seek(r.Context(), name, opts); err != nil {
		u.fail(w, err)
		return
	}
	u.back(w, r, "/consumers", "consumer "+name+" moved")
}

func (u *ui[TTx]) clients(w http.ResponseWriter, r *http.Request) {
	clients, err := u.client.Clients(r.Context())
	if err != nil {
		u.fail(w, err)
		return
	}
	u.render(w, r, "clients", clients)
}

// Template helpers.

func since(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	if d < 0 {
		return "in " + humanDuration(-d)
	}
	return humanDuration(d) + " ago"
}

func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// shorten fits a JSON document on a node label.
func shorten(b json.RawMessage) string {
	s := string(b)
	if len(s) > 30 {
		return s[:29] + "…"
	}
	return s
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func prettyJSON(b json.RawMessage) string {
	if len(b) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return string(b)
	}
	return buf.String()
}
