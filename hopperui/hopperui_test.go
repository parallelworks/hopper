package hopperui_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hopperui"
	"github.com/parallelworks/hopper/internal/testdb"
)

type ping struct {
	N int `json:"n"`
}

func (ping) Kind() string { return "ping" }

type stepArgs struct {
	Name string `json:"name"`
}

func (stepArgs) Kind() string { return "step" }

func TestUI(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	client, err := hopper.NewClient(hopperpgx.New(pool), nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := client.Insert(ctx, ping{N: 1}, &hopper.InsertOpts{Queue: "emails"})
	if err != nil {
		t.Fatal(err)
	}
	wf := hopper.NewWorkflow("ingest", nil)
	first := wf.Add(stepArgs{Name: "fetch"}, nil)
	second := wf.Add(stepArgs{Name: "parse"}, hopper.After(first))
	wf.Add(stepArgs{Name: "index"}, hopper.After(second))
	wf.Add(stepArgs{Name: "notify"}, hopper.After(first))
	wres, err := client.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}

	readOnly := httptest.NewServer(http.StripPrefix("/hopper", hopperui.New(client, &hopperui.Config{Prefix: "/hopper"})))
	defer readOnly.Close()
	denied := errors.New("not an admin")
	admin := httptest.NewServer(http.StripPrefix("/hopper", hopperui.New(client, &hopperui.Config{
		Prefix: "/hopper",
		Authorize: func(r *http.Request, action hopperui.Action) error {
			if r.Header.Get("X-Admin") == "" {
				return denied
			}
			return nil
		},
	})))
	defer admin.Close()

	get := func(srv *httptest.Server, path string) (int, string) {
		t.Helper()
		res, err := http.Get(srv.URL + "/hopper" + path)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}
	post := func(srv *httptest.Server, path string, form url.Values, admin bool) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/hopper"+path, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if admin {
			req.Header.Set("X-Admin", "1")
		}
		c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		res, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}
	want := func(code int, body string, gotCode int, needles ...string) {
		t.Helper()
		if gotCode != code {
			t.Errorf("status = %d, want %d: %s", gotCode, code, body)
		}
		for _, n := range needles {
			if !strings.Contains(body, n) {
				t.Errorf("body lacks %q:\n%s", n, body)
			}
		}
	}

	code, body := get(readOnly, "/")
	want(200, body, code, "emails", "read-only", `href="/hopper/jobs?queue=emails"`, `href="/hopper/static/style.css"`)
	code, body = get(readOnly, "/static/style.css")
	want(200, body, code, "svg.dag")
	code, body = get(readOnly, "/queues")
	want(200, body, code, "active")
	if strings.Contains(body, "/queues/emails/pause") {
		t.Error("read-only UI offers actions")
	}
	code, body = get(readOnly, "/jobs?queue=emails")
	want(200, body, code, job.Job.ID.String(), "ping", `selected>emails`)
	code, body = get(readOnly, "/jobs?state=pending")
	want(200, body, code, "step", wres.Jobs[1].Job.ID.String(), wres.Jobs[3].Job.ID.String())
	if strings.Contains(body, wres.Jobs[0].Job.ID.String()) || strings.Contains(body, "ping") {
		t.Error("state filter ignored")
	}
	code, body = get(readOnly, "/jobs/"+job.Job.ID.String())
	want(200, body, code, `&#34;n&#34;: 1`, "available", "emails")
	code, body = get(readOnly, "/jobs/not-an-id")
	want(400, body, code)
	code, body = get(readOnly, "/jobs/"+hopper.JobID{1}.String())
	want(404, body, code)
	code, body = get(readOnly, "/workflows/"+wres.ID.String())
	want(200, body, code, "Workflow ingest", `<svg class="dag"`, `<title>step {&#34;name&#34;: &#34;fetch&#34;}</title>`, `<title>step {&#34;name&#34;: &#34;notify&#34;}</title>`, `marker-end`, `unfinished <b>4</b>`)
	if n := strings.Count(body, `<line `); n != 3 {
		t.Errorf("edges drawn = %d, want 3", n)
	}
	code, body = get(readOnly, "/subscriptions")
	want(200, body, code, "no subscriptions")
	code, body = get(readOnly, "/consumers")
	want(200, body, code, "no consumers")
	code, body = get(readOnly, "/clients")
	want(200, body, code, "Clients")

	// Actions: refused without a hook, refused by the hook, then applied.
	code, body = post(readOnly, "/queues/emails/pause", nil, true)
	want(403, body, code, "read-only")
	code, body = post(admin, "/queues/emails/pause", nil, false)
	want(403, body, code, "not an admin")
	code, _ = post(admin, "/queues/emails/pause", nil, true)
	want(303, "", code)
	code, body = get(admin, "/queues?msg=x")
	want(200, body, code, "paused", "/queues/emails/resume")
	code, _ = post(admin, "/queues/emails/resume", nil, true)
	want(303, "", code)
	code, _ = post(admin, "/jobs/"+job.Job.ID.String()+"/cancel", nil, true)
	want(303, "", code)
	code, body = get(admin, "/jobs/"+job.Job.ID.String())
	want(200, body, code, "cancelled", "/retry")
	code, _ = post(admin, "/jobs/"+job.Job.ID.String()+"/retry", nil, true)
	want(303, "", code)
	code, body = get(admin, "/jobs/"+job.Job.ID.String())
	want(200, body, code, "available")
	code, body = post(admin, "/consumers/nobody/seek", url.Values{"to": {"earliest"}}, true)
	want(404, body, code)
	code, body = post(admin, "/consumers/nobody/seek", url.Values{"to": {"sideways"}}, true)
	want(400, body, code)
	code, body = post(admin, "/jobs/"+job.Job.ID.String()+"/explode", nil, true)
	want(404, body, code)
}
