package hopper_test

import (
	"context"
	"errors"
	"testing"

	"github.com/parallelworks/hopper"
)

// sendMail is the args type before its kind was renamed.
type sendMail struct {
	To string `json:"to"`
}

func (sendMail) Kind() string { return "send_mail" }

// sendEmail is sendMail after the rename to "send_email".
type sendEmail struct {
	To string `json:"to"`
}

func (sendEmail) Kind() string          { return "send_email" }
func (sendEmail) KindAliases() []string { return []string{"send_mail", "mail"} }

// Args types whose names collide with noop or sendEmail.
type (
	aliasesNoop   struct{}
	aliasesMail   struct{}
	kindIsMail    struct{}
	aliasesItself struct{}
	aliasesTwice  struct{}
	aliasesEmpty  struct{}
	ptrAliases    struct{}
	fresh         struct{}
)

func (aliasesNoop) Kind() string            { return "a" }
func (aliasesNoop) KindAliases() []string   { return []string{"fresh", "noop"} }
func (aliasesMail) Kind() string            { return "b" }
func (aliasesMail) KindAliases() []string   { return []string{"mail"} }
func (kindIsMail) Kind() string             { return "mail" }
func (aliasesItself) Kind() string          { return "c" }
func (aliasesItself) KindAliases() []string { return []string{"c"} }
func (aliasesTwice) Kind() string           { return "d" }
func (aliasesTwice) KindAliases() []string  { return []string{"x", "x"} }
func (aliasesEmpty) Kind() string           { return "e" }
func (aliasesEmpty) KindAliases() []string  { return []string{""} }
func (ptrAliases) Kind() string             { return "p" }
func (*ptrAliases) KindAliases() []string   { return []string{"noop"} }
func (fresh) Kind() string                  { return "fresh" }
func (fresh) KindAliases() []string {
	return []string{"a", "b", "c", "d", "e", "p", "x"}
}

func TestWorkersRejectKindAliasCollisions(t *testing.T) {
	t.Parallel()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[sendEmail]) error { return nil })

	panics := func(name string, add func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: registering did not panic", name)
			}
		}()
		add()
	}
	panics("alias of a registered kind", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[aliasesNoop]) error { return nil })
	})
	panics("alias of a registered alias", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[aliasesMail]) error { return nil })
	})
	panics("kind of a registered alias", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[kindIsMail]) error { return nil })
	})
	panics("alias of its own kind", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[aliasesItself]) error { return nil })
	})
	panics("repeated alias", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[aliasesTwice]) error { return nil })
	})
	panics("empty alias", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[aliasesEmpty]) error { return nil })
	})
	// KindAliases with a pointer receiver is read too, not dropped.
	panics("alias with a pointer receiver", func() {
		hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[ptrAliases]) error { return nil })
	})

	// A rejected registration leaves none of its names behind.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("a rejected registration left a name behind: %v", r)
		}
	}()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[fresh]) error { return nil })
}

func TestKindAliasWorksJobsOfTheOldKind(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()

	// Jobs inserted before the rename, by a client that cannot work them.
	old, err := h.client(&hopper.Config{}).Insert(ctx, sendMail{To: "old@example.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	type seen struct {
		kind, to string
	}
	got := make(chan seen, 2)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[sendEmail]) error {
		got <- seen{job.Kind, job.Args.To}
		return nil
	})
	c := h.started(workers, 2)
	renamed, err := c.Insert(ctx, sendEmail{To: "new@example.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []hopper.JobID{old.Job.ID, renamed.Job.ID} {
		waitForJob(t, c, id, hopper.JobStateCompleted)
	}
	want := map[seen]bool{{"send_mail", "old@example.com"}: true, {"send_email", "new@example.com"}: true}
	for range 2 {
		if s := <-got; !want[s] {
			t.Errorf("worker saw %+v", s)
		}
	}

	// StrictKinds accepts the old kind, since a worker here works it. The
	// job goes to a queue c does not work, so c cannot send it to got
	// ahead of the inline Work below.
	strict := h.client(&hopper.Config{Workers: workers, StrictKinds: true})
	if _, err := strict.Insert(ctx, sendMail{}, &hopper.InsertOpts{Queue: "unworked"}); err != nil {
		t.Errorf("StrictKinds rejected an aliased kind: %v", err)
	}
	var unknown *hopper.UnknownKindError
	if _, err := strict.Insert(ctx, unregistered{}, nil); !errors.As(err, &unknown) {
		t.Errorf("StrictKinds: err = %v", err)
	}

	// Workers.Work, which hoppertest.Work calls, resolves aliases as the
	// client does.
	row := &hopper.JobRow{Kind: "mail", Args: []byte(`{"to":"inline@example.com"}`)}
	if err := workers.Work(ctx, row, nil); err != nil {
		t.Fatal(err)
	}
	if s := <-got; s != (seen{"mail", "inline@example.com"}) {
		t.Errorf("inline worker saw %+v", s)
	}
}
