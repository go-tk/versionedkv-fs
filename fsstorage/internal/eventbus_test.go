package internal_test

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-tk/testcase"
	. "github.com/go-tk/versionedkv-fs/fsstorage/internal"
	"github.com/stretchr/testify/assert"
)

func TestEventBus_Open(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type Output struct {
		ErrIsNotNil bool
	}
	type State = EventBusDetails
	type Context struct {
		EB EventBus

		Init           Init
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{}
	}).Setup(func(t *testing.T, c *Context) {
		c.EB.Init(c.Init.Options)
	}).Run(func(t *testing.T, c *Context) {
		err := c.EB.Open()
		var output Output
		output.ErrIsNotNil = err != nil
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.EB.Inspect()
		assert.Equal(t, c.ExpectedState, state)
		if err == nil {
			err := c.EB.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Then("should succeed").
			PreSetup(func(t *testing.T, c *Context) {
				c.Init.Options.EventDirName = "."
			}),
		tc.Copy().
			When("given event dir does not exist").
			Then("should fail").
			PreSetup(func(t *testing.T, c *Context) {
				c.Init.Options.EventDirName = "/x/y/z"
			}).
			PreRun(func(t *testing.T, c *Context) {
				c.ExpectedOutput.ErrIsNotNil = true
			}),
	)
}

func TestEventBus_AddWatcher(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type Input struct {
		EventName string
	}
	type Output struct {
		Err error
	}
	type State = EventBusDetails
	type Context struct {
		EB EventBus

		Init           Init
		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Init: Init{
				Options: EventBusOptions{
					EventDirName: ".",
				},
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.EB.Init(c.Init.Options)
		err := c.EB.Open()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}).Run(func(t *testing.T, c *Context) {
		_, err := c.EB.AddWatcher(c.Input.EventName)
		var output Output
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.EB.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	}).Teardown(func(t *testing.T, c *Context) {
		if c.ExpectedOutput.Err != ErrEventBusClosed {
			err := c.EB.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("event bus closed").
			Then("should fail with error ErrEventBusClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.EB.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.ExpectedOutput.Err = ErrEventBusClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			Then("should succeed (1)").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.EventName = "foo"
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"foo": {
						Size: 1,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (2)").
			PreRun(func(t *testing.T, c *Context) {
				_, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"foo": {
						Size: 2,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (3)").
			PreRun(func(t *testing.T, c *Context) {
				_, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "bar"
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"foo": {
						Size: 1,
					},
					"bar": {
						Size: 1,
					},
				}
			}),
	)
}

func TestEventBus_RemoveWatcher(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type Input struct {
		EventName string
		Watcher   Watcher
	}
	type Output struct {
		Err error
	}
	type State = EventBusDetails
	type Context struct {
		EB EventBus

		Init           Init
		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Init: Init{
				Options: EventBusOptions{
					EventDirName: ".",
				},
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.EB.Init(c.Init.Options)
		err := c.EB.Open()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}).Run(func(t *testing.T, c *Context) {
		err := c.EB.RemoveWatcher(c.Input.EventName, c.Input.Watcher)
		var output Output
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.EB.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	}).Teardown(func(t *testing.T, c *Context) {
		if c.ExpectedOutput.Err != ErrEventBusClosed {
			err := c.EB.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("event bus closed").
			Then("should fail with error ErrEventBusClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.EB.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.ExpectedOutput.Err = ErrEventBusClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			Then("should succeed (1)").
			PreRun(func(t *testing.T, c *Context) {
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
			}),
		tc.Copy().
			Then("should succeed (2)").
			PreRun(func(t *testing.T, c *Context) {
				_, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"foo": {
						Size: 1,
					},
				}
			}),
		tc.Copy().
			Then("should succeed (3)").
			PreRun(func(t *testing.T, c *Context) {
				_, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w, err := c.EB.AddWatcher("bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "bar"
				c.Input.Watcher = w
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"foo": {
						Size: 1,
					},
				}
			}),
		tc.Copy().
			When("given event name is incorrect").
			Then("should do nothing").
			PreRun(func(t *testing.T, c *Context) {
				w, err := c.EB.AddWatcher("bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "foo"
				c.Input.Watcher = w
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"bar": {
						Size: 1,
					},
				}
			}),
		tc.Copy().
			When("given watcher has already been removed").
			Then("should do nothing").
			PreRun(func(t *testing.T, c *Context) {
				_, err := c.EB.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w, err := c.EB.AddWatcher("bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				err = c.EB.RemoveWatcher("bar", w)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.EventName = "bar"
				c.Input.Watcher = w
				c.ExpectedState.WatcherSets = map[string]WatcherSetDetails{
					"foo": {
						Size: 1,
					},
				}
			}),
	)
}

func TestEventBus_handleEvents(t *testing.T) {
	type Init struct {
		Options EventBusOptions
	}
	type State = EventBusDetails
	type Context struct {
		EB EventBus

		Init          Init
		ExpectedState State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Init: Init{
				Options: EventBusOptions{
					EventDirName: ".",
				},
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.EB.Init(c.Init.Options)
		err := c.EB.Open()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}).Teardown(func(t *testing.T, c *Context) {
		err := c.EB.Close()
		assert.NoError(t, err)
	})
	testcase.RunListParallel(t,
		tc.Copy().
			When("event file was created").
			Then("fire event and remove watcher").
			Run(func(t *testing.T, c *Context) {
				w1, err := c.EB.AddWatcher("foo.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w2, err := c.EB.AddWatcher("foo.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				time.AfterFunc(100*time.Millisecond, func() {
					f, err := os.Create("foo.tmp")
					if !assert.NoError(t, err) {
						return
					}
					f.Close()
				})
				defer os.Remove("foo.tmp")
				select {
				case <-w1.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				select {
				case <-w2.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				state := c.EB.Inspect()
				assert.Equal(t, State{}, state)
			}),
		tc.Copy().
			When("event file was updated").
			Then("fire event").
			Run(func(t *testing.T, c *Context) {
				w0, err := c.EB.AddWatcher("bar.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				f, err := os.Create("bar.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				select {
				case <-w0.Event():
				case <-time.After(10 * time.Second):
					t.FailNow()
				}
				w1, err := c.EB.AddWatcher("bar.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w2, err := c.EB.AddWatcher("bar.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				time.AfterFunc(100*time.Millisecond, func() {
					_, err := f.WriteString("hello world")
					if !assert.NoError(t, err) {
						return
					}
					f.Close()
				})
				defer os.Remove("bar.tmp")
				select {
				case <-w1.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				select {
				case <-w2.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				state := c.EB.Inspect()
				assert.Equal(t, State{}, state)
			}),
		tc.Copy().
			When("event file was overwrote").
			Then("fire event").
			Run(func(t *testing.T, c *Context) {
				w0, err := c.EB.AddWatcher("baz.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				f, err := os.Create("baz.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				f.Close()
				select {
				case <-w0.Event():
				case <-time.After(10 * time.Second):
					t.FailNow()
				}
				w1, err := c.EB.AddWatcher("baz.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w2, err := c.EB.AddWatcher("baz.tmp")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				time.AfterFunc(100*time.Millisecond, func() {
					f, err := os.Create("baz.tmp.2")
					if !assert.NoError(t, err) {
						return
					}
					_, err = f.WriteString("hello world")
					if !assert.NoError(t, err) {
						return
					}
					f.Close()
					err = os.Rename("baz.tmp.2", "baz.tmp")
					if !assert.NoError(t, err) {
						return
					}
				})
				defer os.Remove("baz.tmp")
				select {
				case <-w1.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				select {
				case <-w2.Event():
				case <-time.After(10 * time.Second):
					t.Fatal("timed out")
				}
				state := c.EB.Inspect()
				assert.Equal(t, State{}, state)
			}),
	)
}

func TestEventBus_Close(t *testing.T) {
	var wg sync.WaitGroup
	eb := new(EventBus).Init(EventBusOptions{
		Go: func(routine func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				routine()
			}()
		},
	})
	err := eb.Open()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	err = eb.Close()
	assert.NoError(t, err)
	wg.Wait()
	err = eb.Close()
	assert.Equal(t, ErrEventBusClosed, err)
}
