package internal

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

type EventBus struct {
	options      EventBusOptions
	superWatcher *fsnotify.Watcher
	watcherSets  sync.Map
	isClosed     int32
}

type EventBusOptions struct {
	EventDirName string
	Go           func(func())
}

func (ebo *EventBusOptions) sanitize() {
	if ebo.EventDirName == "" {
		ebo.EventDirName = "."
	}
	if ebo.Go == nil {
		ebo.Go = func(routine func()) { go routine() }
	}
}

func (eb *EventBus) Init(options EventBusOptions) *EventBus {
	eb.options = options
	eb.options.sanitize()
	return eb
}

func (eb *EventBus) Open() error {
	superWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() {
		if superWatcher != nil {
			superWatcher.Close()
		}
	}()
	if err := superWatcher.Add(eb.options.EventDirName); err != nil {
		return err
	}
	eb.superWatcher = superWatcher
	superWatcher = nil
	eb.options.Go(eb.handleEvents)
	return nil
}

func (eb *EventBus) handleEvents() {
	for {
		if eb.IsClosed() {
			return
		}
		select {
		case event, ok := <-eb.superWatcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}
			eb.fireEvent(event.Name)
		case _, ok := <-eb.superWatcher.Errors:
			if !ok {
				return
			}
		}
	}
}

func (eb *EventBus) fireEvent(eventFileName string) {
	eventName := filepath.Base(eventFileName)
	opaqueWatcherSet, ok := eb.watcherSets.Load(eventName)
	if !ok {
		return
	}
	watcherSet := opaqueWatcherSet.(*watcherSet)
	watcherSet.FireEvent(func() { eb.watcherSets.Delete(eventName) })
}

func (eb *EventBus) AddWatcher(eventName string) (Watcher, error) {
	for {
		if eb.IsClosed() {
			return Watcher{}, ErrEventBusClosed
		}
		opaqueWatcherSet, ok := eb.watcherSets.Load(eventName)
		if !ok {
			opaqueWatcherSet, _ = eb.watcherSets.LoadOrStore(eventName, new(watcherSet).Init())
		}
		watcherSet := opaqueWatcherSet.(*watcherSet)
		watcher := new(watcher).Init()
		if !watcherSet.AddItem(watcher) {
			continue
		}
		wrappedWatcher := Watcher{watcher}
		return wrappedWatcher, nil
	}
}

func (eb *EventBus) RemoveWatcher(eventName string, wrappedWatcher Watcher) error {
	if eb.IsClosed() {
		return ErrEventBusClosed
	}
	opaqueWatcherSet, ok := eb.watcherSets.Load(eventName)
	if !ok {
		return nil
	}
	watcherSet := opaqueWatcherSet.(*watcherSet)
	watcher := wrappedWatcher.w
	watcherSet.RemoveItem(watcher, func() { eb.watcherSets.Delete(eventName) })
	return nil
}

func (eb *EventBus) Close() error {
	if atomic.SwapInt32(&eb.isClosed, 1) != 0 {
		return ErrEventBusClosed
	}
	return eb.superWatcher.Close()
}

func (eb *EventBus) IsClosed() bool {
	return atomic.LoadInt32(&eb.isClosed) != 0
}

type Watcher struct{ w *watcher }

func (w Watcher) Event() <-chan struct{} { return w.w.Event() }

type EventArgs struct {
	WatchLoss bool
	Message   string
}

var ErrEventBusClosed error = errors.New("internal: event bus closed")

type watcherSet struct {
	mu        sync.Mutex
	items     map[*watcher]struct{}
	isRemoved bool
}

func (ws *watcherSet) Init() *watcherSet {
	ws.items = make(map[*watcher]struct{})
	return ws
}

func (ws *watcherSet) AddItem(item *watcher) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.isRemoved {
		return false
	}
	ws.items[item] = struct{}{}
	return true
}

func (ws *watcherSet) RemoveItem(item *watcher, remover watcherSetRemover) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.isRemoved {
		return
	}
	if _, ok := ws.items[item]; !ok {
		return
	}
	delete(ws.items, item)
	if n := len(ws.items); n >= 1 {
		return
	}
	ws.remove(remover)
}

func (ws *watcherSet) FireEvent(remover watcherSetRemover) {
	mu := &ws.mu
	mu.Lock()
	defer func() {
		if mu != nil {
			mu.Unlock()
		}
	}()
	if ws.isRemoved {
		return
	}
	ws.remove(remover)
	mu.Unlock()
	mu = nil
	for item := range ws.items {
		item.FireEvent()
	}
}

func (ws *watcherSet) remove(remover watcherSetRemover) {
	remover()
	ws.isRemoved = true
}

type watcherSetRemover func()

type watcher struct {
	event chan struct{}
}

func (w *watcher) Init() *watcher {
	w.event = make(chan struct{})
	return w
}

func (w *watcher) FireEvent() {
	close(w.event)
}

func (w *watcher) Event() <-chan struct{} {
	return w.event
}
