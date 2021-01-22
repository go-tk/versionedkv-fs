package internal

type EventBusDetails struct {
	WatcherSets map[string]WatcherSetDetails
	IsClosed    bool
}

type WatcherSetDetails struct {
	Size int
}

func (eb *EventBus) Inspect() EventBusDetails {
	if eb.IsClosed() {
		return EventBusDetails{IsClosed: true}
	}
	var watcherSetDetails map[string]WatcherSetDetails
	eb.watcherSets.Range(func(opaqueEventName, opaqueWatcherSet interface{}) bool {
		eventName := opaqueEventName.(string)
		watcherSet := opaqueWatcherSet.(*watcherSet)
		if watcherSetDetails == nil {
			watcherSetDetails = make(map[string]WatcherSetDetails)
		}
		watcherSetSize := watcherSet.Size()
		watcherSetDetails[eventName] = WatcherSetDetails{
			Size: watcherSetSize,
		}
		return true
	})
	return EventBusDetails{
		WatcherSets: watcherSetDetails,
	}
}

func (ws *watcherSet) Size() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.items)
}
