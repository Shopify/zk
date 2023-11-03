package zk

import (
	"sync"
	"time"
)

// TreeCacheListenerMock is a mock implementation of TreeCacheListener.
type TreeCacheListenerMock struct {
	mu sync.Mutex

	onSyncStartedCalled int
	onSyncStoppedCalled int
	onSyncErrorCalled   int
	onTreeSyncedCalled  int

	onNodeCreatedCalled int
	nodesCreated        map[string][]byte

	onNodeDataChangedCalled int
	nodesDataChanged        map[string][]byte

	onNodeDeletingCalled int
	nodesDeleting        map[string][]byte

	onNodeDeletedCalled int
	nodesDeleted        map[string]string
}

func NewTreeCacheListenerMock() *TreeCacheListenerMock {
	return &TreeCacheListenerMock{
		mu:               sync.Mutex{},
		nodesCreated:     make(map[string][]byte),
		nodesDeleting:    make(map[string][]byte),
		nodesDeleted:     make(map[string]string),
		nodesDataChanged: make(map[string][]byte),
	}
}

func (m *TreeCacheListenerMock) OnSyncStarted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onSyncStartedCalled++
}

func (m *TreeCacheListenerMock) OnSyncStopped(_ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onSyncStoppedCalled++
}

func (m *TreeCacheListenerMock) OnSyncError(_ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onSyncErrorCalled++
}

func (m *TreeCacheListenerMock) OnTreeSynced(_ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onTreeSyncedCalled++
}

func (m *TreeCacheListenerMock) OnNodeCreated(path string, data []byte, _ *Stat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNodeCreatedCalled++
	m.nodesCreated[path] = data
}

func (m *TreeCacheListenerMock) OnNodeDeleting(path string, data []byte, _ *Stat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNodeDeletingCalled++
	m.nodesDeleting[path] = data
}

func (m *TreeCacheListenerMock) OnNodeDeleted(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNodeDeletedCalled++
	m.nodesDeleted[path] = path
}

func (m *TreeCacheListenerMock) OnNodeDataChanged(path string, data []byte, _ *Stat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNodeDataChangedCalled++
	m.nodesDataChanged[path] = data
}

func (m *TreeCacheListenerMock) OnSyncStartedCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onSyncStartedCalled
}

func (m *TreeCacheListenerMock) OnSyncStoppedCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onSyncStoppedCalled
}

func (m *TreeCacheListenerMock) OnSyncErrorCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onSyncErrorCalled
}

func (m *TreeCacheListenerMock) OnTreeSyncedCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onTreeSyncedCalled
}

func (m *TreeCacheListenerMock) OnNodeCreatedCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onNodeCreatedCalled
}

func (m *TreeCacheListenerMock) NodesCreated() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodesCreated
}

func (m *TreeCacheListenerMock) OnNodeDataChangedCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onNodeDataChangedCalled
}

func (m *TreeCacheListenerMock) NodesDataChanged() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodesDataChanged
}

func (m *TreeCacheListenerMock) OnNodeDeletingCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onNodeDeletingCalled
}

func (m *TreeCacheListenerMock) NodesDeleting() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodesDeleting
}

func (m *TreeCacheListenerMock) OnNodeDeletedCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onNodeDeletedCalled
}

func (m *TreeCacheListenerMock) NodesDeleted() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodesDeleted
}
