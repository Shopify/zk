package zk

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestTreeWalker(t *testing.T) {
	testCases := []struct {
		name          string
		setupWalker   func(w TreeWalker) TreeWalker
		expected      []string
		ignoreOrder   bool // For parallel walks which are never consistent.
		cancelContext bool // Cancels context before walk starts.
		wantError     string
	}{
		{
			name: "DepthFirst_IncludeRoot",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().IncludeRoot(true)
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a",
				"/gozk-test-walker",
			},
		},
		{
			name: "DepthFirst_IncludeRoot_CancelContext",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().IncludeRoot(true)
			},
			cancelContext: true,
			wantError:     "context canceled",
		},
		{
			name: "DepthFirst_IncludeRoot_Concurrency2",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().Concurrency(2).IncludeRoot(true)
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a",
				"/gozk-test-walker",
			},
			ignoreOrder: true, // Parallel traversal causes non-deterministic ordering.
		},
		{
			name: "BreadthFirst_IncludeRoot",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().IncludeRoot(true)
			},
			expected: []string{
				"/gozk-test-walker",
				"/gozk-test-walker/a",
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a/c/d",
			},
		},
		{
			name: "BreadthFirst_IncludeRoot_CancelContext",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().IncludeRoot(true)
			},
			cancelContext: true,
			wantError:     "context canceled",
		},
		{
			name: "BreadthFirst_IncludeRoot_Concurrency2",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().IncludeRoot(true).Concurrency(2)
			},
			expected: []string{
				"/gozk-test-walker",
				"/gozk-test-walker/a",
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a/c/d",
			},
			ignoreOrder: true, // Parallel traversal causes non-deterministic ordering.
		},
		{
			name: "DepthFirst_ExcludeRoot",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().IncludeRoot(false)
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a",
			},
		},
		{
			name: "DepthFirst_ExcludeRoot_Concurrency2",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().Concurrency(2).IncludeRoot(false)
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a",
			},
			ignoreOrder: true, // Parallel traversal causes non-deterministic ordering.
		},
		{
			name: "BreadthFirst_ExcludeRoot",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().IncludeRoot(false)
			},
			expected: []string{
				"/gozk-test-walker/a",
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a/c/d",
			},
		},
		{
			name: "BreadthFirst_ExcludeRoot_Concurrency2",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().Concurrency(2).IncludeRoot(false)
			},
			expected: []string{
				"/gozk-test-walker/a",
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a/c/d",
			},
			ignoreOrder: true, // Parallel traversal causes non-deterministic ordering.
		},
		{
			name: "DepthFirst_LeavesOnly",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().LeavesOnly()
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
			},
		},
		{
			name: "DepthFirst_LeavesOnly_Concurrency2",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.DepthFirst().Concurrency(2).LeavesOnly()
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
			},
			ignoreOrder: true, // Parallel traversal causes non-deterministic ordering.
		},
		{
			name: "BreadthFirst_LeavesOnly",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().LeavesOnly()
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
			},
		},
		{
			name: "BreadthFirst_LeavesOnly_Concurrency2",
			setupWalker: func(w TreeWalker) TreeWalker {
				return w.BreadthFirst().Concurrency(2).LeavesOnly()
			},
			expected: []string{
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c/d",
			},
			ignoreOrder: true, // Parallel traversal causes non-deterministic ordering.
		},
	}

	expectVisitedExact := func(t *testing.T, expected []string, visited []string) {
		if !reflect.DeepEqual(expected, visited) {
			t.Fatalf("%s saw unexpected paths:\n Expected: %+v\n Got:      %+v",
				t.Name(), expected, visited)
		}
	}

	expectVisitedUnordered := func(t *testing.T, expected []string, visited []string) {
		expectedSet := make(map[string]struct{})
		visitedSet := make(map[string]struct{})
		for _, p := range expected {
			expectedSet[p] = struct{}{}
		}
		for _, p := range visited {
			visitedSet[p] = struct{}{}
		}
		if !reflect.DeepEqual(expectedSet, visitedSet) {
			t.Fatalf("%s saw unexpected paths:\n Expected: %+v\n Got:      %+v",
				t.Name(), expected, visited)
		}
	}

	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			paths := []string{
				"/gozk-test-walker",
				"/gozk-test-walker/a",
				"/gozk-test-walker/a/b",
				"/gozk-test-walker/a/c",
				"/gozk-test-walker/a/c/d",
			}
			for _, p := range paths {
				if path, err := c.Create(p, []byte{1, 2, 3, 4}, 0, WorldACL(PermAll)); err != nil {
					t.Fatalf("Create returned error: %+v", err)
				} else if path != p {
					t.Fatalf("Create returned different path '%s' != '%s'", path, p)
				}
			}

			for _, testCase := range testCases {
				// Test WalkCtx with visitor.
				t.Run(testCase.name+"_WalkCtx", func(t *testing.T) {
					var visited []string
					l := sync.Mutex{} // Protects visited from concurrent access.
					w := InitTreeWalker(c.ChildrenCtx, "/gozk-test-walker")

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if testCase.cancelContext {
						cancel() // Immediately cancel the context to abort the walk.
					}

					err := testCase.setupWalker(w).WalkCtx(ctx, func(_ context.Context, path string, _ *Stat) error {
						l.Lock()
						defer l.Unlock()
						visited = append(visited, path)
						return nil
					})
					if err != nil {
						if testCase.wantError != "" {
							if !strings.Contains(err.Error(), testCase.wantError) {
								t.Fatalf("%s WalkCtx returned unexpected error: %+v; wanted error: %s", testCase.name, err, testCase.wantError)
							}
						} else {
							t.Fatalf("%s WalkCtx returned an unexpected error: %+v; wanted no error", testCase.name, err)
						}
					} else if testCase.wantError != "" {
						t.Fatalf("%s WalkCtx returned no error; wanted error: %s", testCase.name, testCase.wantError)
					}

					if len(testCase.expected) > 0 {
						l.Lock()
						defer l.Unlock()
						if testCase.ignoreOrder {
							expectVisitedUnordered(t, testCase.expected, visited)
						} else {
							expectVisitedExact(t, testCase.expected, visited)
						}
					}
				})

				// Test WalkChanCtx.
				t.Run(testCase.name+"_WalkChanCtx", func(t *testing.T) {
					var visited []string
					w := InitTreeWalker(c.ChildrenCtx, "/gozk-test-walker")

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if testCase.cancelContext {
						cancel() // Immediately cancel the context to abort the walk.
					}

					ch := testCase.setupWalker(w).WalkChanCtx(ctx, 1)
					for e := range ch {
						if e.Err != nil {
							if testCase.wantError != "" {
								if !strings.Contains(e.Err.Error(), testCase.wantError) {
									t.Fatalf("%s WalkChanCtx returned unexpected error: %+v; wanted error: %s", testCase.name, e.Err, testCase.wantError)
								}
							} else {
								t.Fatalf("%s WalkChanCtx returned an unexpected error: %+v; wanted no error", testCase.name, e.Err)
							}
						} else if testCase.wantError != "" {
							t.Fatalf("%s WalkChanCtx returned no error; wanted error: %s", testCase.name, testCase.wantError)
						}

						visited = append(visited, e.Path)
					}

					if len(testCase.expected) > 0 {
						if testCase.ignoreOrder {
							expectVisitedUnordered(t, testCase.expected, visited)
						} else {
							expectVisitedExact(t, testCase.expected, visited)
						}
					}
				})
			}
		})
	})
}
