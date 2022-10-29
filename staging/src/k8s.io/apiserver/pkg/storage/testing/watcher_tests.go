/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testing

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
)

func RunTestWatch(ctx context.Context, t *testing.T, store storage.Interface) {
	testWatch(ctx, t, store, false)
	testWatch(ctx, t, store, true)
}

// It tests that
// - first occurrence of objects should notify Add event
// - update should trigger Modified event
// - update that gets filtered should trigger Deleted event
func testWatch(ctx context.Context, t *testing.T, store storage.Interface, recursive bool) {
	podFoo := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	podBar := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bar"}}

	tests := []struct {
		name       string
		key        string
		pred       storage.SelectionPredicate
		watchTests []*testWatchStruct
	}{{
		name:       "create a key",
		key:        "/somekey-1",
		watchTests: []*testWatchStruct{{podFoo, true, watch.Added}},
		pred:       storage.Everything,
	}, {
		name:       "key updated to match predicate",
		key:        "/somekey-3",
		watchTests: []*testWatchStruct{{podFoo, false, ""}, {podBar, true, watch.Added}},
		pred: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("metadata.name=bar"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		},
	}, {
		name:       "update",
		key:        "/somekey-4",
		watchTests: []*testWatchStruct{{podFoo, true, watch.Added}, {podBar, true, watch.Modified}},
		pred:       storage.Everything,
	}, {
		name:       "delete because of being filtered",
		key:        "/somekey-5",
		watchTests: []*testWatchStruct{{podFoo, true, watch.Added}, {podBar, true, watch.Deleted}},
		pred: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("metadata.name!=bar"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := store.Watch(ctx, tt.key, storage.ListOptions{ResourceVersion: "0", Predicate: tt.pred, Recursive: recursive})
			if err != nil {
				t.Fatalf("Watch failed: %v", err)
			}
			var prevObj *example.Pod
			for _, watchTest := range tt.watchTests {
				out := &example.Pod{}
				key := tt.key
				if recursive {
					key = key + "/item"
				}
				err := store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
					func(runtime.Object) (runtime.Object, error) {
						return watchTest.obj, nil
					}), nil)
				if err != nil {
					t.Fatalf("GuaranteedUpdate failed: %v", err)
				}
				if watchTest.expectEvent {
					expectObj := out
					if watchTest.watchType == watch.Deleted {
						expectObj = prevObj
						expectObj.ResourceVersion = out.ResourceVersion
					}
					TestCheckResult(t, watchTest.watchType, w, expectObj)
				}
				prevObj = out
			}
			w.Stop()
			TestCheckStop(t, w)
		})
	}
}

// RunTestWatchFromZero tests that
// - watch from 0 should sync up and grab the object added before
// - watch from 0 is able to return events for objects whose previous version has been compacted
func RunTestWatchFromZero(ctx context.Context, t *testing.T, store storage.Interface, compaction Compaction) {
	key, storedObj := TestPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"}})

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	TestCheckResult(t, watch.Added, w, storedObj)
	w.Stop()

	// Update
	out := &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns", Annotations: map[string]string{"a": "1"}}}, nil
		}), nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate failed: %v", err)
	}

	// Make sure when we watch from 0 we receive an ADDED event
	w, err = store.Watch(ctx, key, storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	TestCheckResult(t, watch.Added, w, out)
	w.Stop()

	if compaction == nil {
		t.Skip("compaction callback not provided")
	}

	// Update again
	out = &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"}}, nil
		}), nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate failed: %v", err)
	}

	// Compact previous versions
	compaction(ctx, t, out.ResourceVersion)

	// Make sure we can still watch from 0 and receive an ADDED event
	w, err = store.Watch(ctx, key, storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	TestCheckResult(t, watch.Added, w, out)
}

func RunTestDeleteTriggerWatch(ctx context.Context, t *testing.T, store storage.Interface) {
	key, storedObj := TestPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}})
	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	TestCheckEventType(t, watch.Deleted, w)
}

func RunTestWatchFromNoneZero(ctx context.Context, t *testing.T, store storage.Interface) {
	key, storedObj := TestPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}})

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	out := &example.Pod{}
	store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bar"}}, err
		}), nil)
	TestCheckResult(t, watch.Modified, w, out)
}

func RunTestWatchError(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer) {
	// Compute the initial resource version from which we can start watching later.
	list := &example.PodList{}
	storageOpts := storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/", storageOpts, list); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if err := store.GuaranteedUpdate(ctx, "//foo", &example.Pod{}, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}, nil
		}), nil); err != nil {
		t.Fatalf("GuaranteedUpdate failed: %v", err)
	}

	// Now trigger watch error by injecting failing transformer.
	revertTransformer := store.UpdatePrefixTransformer(
		func(previousTransformer *PrefixTransformer) value.Transformer {
			return &failingTransformer{}
		})
	defer revertTransformer()

	w, err := store.Watch(ctx, "//foo", storage.ListOptions{ResourceVersion: list.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	TestCheckEventType(t, watch.Error, w)
}

func RunTestWatchContextCancel(ctx context.Context, t *testing.T, store storage.Interface) {
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	// When we watch with a canceled context, we should detect that it's context canceled.
	// We won't take it as error and also close the watcher.
	w, err := store.Watch(canceledCtx, "/abc", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Error("ResultChan() should be closed")
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("timeout after %v", wait.ForeverTestTimeout)
	}
}

func RunTestWatchInitializationSignal(ctx context.Context, t *testing.T, store storage.Interface) {
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)
	initSignal := utilflowcontrol.NewInitializationSignal()
	ctx = utilflowcontrol.WithInitializationSignal(ctx, initSignal)

	key, storedObj := TestPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}})
	_, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	initSignal.Wait()
}

type testWatchStruct struct {
	obj         *example.Pod
	expectEvent bool
	watchType   watch.EventType
}
