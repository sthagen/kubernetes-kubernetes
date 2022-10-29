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

package etcd3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
)

func TestWatch(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestWatch(ctx, t, store)
}

func TestDeleteTriggerWatch(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestDeleteTriggerWatch(ctx, t, store)
}

func TestWatchFromZero(t *testing.T) {
	ctx, store, client := testSetup(t)
	storagetesting.RunTestWatchFromZero(ctx, t, store, compactStorage(client))
}

// TestWatchFromNoneZero tests that
// - watch from non-0 should just watch changes after given version
func TestWatchFromNoneZero(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestWatchFromNoneZero(ctx, t, store)
}

func TestWatchError(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestWatchError(ctx, t, &storeWithPrefixTransformer{store})
}

func TestWatchContextCancel(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestWatchContextCancel(ctx, t, store)
}

func TestWatchErrResultNotBlockAfterCancel(t *testing.T) {
	origCtx, store, _ := testSetup(t)
	ctx, cancel := context.WithCancel(origCtx)
	w := store.watcher.createWatchChan(ctx, "/abc", 0, false, false, newTestTransformer(), storage.Everything)
	// make resutlChan and errChan blocking to ensure ordering.
	w.resultChan = make(chan watch.Event)
	w.errChan = make(chan error)
	// The event flow goes like:
	// - first we send an error, it should block on resultChan.
	// - Then we cancel ctx. The blocking on resultChan should be freed up
	//   and run() goroutine should return.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.run()
		wg.Done()
	}()
	w.errChan <- fmt.Errorf("some error")
	cancel()
	wg.Wait()
}

func TestWatchDeleteEventObjectHaveLatestRV(t *testing.T) {
	ctx, store, client := testSetup(t)
	key, storedObj := storagetesting.TestPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}})

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(storedObj)
	if err != nil {
		t.Fatalf("failed to parse resourceVersion on stored object: %v", err)
	}
	etcdW := client.Watch(ctx, key, clientv3.WithRev(int64(rv)))

	if err := store.Delete(ctx, key, &example.Pod{}, &storage.Preconditions{}, storage.ValidateAllObjectFunc, nil); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	var e watch.Event
	watchCtx, _ := context.WithTimeout(ctx, wait.ForeverTestTimeout)
	select {
	case e = <-w.ResultChan():
	case <-watchCtx.Done():
		t.Fatalf("timed out waiting for watch event")
	}
	deletedRV, err := deletedRevision(watchCtx, etcdW)
	if err != nil {
		t.Fatalf("did not see delete event in raw watch: %v", err)
	}
	watchedDeleteObj := e.Object.(*example.Pod)

	watchedDeleteRev, err := store.versioner.ParseResourceVersion(watchedDeleteObj.ResourceVersion)
	if err != nil {
		t.Fatalf("ParseWatchResourceVersion failed: %v", err)
	}
	if int64(watchedDeleteRev) != deletedRV {
		t.Errorf("Object from delete event have version: %v, should be the same as etcd delete's mod rev: %d",
			watchedDeleteRev, deletedRV)
	}
}

func deletedRevision(ctx context.Context, watch <-chan clientv3.WatchResponse) (int64, error) {
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case wres := <-watch:
			for _, evt := range wres.Events {
				if evt.Type == mvccpb.DELETE && evt.Kv != nil {
					return evt.Kv.ModRevision, nil
				}
			}
		}
	}
}

func TestWatchInitializationSignal(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestWatchInitializationSignal(ctx, t, store)
}

func TestProgressNotify(t *testing.T) {
	clusterConfig := testserver.NewTestConfig(t)
	clusterConfig.ExperimentalWatchProgressNotifyInterval = time.Second
	ctx, store, _ := testSetup(t, withClientConfig(clusterConfig))

	key := "/somekey"
	input := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name"}}
	out := &example.Pod{}
	if err := store.Create(ctx, key, input, out, 0); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	validateResourceVersion := storagetesting.ResourceVersionNotOlderThan(out.ResourceVersion)

	opts := storage.ListOptions{
		ResourceVersion: out.ResourceVersion,
		Predicate:       storage.Everything,
		ProgressNotify:  true,
	}
	w, err := store.Watch(ctx, key, opts)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// when we send a bookmark event, the client expects the event to contain an
	// object of the correct type, but with no fields set other than the resourceVersion
	storagetesting.TestCheckResultFunc(t, watch.Bookmark, w, func(object runtime.Object) error {
		// first, check that we have the correct resource version
		obj, ok := object.(metav1.Object)
		if !ok {
			return fmt.Errorf("got %T, not metav1.Object", object)
		}
		if err := validateResourceVersion(obj.GetResourceVersion()); err != nil {
			return err
		}

		// then, check that we have the right type and content
		pod, ok := object.(*example.Pod)
		if !ok {
			return fmt.Errorf("got %T, not *example.Pod", object)
		}
		pod.ResourceVersion = ""
		storagetesting.ExpectNoDiff(t, "bookmark event should contain an object with no fields set other than resourceVersion", newPod(), pod)
		return nil
	})
}
