/*
Copyright 2023 The Kubernetes Authors.

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

package ktesting_test

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestCancelManual(t *testing.T) {
	tCtx := ktesting.Init(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Blocks until tCtx.Cancel is called below.
		<-tCtx.Done()
	}()
	tCtx.Cancel("manually canceled")
	wg.Wait()
}

func TestCancelAutomatic(t *testing.T) {
	var wg sync.WaitGroup
	// This callback gets registered first and thus
	// gets invoked last.
	t.Cleanup(wg.Wait)
	tCtx := ktesting.Init(t)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Blocks until the context gets canceled automatically.
		<-tCtx.Done()
	}()
}

func TestSyncTestInit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// This must work inside a synctest bubble, despite Deadline panicking there.
		// We then don't have a deadline.
		tCtx := ktesting.Init(t)
		deadline, ok := tCtx.Deadline()
		if ok {
			tCtx.Errorf("Expected no deadline, got %s", deadline)
		}
		if !tCtx.IsSyncTest() {
			tCtx.Errorf("Expected to run as synctest")
		}
	})
}

func TestNormalInit(t *testing.T) {
	// The outcome depends on how the unit test was started.
	// See below for deterministic deadline/no deadline testing.
	expectDeadline, expectOK := t.Deadline()
	expectDeadline = expectDeadline.Add(-ktesting.CleanupGracePeriod)
	tCtx := ktesting.Init(t)
	actualDeadline, actualOK := tCtx.Deadline()
	tCtx.Expect(actualOK).To(gomega.Equal(expectOK), "have deadline")
	if expectOK {
		tCtx.Expect(actualDeadline).To(gomega.BeTemporally("~", expectDeadline, 2*time.Second), "deadline")
	}
	if tCtx.IsSyncTest() {
		tCtx.Errorf("Expected to not run as synctest")
	}
}

func TestNoDeadline(t *testing.T) {
	mockT := &deadlineT{T: t, deadline: nil}
	tCtx := ktesting.Init(mockT)
	deadline, ok := tCtx.Deadline()
	if ok {
		tCtx.Errorf("Expected no deadline, got %s", deadline)
	}
}

func TestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Inside a synctest bubble this is always in the future.
		mockDeadline := time.Date(2000, 01, 01, 0, 0, 0, 0, time.UTC)
		mockT := &deadlineT{T: t, deadline: &mockDeadline}
		tCtx := ktesting.Init(mockT)
		actualDeadline, ok := tCtx.Deadline()
		if ok {
			expectDeadline := mockDeadline.Add(-ktesting.CleanupGracePeriod)
			tCtx.Expect(actualDeadline).To(gomega.BeTemporally("==", expectDeadline), "deadline")
		} else {
			tCtx.Error("Expected a deadline, got none")
		}
	})
}

// deadlineT overrides Deadline, returning false if no
// deadline is configured and the deadline otherwise.
type deadlineT struct {
	*testing.T
	deadline *time.Time
}

func (t *deadlineT) Deadline() (time.Time, bool) {
	if t.deadline == nil {
		return time.Time{}, false
	}
	return *t.deadline, true
}

func TestCancelCtx(t *testing.T) {
	tCtx := ktesting.Init(t)
	var discardLogger klog.Logger
	tCtx = tCtx.WithLogger(discardLogger)
	tCtx = tCtx.WithRESTConfig(new(rest.Config))
	baseCtx := tCtx

	tCtx.Cleanup(func() {
		if tCtx.Err() == nil {
			t.Error("context should be canceled but isn't")
		}
	})
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		if tCtx.Err() != nil {
			t.Errorf("context should not be canceled but is: %v", tCtx.Err())
		}
		assert.Equal(t, baseCtx.Logger(), tCtx.Logger(), "Logger()")
		assert.Equal(t, baseCtx.RESTConfig(), tCtx.RESTConfig(), "RESTConfig()")
		assert.Equal(t, baseCtx.RESTMapper(), tCtx.RESTMapper(), "RESTMapper()")
		assert.Equal(t, baseCtx.Client(), tCtx.Client(), "Client()")
		assert.Equal(t, baseCtx.Dynamic(), tCtx.Dynamic(), "Dynamic()")
		assert.Equal(t, baseCtx.APIExtensions(), tCtx.APIExtensions(), "APIExtensions()")
	})

	// Cancel, then let testing.T invoke test cleanup.
	tCtx.Cancel("test is complete")
}

func TestParallel(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(3)

	tCtx := ktesting.Init(t)

	// Each sub-test runs in parallel to the others and waits for the other two.
	test := func(tCtx ktesting.TContext) {
		tCtx.Parallel()
		wg.Done()
		wg.Wait()
	}
	tCtx.Run("one", test)
	tCtx.Run("two", test)
	tCtx.Run("three", test)
}

func TestRun(t *testing.T) {
	tCtx := ktesting.Init(t)

	cfg := new(rest.Config)
	mapper := new(restmapper.DeferredDiscoveryRESTMapper)
	client := clientset.New(nil)
	dynamic := dynamic.New(nil)
	apiextensions := apiextensions.New(nil)
	tCtx = tCtx.WithClients(cfg, mapper, client, dynamic, apiextensions)

	tCtx.Run("sub", func(tCtx ktesting.TContext) {
		assert.Equal(t, cfg, tCtx.RESTConfig(), "RESTConfig")
		assert.Equal(t, mapper, tCtx.RESTMapper(), "RESTMapper")
		assert.Equal(t, client, tCtx.Client(), "Client")
		assert.Equal(t, dynamic, tCtx.Dynamic(), "Dynamic")
		assert.Equal(t, apiextensions, tCtx.APIExtensions(), "APIExtensions")

		tCtx.Cancel("test is complete")
		<-tCtx.Done()
	})

	if err := tCtx.Err(); err != nil {
		t.Errorf("parent TContext should not have been cancelled: %v", err)
	}
}

func TestWithNamespace(t *testing.T) {
	tCtx := ktesting.Init(t)
	namespace := "foo"
	tCtxWithNamespace := tCtx.WithNamespace(namespace)
	tCtx.Expect(tCtxWithNamespace.Namespace()).To(gomega.Equal(namespace))
}
