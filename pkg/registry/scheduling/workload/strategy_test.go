/*
Copyright 2025 The Kubernetes Authors.

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

package workload

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
)

var workload = &scheduling.Workload{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "foo",
		Namespace: metav1.NamespaceDefault,
	},
	Spec: scheduling.WorkloadSpec{
		PodGroupTemplates: []scheduling.PodGroupTemplate{
			{
				Name: "bar",
				SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
					Gang: &scheduling.GangSchedulingPolicy{
						MinCount: 5,
					},
				},
			},
		},
	},
}

func TestWorkloadStrategy(t *testing.T) {
	if !Strategy.NamespaceScoped() {
		t.Errorf("Workload must be namespace scoped")
	}
	if Strategy.AllowCreateOnUpdate() {
		t.Errorf("Workload should not allow create on update")
	}
}

func ctxWithRequestInfo() context.Context {
	return genericapirequest.WithRequestInfo(genericapirequest.NewDefaultContext(), &genericapirequest.RequestInfo{
		APIGroup:          "scheduling.k8s.io",
		APIVersion:        "v1alpha2",
		Resource:          "workloads",
		IsResourceRequest: true,
	})
}

func TestPodSchedulingStrategyCreate(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		ctx := ctxWithRequestInfo()
		workload := workload.DeepCopy()

		Strategy.PrepareForCreate(ctx, workload)
		errs := Strategy.Validate(ctx, workload)
		if len(errs) != 0 {
			t.Errorf("Unexpected validation error: %v", errs)
		}
	})

	t.Run("failed validation", func(t *testing.T) {
		ctx := ctxWithRequestInfo()
		workload := workload.DeepCopy()
		workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount = -1

		Strategy.PrepareForCreate(ctx, workload)
		errs := Strategy.Validate(ctx, workload)
		if len(errs) == 0 {
			t.Errorf("Expected validation error")
		}
	})
}

func TestPodSchedulingStrategyCreate_SchedulingConstraints(t *testing.T) {
	testCases := map[string]struct {
		obj                   *scheduling.Workload
		expectObj             *scheduling.Workload
		expectValidationError string
		tasEnabled            bool
	}{
		"drops field with SchedulingConstraints set and TAS disabled": {
			obj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectObj:  workload,
			tasEnabled: false,
		},
		"valid with SchedulingConstraints set and TAS enabled": {
			obj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			tasEnabled: true,
		},
		"invalid with multiple topology constraints": {
			obj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{
						{Key: "foo"},
						{Key: "bar"},
					},
				}
				return workload
			}(),
			expectValidationError: "must have at most 1 item",
			tasEnabled:            true,
		},
		"invalid with invalid topology key": {
			obj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{
						{Key: ""},
					},
				}
				return workload
			}(),
			expectValidationError: "Required value",
			tasEnabled:            true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 tc.tasEnabled,
				features.TopologyAwareWorkloadScheduling: tc.tasEnabled,
			})
			ctx := ctxWithRequestInfo()
			workload := tc.obj.DeepCopy()

			Strategy.PrepareForCreate(ctx, workload)
			if errs := Strategy.Validate(ctx, workload); len(errs) != 0 {
				if tc.expectValidationError == "" {
					t.Fatalf("unexpected error(s): %v", errs)
				}
				if len(errs) != 1 {
					t.Fatalf("exactly one error expected")
				}
				if errMsg := errs[0].Error(); !strings.Contains(errMsg, tc.expectValidationError) {
					t.Fatalf("error %#v does not contain the expected message %q", errMsg, tc.expectValidationError)
				}
			}
			if tc.expectObj != nil {
				if diff := cmp.Diff(tc.expectObj, workload); diff != "" {
					t.Errorf("got unexpected workload object (-want, +got): %s", diff)
				}
			}
		})
	}
}

func TestPodSchedulingStrategyUpdate(t *testing.T) {
	t.Run("no changes", func(t *testing.T) {
		ctx := ctxWithRequestInfo()
		workload := workload.DeepCopy()
		newWorkload := workload.DeepCopy()
		newWorkload.ResourceVersion = "4"

		Strategy.PrepareForUpdate(ctx, newWorkload, workload)
		errs := Strategy.ValidateUpdate(ctx, newWorkload, workload)
		if len(errs) != 0 {
			t.Errorf("Unexpected validation error: %v", errs)
		}
	})

	t.Run("name update", func(t *testing.T) {
		ctx := ctxWithRequestInfo()
		workload := workload.DeepCopy()
		newWorkload := workload.DeepCopy()
		newWorkload.Name += "bar"
		newWorkload.ResourceVersion = "4"

		Strategy.PrepareForUpdate(ctx, newWorkload, workload)
		errs := Strategy.ValidateUpdate(ctx, newWorkload, workload)
		if len(errs) == 0 {
			t.Errorf("Expected validation error")
		}
	})

	t.Run("invalid spec update - controllerRef", func(t *testing.T) {
		ctx := ctxWithRequestInfo()
		workload := workload.DeepCopy()
		newWorkload := workload.DeepCopy()
		newWorkload.Spec.ControllerRef = &scheduling.TypedLocalObjectReference{
			Kind: "foo",
			Name: "baz",
		}
		newWorkload.ResourceVersion = "4"

		Strategy.PrepareForUpdate(ctx, newWorkload, workload)
		errs := Strategy.ValidateUpdate(ctx, newWorkload, workload)
		if len(errs) == 0 {
			t.Errorf("Expected validation error")
		}
	})

	t.Run("invalid spec update - podGroupTemplates", func(t *testing.T) {
		ctx := ctxWithRequestInfo()
		workload := workload.DeepCopy()
		newWorkload := workload.DeepCopy()
		newWorkload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount = 4
		newWorkload.ResourceVersion = "4"

		Strategy.PrepareForUpdate(ctx, newWorkload, workload)
		errs := Strategy.ValidateUpdate(ctx, newWorkload, workload)
		if len(errs) == 0 {
			t.Errorf("Expected validation error")
		}
	})
}

func TestPodSchedulingStrategyUpdate_SchedulingConstraints(t *testing.T) {
	testCases := map[string]struct {
		oldObj                *scheduling.Workload
		newObj                *scheduling.Workload
		expectObj             *scheduling.Workload
		expectValidationError string
		tasEnabled            bool
	}{
		"valid update with scheduling constraints unchanged and TAS disabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			tasEnabled: false,
		},
		"valid update with scheduling constraints unchanged and TAS enabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			tasEnabled: true,
		},
		"changing topology key not allowed with TAS disabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "bar"}},
				}
				return workload
			}(),
			expectValidationError: "field is immutable",
			tasEnabled:            false,
		},
		"changing topology key not allowed with TAS enabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "bar"}},
				}
				return workload
			}(),
			expectValidationError: "field is immutable",
			tasEnabled:            true,
		},
		"changing topology constraints not allowed with TAS disabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				workload.Spec.PodGroupTemplates[1].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectValidationError: "field is immutable",
			tasEnabled:            false,
		},
		"changing topology constraints not allowed with TAS enabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				workload.Spec.PodGroupTemplates[1].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectValidationError: "field is immutable",
			tasEnabled:            true,
		},
		"adding scheduling constraints not allowed with TAS disabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				workload.Spec.PodGroupTemplates[1].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectValidationError: "field is immutable",
			tasEnabled:            false,
		},
		"adding scheduling constraints not allowed with TAS enabled": {
			oldObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			newObj: func() *scheduling.Workload {
				workload := workload.DeepCopy()
				workload.Spec.PodGroupTemplates = append(workload.Spec.PodGroupTemplates, *workload.Spec.PodGroupTemplates[0].DeepCopy())
				workload.Spec.PodGroupTemplates[0].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				workload.Spec.PodGroupTemplates[1].SchedulingConstraints = &scheduling.PodGroupSchedulingConstraints{
					Topology: []scheduling.TopologyConstraint{{Key: "foo"}},
				}
				return workload
			}(),
			expectValidationError: "field is immutable",
			tasEnabled:            true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.GenericWorkload:                 tc.tasEnabled,
				features.TopologyAwareWorkloadScheduling: tc.tasEnabled,
			})
			ctx := ctxWithRequestInfo()
			oldWorkload := tc.oldObj.DeepCopy()
			oldWorkload.ResourceVersion = "1"
			newWorkload := tc.newObj.DeepCopy()
			newWorkload.ResourceVersion = "2"

			Strategy.PrepareForUpdate(ctx, newWorkload, oldWorkload)
			if errs := Strategy.ValidateUpdate(ctx, newWorkload, oldWorkload); len(errs) != 0 {
				if tc.expectValidationError == "" {
					t.Fatalf("unexpected error(s): %v", errs)
				}
				if len(errs) != 1 {
					t.Fatalf("exactly one error expected")
				}
				if errMsg := errs[0].Error(); !strings.Contains(errMsg, tc.expectValidationError) {
					t.Fatalf("error %#v does not contain the expected message %q", errMsg, tc.expectValidationError)
				}
			}
			if tc.expectObj != nil {
				tc.expectObj.ResourceVersion = newWorkload.ResourceVersion
				if diff := cmp.Diff(tc.expectObj, newWorkload); diff != "" {
					t.Errorf("got unexpected workload object (-want, +got): %s", diff)
				}
			}
		})
	}
}

func TestDropPodGroupTemplateResourceClaims(t *testing.T) {
	var noWorkload *scheduling.Workload
	workloadWithoutClaims := workload
	workloadWithClaims := func() *scheduling.Workload {
		w := workloadWithoutClaims.DeepCopy()
		w.Spec.PodGroupTemplates[0].ResourceClaims = []scheduling.PodGroupResourceClaim{
			{
				Name:              "my-claim",
				ResourceClaimName: new("resource-claim"),
			},
		}
		return w
	}()

	tests := []struct {
		description  string
		enabled      bool
		oldWorkload  *scheduling.Workload
		newWorkload  *scheduling.Workload
		wantWorkload *scheduling.Workload
	}{
		{
			description:  "old with claims / new with claims / disabled",
			oldWorkload:  workloadWithClaims,
			newWorkload:  workloadWithClaims,
			wantWorkload: workloadWithClaims,
		},
		{
			description:  "old without claims / new with claims / disabled",
			oldWorkload:  workloadWithoutClaims,
			newWorkload:  workloadWithClaims,
			wantWorkload: workloadWithoutClaims,
		},
		{
			description:  "no old workload / new with claims / disabled",
			oldWorkload:  noWorkload,
			newWorkload:  workloadWithClaims,
			wantWorkload: workloadWithoutClaims,
		},

		{
			description:  "old with claims / new without claims / disabled",
			oldWorkload:  workloadWithClaims,
			newWorkload:  workloadWithoutClaims,
			wantWorkload: workloadWithoutClaims,
		},
		{
			description:  "old without claims / new without claims / disabled",
			oldWorkload:  workloadWithoutClaims,
			newWorkload:  workloadWithoutClaims,
			wantWorkload: workloadWithoutClaims,
		},
		{
			description:  "no old workload / new without claims / disabled",
			oldWorkload:  noWorkload,
			newWorkload:  workloadWithoutClaims,
			wantWorkload: workloadWithoutClaims,
		},

		{
			description:  "old with claims / new with claims / enabled",
			enabled:      true,
			oldWorkload:  workloadWithClaims,
			newWorkload:  workloadWithClaims,
			wantWorkload: workloadWithClaims,
		},
		{
			description:  "old without claims / new with claims / enabled",
			enabled:      true,
			oldWorkload:  workloadWithoutClaims,
			newWorkload:  workloadWithClaims,
			wantWorkload: workloadWithClaims,
		},
		{
			description:  "no old workload / new with claims / enabled",
			enabled:      true,
			oldWorkload:  noWorkload,
			newWorkload:  workloadWithClaims,
			wantWorkload: workloadWithClaims,
		},

		{
			description:  "old with claims / new without claims / enabled",
			enabled:      true,
			oldWorkload:  workloadWithClaims,
			newWorkload:  workloadWithoutClaims,
			wantWorkload: workloadWithoutClaims,
		},
		{
			description:  "old without claims / new without claims / enabled",
			enabled:      true,
			oldWorkload:  workloadWithoutClaims,
			newWorkload:  workloadWithoutClaims,
			wantWorkload: workloadWithoutClaims,
		},
		{
			description:  "no old workload / new without claims / enabled",
			enabled:      true,
			oldWorkload:  noWorkload,
			newWorkload:  workloadWithoutClaims,
			wantWorkload: workloadWithoutClaims,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRAWorkloadResourceClaims: tc.enabled,
				features.GenericWorkload:           tc.enabled,
			})

			oldWorkload := tc.oldWorkload.DeepCopy()
			newWorkload := tc.newWorkload.DeepCopy()
			wantWorkload := tc.wantWorkload
			dropDisabledWorkloadFields(newWorkload, oldWorkload)

			// old Workload should never be changed
			if diff := cmp.Diff(oldWorkload, tc.oldWorkload); diff != "" {
				t.Errorf("old Workload changed: %s", diff)
			}

			if diff := cmp.Diff(wantWorkload, newWorkload); diff != "" {
				t.Errorf("new Workload changed (- want, + got): %s", diff)
			}
		})
	}
}
