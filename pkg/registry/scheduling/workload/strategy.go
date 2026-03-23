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

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/apis/scheduling/validation"
	"k8s.io/kubernetes/pkg/features"
)

// workloadStrategy implements behavior for Workload objects.
type workloadStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating Workload objects.
var Strategy = workloadStrategy{legacyscheme.Scheme, names.SimpleNameGenerator}

func (workloadStrategy) NamespaceScoped() bool {
	return true
}

func (workloadStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	dropDisabledWorkloadFields(obj.(*scheduling.Workload), nil)
}

func (workloadStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	workloadScheduling := obj.(*scheduling.Workload)
	allErrs := validation.ValidateWorkload(workloadScheduling)
	opts := []string{}
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) || anySchedulingConstraintsInUse(nil) {
		opts = append(opts, string(features.TopologyAwareWorkloadScheduling))
	}
	return rest.ValidateDeclarativelyWithMigrationChecks(ctx, legacyscheme.Scheme, obj, nil, allErrs, operation.Create, rest.WithDeclarativeEnforcement(), rest.WithOptions(opts))
}

func (workloadStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (workloadStrategy) Canonicalize(obj runtime.Object) {}

func (workloadStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (workloadStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	dropDisabledWorkloadFields(obj.(*scheduling.Workload), old.(*scheduling.Workload))
}

func (workloadStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	allErrs := validation.ValidateWorkloadUpdate(obj.(*scheduling.Workload), old.(*scheduling.Workload))
	opts := []string{}
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) || anySchedulingConstraintsInUse(old.(*scheduling.Workload)) {
		opts = append(opts, string(features.TopologyAwareWorkloadScheduling))
	}
	return rest.ValidateDeclarativelyWithMigrationChecks(ctx, legacyscheme.Scheme, obj, old, allErrs, operation.Update, rest.WithDeclarativeEnforcement(), rest.WithOptions(opts))
}

func (workloadStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (workloadStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// dropDisabledWorkloadFields removes fields which are covered by a feature gate.
func dropDisabledWorkloadFields(workload, oldWorkload *scheduling.Workload) {
	var workloadSpec, oldWorkloadSpec *scheduling.WorkloadSpec
	if workload != nil {
		workloadSpec = &workload.Spec
	}
	if oldWorkload != nil {
		oldWorkloadSpec = &oldWorkload.Spec
	}
	dropDisabledWorkloadSpecFields(workloadSpec, oldWorkloadSpec)
}

func dropDisabledWorkloadSpecFields(workloadSpec, oldWorkloadSpec *scheduling.WorkloadSpec) {
	var templates, oldTemplates []scheduling.PodGroupTemplate
	if workloadSpec != nil {
		templates = workloadSpec.PodGroupTemplates
	}
	if oldWorkloadSpec != nil {
		oldTemplates = oldWorkloadSpec.PodGroupTemplates
	}
	dropDisabledPodGroupTemplatesFields(templates, oldTemplates)
}

func dropDisabledPodGroupTemplatesFields(templates, oldTemplates []scheduling.PodGroupTemplate) {
	m := len(oldTemplates)
	for i := range templates {
		var oldTemplate *scheduling.PodGroupTemplate
		if i < m {
			oldTemplate = &oldTemplates[i]
		}
		template := &templates[i]
		dropDisabledSchedulingConstraintsFields(template, oldTemplate)
		dropDisabledDRAWorkloadResourceClaimsFields(template, oldTemplate)
	}
}

// dropDisabledSchedulingConstraintsFields drops the SchedulingConstraints field
// from the PodGroupTemplate if the TopologyAwareWorkloadScheduling feature gate is disabled.
func dropDisabledSchedulingConstraintsFields(template, oldTemplate *scheduling.PodGroupTemplate) {
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) || schedulingConstraintsInUse(oldTemplate) {
		return
	}

	template.SchedulingConstraints = nil
}

func anySchedulingConstraintsInUse(workload *scheduling.Workload) bool {
	if workload == nil {
		return false
	}

	for i := range workload.Spec.PodGroupTemplates {
		if schedulingConstraintsInUse(&workload.Spec.PodGroupTemplates[i]) {
			return true
		}
	}
	return false
}

func schedulingConstraintsInUse(pgt *scheduling.PodGroupTemplate) bool {
	return pgt != nil && pgt.SchedulingConstraints != nil
}

// dropDisabledDRAWorkloadResourceClaimsFields removes resource claim references from
// podGroupTemplates unless they are already used by the old Workload spec.
func dropDisabledDRAWorkloadResourceClaimsFields(template, oldTemplate *scheduling.PodGroupTemplate) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims) || draWorkloadResourceClaimsInUse(oldTemplate) {
		return
	}
	template.ResourceClaims = nil
}

func draWorkloadResourceClaimsInUse(pgt *scheduling.PodGroupTemplate) bool {
	return pgt != nil && len(pgt.ResourceClaims) > 0
}
