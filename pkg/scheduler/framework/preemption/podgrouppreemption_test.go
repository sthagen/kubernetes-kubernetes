/*
Copyright The Kubernetes Authors.

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

package preemption

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	schedulingapi "k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func TestPodGroupEvaluator_SelectVictimsOnDomain(t *testing.T) {
	// blockingRule mocks complex scheduling constraints for tests.
	// It states: "Node 'nodeName' can host 'capacity' preempting pods,
	// but ONLY IF the pods in 'blockingVictims' are removed first."
	type blockingRule struct {
		nodeName        string
		capacity        int              // Available slots for preemptor once victims are removed.
		blockingVictims sets.Set[string] // Pods on the node preventing capacity usage.
	}

	tests := []struct {
		name           string
		nodeNames      []string
		initPods       []*v1.Pod
		preemptor      *podGroupPreemptor
		pdbs           []*policy.PodDisruptionBudget
		blockingRules  []blockingRule
		expectedPods   []string
		expectedStatus *fwk.Status
	}{
		{
			name:      "Mix of no-group and single-pod-groups",
			nodeNames: []string{"node1", "node2", "node3"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			preemptor: newPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: sets.New("p1")},
				{nodeName: "node2", capacity: 1, blockingVictims: sets.New("p2")},
				{nodeName: "node3", capacity: 1, blockingVictims: sets.New("p3")},
			},
			expectedPods:   []string{"p1"}, // p1 is less important than p2 because it's not part of a pod group
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:      "Priority: Shared group vs no group",
			nodeNames: []string{"node1", "node2", "node3"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(1, 0)).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(0, 0)).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(midPriority).Obj(),
			},
			preemptor: newPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: sets.New("p1")},
				{nodeName: "node2", capacity: 1, blockingVictims: sets.New("p2")},
				{nodeName: "node3", capacity: 1, blockingVictims: sets.New("p3")},
			},
			expectedPods:   []string{"p1"}, // p1 is less important than p2 because of later StartTime
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:      "Shared Group: Preempt separately",
			nodeNames: []string{"node1", "node2"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(1, 0)).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(0, 0)).Obj(),
			},
			preemptor: newPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: sets.New("p1")},
				{nodeName: "node2", capacity: 1, blockingVictims: sets.New("p2")},
			},
			expectedPods:   []string{"p1"}, // p1 is less important than p2
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:      "Complex Mixed: Shared, different, and no groups",
			nodeNames: []string{"node1", "node2", "node3", "node4", "node5"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(2, 0)).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(1, 0)).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").StartTime(metav1.Unix(0, 0)).Obj(),
				st.MakePod().Name("p4").UID("v4").Node("node4").Priority(midPriority).Obj(),
				st.MakePod().Name("p5").UID("v5").Node("node5").Priority(highPriority).PodGroupName("pg3").StartTime(metav1.Unix(0, 0)).Obj(),
			},
			preemptor: newPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{
					st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).Obj(),
					st.MakePod().Name("p-2").UID("p-2").Priority(highPriority).Obj(),
					st.MakePod().Name("p-3").UID("p-3").Priority(highPriority).Obj(),
				},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: sets.New("p1")},
				{nodeName: "node2", capacity: 1, blockingVictims: sets.New("p2")},
				{nodeName: "node3", capacity: 1, blockingVictims: sets.New("p3")},
				{nodeName: "node4", capacity: 1, blockingVictims: sets.New("p4")},
				{nodeName: "node5", capacity: 1, blockingVictims: sets.New("p5")},
			},
			expectedPods:   []string{"p1", "p2", "p3"},
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:      "PDB: Mixed groups",
			nodeNames: []string{"node1", "node2"},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim-pdb").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("victim-no-pdb").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: newPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: sets.New("victim-pdb")},
				{nodeName: "node2", capacity: 1, blockingVictims: sets.New("victim-no-pdb")},
			},
			expectedPods:   []string{"victim-no-pdb"},
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:      "PodGroup with PreemptNever does not perform preemption",
			nodeNames: []string{"node1", "node2", "node3"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			preemptor: newPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{
					st.MakePod().Name("p-1").UID("p-1").Priority(highPriority).PreemptionPolicy(v1.PreemptNever).Obj(),
					st.MakePod().Name("p-2").UID("p-2").Priority(highPriority).Obj(),
				},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: sets.New("p1")},

				{nodeName: "node2", capacity: 1, blockingVictims: sets.New("p2")},
				{nodeName: "node3", capacity: 1, blockingVictims: sets.New("p3")},
			},
			expectedPods:   []string{},
			expectedStatus: fwk.NewStatus(fwk.Unschedulable),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			nodes := make([]*v1.Node, len(tt.nodeNames))
			for i, nodeName := range tt.nodeNames {
				nodes[i] = st.MakeNode().Name(nodeName).Obj()
			}

			// Build nodes with pods
			nodeInfos := make(map[string]fwk.NodeInfo)
			for _, node := range nodes {
				nodeInfos[node.Name] = framework.NewNodeInfo()
				nodeInfos[node.Name].SetNode(node)
			}
			for _, p := range tt.initPods {
				podInfo, _ := framework.NewPodInfo(p)
				nodeInfos[p.Spec.NodeName].AddPodInfo(podInfo)
			}

			var domainNodes []fwk.NodeInfo
			for _, name := range tt.nodeNames {
				domainNodes = append(domainNodes, nodeInfos[name])
			}

			domain := newDomainForWorkloadPreemption(domainNodes, "test-domain")

			// Create a mock podGroupSchedulingFunc.
			// This simulates whether the preempting PodGroup can schedule given the current
			// hypothetical state of the cluster (where some candidate victims might be removed).
			mockSchedulingFunc := func(ctx context.Context) *fwk.Status {
				neededSlots := len(tt.preemptor.Members())
				availableSlots := 0
				nodeMap := make(map[string]fwk.NodeInfo)
				for _, n := range domainNodes {
					nodeMap[n.Node().Name] = n
				}

				for _, rule := range tt.blockingRules {
					node, exists := nodeMap[rule.nodeName]
					if !exists {
						continue
					}

					isBlocked := false
					for _, pod := range node.GetPods() {
						// If any of the blocking victims are still on the simulated node, it provides 0 capacity.
						if rule.blockingVictims.Has(pod.GetPod().Name) {
							isBlocked = true
							break
						}
					}

					// If all blocking victims are removed, the node provides its capacity.
					if !isBlocked {
						availableSlots += rule.capacity
					}
				}

				if availableSlots >= neededSlots {
					return fwk.NewStatus(fwk.Success)
				}
				return fwk.NewStatus(fwk.Unschedulable)
			}

			pl := &PodGroupEvaluator{}

			victims, gotStatus := pl.selectVictimsOnDomain(ctx, tt.preemptor, domain, tt.pdbs, mockSchedulingFunc)
			if !gotStatus.IsSuccess() {
				t.Logf("SelectVictimsOnDomain failed: %v", gotStatus.Message())
			}

			wantCode := tt.expectedStatus.Code()
			gotCode := gotStatus.Code()
			if gotCode != wantCode {
				t.Errorf("Status mismatch. Want %v, Got %v", wantCode, gotCode)
			}
			if wantCode != fwk.Success {
				return
			}

			gotPods := victims.Pods
			gotNames := sets.Set[string]{}
			for _, p := range gotPods {
				gotNames.Insert(p.Name)
			}
			wantNames := sets.New(tt.expectedPods...)
			if diff := cmp.Diff(wantNames, gotNames); diff != "" {
				t.Errorf("Victims mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMoreImportantVictim(t *testing.T) {
	newPodInfo := func(p *v1.Pod) fwk.PodInfo {
		pi, _ := framework.NewPodInfo(p)
		return pi
	}

	now := &metav1.Time{Time: time.Unix(1000, 0)}
	before := &metav1.Time{Time: time.Unix(500, 0)}

	tests := []struct {
		name string
		vi1  *victim
		vi2  *victim
		want bool
	}{
		{
			name: "vi1 has higher priority",
			vi1:  &victim{priority: 20},
			vi2:  &victim{priority: 10},
			want: true,
		},
		{
			name: "vi2 has higher priority",
			vi1:  &victim{priority: 10},
			vi2:  &victim{priority: 20},
			want: false,
		},
		{
			name: "vi1 is PG, vi2 is Pod, same priority",
			vi1:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().PodGroupName("pg").Obj())}},
			vi2:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().Obj())}},
			want: true,
		},
		{
			name: "vi1 is Pod, vi2 is PG, same priority",
			vi1:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().Obj())}},
			vi2:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().PodGroupName("pg").Obj())}},
			want: false,
		},
		{
			name: "both Pods, vi1 older",
			vi1:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().Obj())}, earliestStartTime: before},
			vi2:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().Obj())}, earliestStartTime: now},
			want: true,
		},
		{
			name: "both Pods, vi2 older",
			vi1:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().Obj())}, earliestStartTime: now},
			vi2:  &victim{priority: 10, pods: []fwk.PodInfo{newPodInfo(st.MakePod().Obj())}, earliestStartTime: before},
			want: false,
		},
		{
			name: "both PGs, vi1 larger",
			vi1: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
			},
			vi2: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
			},
			want: true,
		},
		{
			name: "both PGs, vi2 larger",
			vi1: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
			},
			vi2: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
			},
			want: false,
		},
		{
			name: "both PGs, same size, vi1 older",
			vi1: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
				earliestStartTime: before,
			},
			vi2: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
				earliestStartTime: now,
			},
			want: true,
		},
		{
			name: "both PGs, same size, vi2 older",
			vi1: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
				earliestStartTime: now,
			},
			vi2: &victim{
				priority: 10,
				pods: []fwk.PodInfo{
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
					newPodInfo(st.MakePod().PodGroupName("pg").Obj()),
				},
				earliestStartTime: before,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := moreImportantVictim(tt.vi1, tt.vi2)
			if got != tt.want {
				t.Errorf("MoreImportantVictim() = %v, want %v", got, tt.want)
			}
		})
	}
}
