/*
Copyright 2020 The Kubernetes Authors.

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

package instancestate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"k8s.io/utils/ptr"

	infrav1 "sigs.k8s.io/cluster-api-provider-aws/v2/api/v1beta2"
	mocks "sigs.k8s.io/cluster-api-provider-aws/v2/test/mocks/v2"
)

func TestReconcileRules(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	ruleName := "test-cluster-ec2-rule"

	testCases := []struct {
		name                        string
		eventBridgeExpect           func(m *mocks.MockEventBridgeClientMockRecorder)
		postCreateEventBridgeExpect func(m *mocks.MockEventBridgeClientMockRecorder)
		sqsExpect                   func(m *mocks.MockSQSAPIMockRecorder)
		expectErr                   bool
	}{
		{
			name: "successfully creates missing rule and target",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), gomock.Eq(&eventbridge.DescribeRuleInput{
					Name: ptr.To(ruleName),
				})).Return(nil, &types.ResourceNotFoundException{})
				e := &eventPattern{
					Source:     []string{"aws.ec2"},
					DetailType: []string{Ec2StateChangeNotification},
					EventDetail: &eventDetail{
						States: []infrav1.InstanceState{infrav1.InstanceStateShuttingDown, infrav1.InstanceStateTerminated},
					},
				}
				data, err := json.Marshal(e)
				if err != nil {
					t.Fatalf("got an unexpected error: %v", err)
				}
				m.PutRule(gomock.Any(), gomock.Eq(&eventbridge.PutRuleInput{
					Name:         ptr.To(ruleName),
					State:        types.RuleStateDisabled,
					EventPattern: ptr.To(string(data)),
				}))
			},
			postCreateEventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), gomock.Eq(&eventbridge.DescribeRuleInput{
					Name: ptr.To(ruleName),
				})).Return(&eventbridge.DescribeRuleOutput{Name: ptr.To(ruleName), Arn: ptr.To("rule-arn")}, nil)
				m.ListTargetsByRule(gomock.Any(), &eventbridge.ListTargetsByRuleInput{
					Rule: ptr.To(ruleName),
				}).Return(&eventbridge.ListTargetsByRuleOutput{
					Targets: []types.Target{{
						Id:  ptr.To("another-queue"),
						Arn: ptr.To("another-queue-arn"),
					}},
				}, nil)
				m.PutTargets(gomock.Any(), gomock.Eq(&eventbridge.PutTargetsInput{
					Rule: ptr.To(ruleName),
					Targets: []types.Target{{
						Arn: ptr.To("test-cluster-queue-arn"),
						Id:  ptr.To("test-cluster-queue"),
					}},
				}))
			},
			sqsExpect: func(m *mocks.MockSQSAPIMockRecorder) {
				m.GetQueueUrl(gomock.Any(), gomock.Eq(&sqs.GetQueueUrlInput{
					QueueName: ptr.To("test-cluster-queue"),
				})).Return(&sqs.GetQueueUrlOutput{QueueUrl: ptr.To("test-cluster-queue-url")}, nil)
				attrs := make(map[string]string)
				attrs[string(sqstypes.QueueAttributeNameQueueArn)] = "test-cluster-queue-arn"
				m.GetQueueAttributes(gomock.Any(), gomock.Eq(&sqs.GetQueueAttributesInput{
					AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn, sqstypes.QueueAttributeNamePolicy},
					QueueUrl:       ptr.To("test-cluster-queue-url"),
				}), gomock.Any()).Return(&sqs.GetQueueAttributesOutput{Attributes: attrs}, nil)
				m.SetQueueAttributes(gomock.Any(), gomock.AssignableToTypeOf(&sqs.SetQueueAttributesInput{}), gomock.Any()).Return(nil, nil)
			},
			expectErr: false,
		},
		{
			name: "skips creating target and queue policy if they already exist",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), gomock.Eq(&eventbridge.DescribeRuleInput{
					Name: ptr.To(ruleName),
				})).Return(&eventbridge.DescribeRuleOutput{Name: ptr.To(ruleName), Arn: ptr.To("rule-arn")}, nil)
				m.ListTargetsByRule(gomock.Any(), gomock.AssignableToTypeOf(&eventbridge.ListTargetsByRuleInput{})).Return(&eventbridge.ListTargetsByRuleOutput{
					Targets: []types.Target{{
						Id:  ptr.To("test-cluster-queue"),
						Arn: ptr.To("test-cluster-queue-arn"),
					}},
				}, nil)
			},
			postCreateEventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {},
			sqsExpect: func(m *mocks.MockSQSAPIMockRecorder) {
				m.GetQueueUrl(gomock.Any(), gomock.AssignableToTypeOf(&sqs.GetQueueUrlInput{}), gomock.Any()).Return(&sqs.GetQueueUrlOutput{QueueUrl: ptr.To("test-cluster-queue-url")}, nil)
				attrs := make(map[string]string)
				attrs[string(sqstypes.QueueAttributeNameQueueArn)] = "test-cluster-queue-arn"
				attrs[string(sqstypes.QueueAttributeNamePolicy)] = "some policy"
				m.GetQueueAttributes(gomock.Any(), gomock.AssignableToTypeOf(&sqs.GetQueueAttributesInput{}), gomock.Any()).Return(&sqs.GetQueueAttributesOutput{Attributes: attrs}, nil)
			},
		},
		{
			name: "returns error if DescribeRule runs into unexpected error",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), gomock.Eq(&eventbridge.DescribeRuleInput{
					Name: ptr.To(ruleName),
				})).Return(nil, errors.New("some error"))
			},
			postCreateEventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {},
			sqsExpect:                   func(m *mocks.MockSQSAPIMockRecorder) {},
			expectErr:                   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			eventbridgeMock := mocks.NewMockEventBridgeClient(mockCtrl)
			sqsMock := mocks.NewMockSQSAPI(mockCtrl)
			ctx := context.Background()
			clusterScope, err := setupCluster("test-cluster")
			g.Expect(err).To(Not(HaveOccurred()))
			tc.sqsExpect(sqsMock.EXPECT())
			tc.eventBridgeExpect(eventbridgeMock.EXPECT())
			tc.postCreateEventBridgeExpect(eventbridgeMock.EXPECT())

			s := NewService(clusterScope)
			s.EventBridgeClient = eventbridgeMock
			s.SQSClient = sqsMock

			err = s.reconcileRules(ctx)
			if tc.expectErr {
				g.Expect(err).NotTo(BeNil())
			} else {
				g.Expect(err).To(BeNil())
			}
		})
	}
}

func TestDeleteRules(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	testCases := []struct {
		name              string
		eventBridgeExpect func(m *mocks.MockEventBridgeClientMockRecorder)
		expectErr         bool
	}{
		{
			name: "removes target and ec2 rule successfully when they both exist",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.RemoveTargets(gomock.Any(), gomock.Eq(&eventbridge.RemoveTargetsInput{
					Rule: ptr.To("test-cluster-ec2-rule"),
					Ids:  []string{"test-cluster-queue"},
				})).Return(nil, nil)
				m.DeleteRule(gomock.Any(), gomock.Eq(&eventbridge.DeleteRuleInput{
					Name: ptr.To("test-cluster-ec2-rule"),
				})).Return(nil, nil)
			},
			expectErr: false,
		},
		{
			name: "continues to remove rule when target doesn't exist",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.RemoveTargets(gomock.Any(), gomock.AssignableToTypeOf(&eventbridge.RemoveTargetsInput{})).
					Return(nil, &types.ResourceNotFoundException{})
				m.DeleteRule(gomock.Any(), gomock.AssignableToTypeOf(&eventbridge.DeleteRuleInput{})).Return(nil, nil)
			},
			expectErr: false,
		},
		{
			name: "returns error when remove target fails unexpectedly",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.RemoveTargets(gomock.Any(), gomock.AssignableToTypeOf(&eventbridge.RemoveTargetsInput{})).Return(nil, errors.New("some error"))
			},
			expectErr: true,
		},
		{
			name: "returns error when delete rule fails unexpectedly",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.RemoveTargets(gomock.Any(), gomock.AssignableToTypeOf(&eventbridge.RemoveTargetsInput{})).Return(nil, nil)
				m.DeleteRule(gomock.Any(), gomock.AssignableToTypeOf(&eventbridge.DeleteRuleInput{})).Return(nil, errors.New("some error"))
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			eventbridgeMock := mocks.NewMockEventBridgeClient(mockCtrl)
			clusterScope, err := setupCluster("test-cluster")
			g.Expect(err).To(Not(HaveOccurred()))
			tc.eventBridgeExpect(eventbridgeMock.EXPECT())

			s := NewService(clusterScope)
			s.EventBridgeClient = eventbridgeMock

			err = s.deleteRules(context.Background())
			if tc.expectErr {
				g.Expect(err).NotTo(BeNil())
			} else {
				g.Expect(err).To(BeNil())
			}
		})
	}
}

func TestAddInstanceToRule(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	pattern := eventPattern{
		DetailType: []string{Ec2StateChangeNotification},
		Source:     []string{"aws.ec2"},
		EventDetail: &eventDetail{
			InstanceIDs: []string{"instance-a"},
		},
	}
	patternData, err := json.Marshal(pattern)
	if err != nil {
		t.Fatalf("got an unexpected error: %v", err)
	}

	testCases := []struct {
		name              string
		eventBridgeExpect func(m *mocks.MockEventBridgeClientMockRecorder)
		newInstanceID     string
		expectErr         bool
	}{
		{
			name: "adds instance to event pattern when it doesn't exist",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), &eventbridge.DescribeRuleInput{
					Name: ptr.To("test-cluster-ec2-rule"),
				}).Return(&eventbridge.DescribeRuleOutput{
					EventPattern: ptr.To(string(patternData)),
				}, nil)
				expectedPattern := pattern
				expectedPattern.EventDetail.InstanceIDs = append(expectedPattern.EventDetail.InstanceIDs, "instance-b")
				expectedData, err := json.Marshal(expectedPattern)
				if err != nil {
					t.Fatalf("got an unexpected error: %v", err)
				}
				m.PutRule(gomock.Any(), &eventbridge.PutRuleInput{
					Name:         ptr.To("test-cluster-ec2-rule"),
					EventPattern: ptr.To(string(expectedData)),
					State:        types.RuleStateEnabled,
				}).Return(nil, nil)
			},
			newInstanceID: "instance-b",
			expectErr:     false,
		},
		{
			name: "does nothing if instance is already tracked in event pattern",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), &eventbridge.DescribeRuleInput{
					Name: ptr.To("test-cluster-ec2-rule"),
				}).Return(&eventbridge.DescribeRuleOutput{
					EventPattern: ptr.To(string(patternData)),
				}, nil)
			},
			newInstanceID: "instance-a",
			expectErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			eventbridgeMock := mocks.NewMockEventBridgeClient(mockCtrl)
			clusterScope, err := setupCluster("test-cluster")
			g.Expect(err).To(Not(HaveOccurred()))
			tc.eventBridgeExpect(eventbridgeMock.EXPECT())

			s := NewService(clusterScope)
			s.EventBridgeClient = eventbridgeMock

			err = s.AddInstanceToEventPattern(context.Background(), tc.newInstanceID)
			if tc.expectErr {
				g.Expect(err).NotTo(BeNil())
			} else {
				g.Expect(err).To(BeNil())
			}
		})
	}
}

func TestRemoveInstanceStateFromEventPattern(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	pattern := eventPattern{
		DetailType: []string{Ec2StateChangeNotification},
		Source:     []string{"aws.ec2"},
		EventDetail: &eventDetail{
			InstanceIDs: []string{"instance-a", "instance-b", "instance-c"},
		},
	}
	patternData, err := json.Marshal(pattern)
	if err != nil {
		t.Fatalf("got an unexpected error: %v", err)
	}

	testCases := []struct {
		name              string
		eventBridgeExpect func(m *mocks.MockEventBridgeClientMockRecorder)
		instanceID        string
	}{
		{
			name: "remove instance from instance IDs and disables rule when no instances are tracked",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				singleInstanceEventPattern := pattern
				singleInstanceEventPattern.EventDetail.InstanceIDs = []string{"instance-a"}
				patternData, err := json.Marshal(pattern)
				if err != nil {
					t.Fatalf("got an unexpected error: %v", err)
				}
				m.DescribeRule(gomock.Any(), &eventbridge.DescribeRuleInput{
					Name: ptr.To("test-cluster-ec2-rule"),
				}).Return(&eventbridge.DescribeRuleOutput{
					EventPattern: ptr.To(string(patternData)),
				}, nil)
				expectedPattern := pattern
				expectedPattern.EventDetail.InstanceIDs = []string{}
				expectedData, err := json.Marshal(expectedPattern)
				if err != nil {
					t.Fatalf("got an unexpected error: %v", err)
				}

				m.PutRule(gomock.Any(), &eventbridge.PutRuleInput{
					Name:         ptr.To("test-cluster-ec2-rule"),
					EventPattern: ptr.To(string(expectedData)),
					State:        types.RuleStateDisabled,
				}).Return(nil, nil)
			},
			instanceID: "instance-a",
		},
		{
			name: "remove instance from instance IDs and rule remains enabled when other instances are tracked",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), &eventbridge.DescribeRuleInput{
					Name: ptr.To("test-cluster-ec2-rule"),
				}).Return(&eventbridge.DescribeRuleOutput{
					EventPattern: ptr.To(string(patternData)),
				}, nil)
				expectedPattern := pattern
				expectedPattern.EventDetail.InstanceIDs = []string{"instance-a", "instance-c"}
				expectedData, err := json.Marshal(expectedPattern)
				if err != nil {
					t.Fatalf("got an unexpected error: %v", err)
				}
				m.PutRule(gomock.Any(), &eventbridge.PutRuleInput{
					Name:         ptr.To("test-cluster-ec2-rule"),
					EventPattern: ptr.To(string(expectedData)),
					State:        types.RuleStateEnabled,
				}).Return(nil, nil)
			},
			instanceID: "instance-b",
		},
		{
			name: "does nothing when instanceID is not tracked",
			eventBridgeExpect: func(m *mocks.MockEventBridgeClientMockRecorder) {
				m.DescribeRule(gomock.Any(), &eventbridge.DescribeRuleInput{
					Name: ptr.To("test-cluster-ec2-rule"),
				}).Return(&eventbridge.DescribeRuleOutput{
					EventPattern: ptr.To(string(patternData)),
				}, nil)
			},
			instanceID: "instance-d",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			eventbridgeMock := mocks.NewMockEventBridgeClient(mockCtrl)
			clusterScope, err := setupCluster("test-cluster")
			g.Expect(err).To(Not(HaveOccurred()))
			tc.eventBridgeExpect(eventbridgeMock.EXPECT())

			s := NewService(clusterScope)
			s.EventBridgeClient = eventbridgeMock

			s.RemoveInstanceFromEventPattern(context.Background(), tc.instanceID)
		})
	}
}
