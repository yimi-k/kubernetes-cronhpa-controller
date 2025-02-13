/*
Copyright 2018 zhongwei.lzw@alibaba-inc.com.

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

package controller

import (
	"context"
	"fmt"
	"github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	autoscalingv1beta1 "github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	log "k8s.io/klog/v2"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// newReconciler returns a new reconcile.Reconciler
func NewReconciler(mgr manager.Manager) reconcile.Reconciler {
	var stopChan chan struct{}
	cm := NewCronManager(nil, mgr)
	r := &ReconcileCronHorizontalPodAutoscaler{Client: mgr.GetClient(), scheme: mgr.GetScheme(), CronManager: cm}
	go func(cronManager *CronManager, stopChan chan struct{}) {
		cm.Run(stopChan)
		<-stopChan
	}(cm, stopChan)

	go func(cronManager *CronManager, stopChan chan struct{}) {
		server := NewWebServer(cronManager)
		server.serve()
	}(cm, stopChan)
	return r
}

var _ reconcile.Reconciler = &ReconcileCronHorizontalPodAutoscaler{}

// ReconcileCronHorizontalPodAutoscaler reconciles a CronHorizontalPodAutoscaler object
type ReconcileCronHorizontalPodAutoscaler struct {
	client.Client
	scheme      *runtime.Scheme
	CronManager *CronManager
}

// Reconcile reads that state of the cluster for a CronHorizontalPodAutoscaler object and makes changes based on the state read
// and what is in the CronHorizontalPodAutoscaler.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  The scaffolding writes
// a Deployment as an example
// Automatically generate RBAC rules to allow the Controller to read and write Deployments
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.alibabacloud.com,resources=cronhorizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
func (r *ReconcileCronHorizontalPodAutoscaler) Reconcile(context context.Context, request reconcile.Request) (reconcile.Result, error) {
	// Fetch the CronHorizontalPodAutoscaler instance
	log.Infof("Start to handle cronHPA %s in %s namespace", request.Name, request.Namespace)
	cronHpa := &autoscalingv1beta1.CronHorizontalPodAutoscaler{}
	err := r.Get(context, request.NamespacedName, cronHpa)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			log.Infof("GC start for: cronHPA %s in %s namespace is not found", request.Name, request.Namespace)
			r.CronManager.jobManager.RemoveAllByCronHpa(request.Name)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	r.CronManager.Clean(cronHpa)

	conditions := make([]autoscalingv1beta1.Condition, 0)
	for _, job := range cronHpa.Spec.Jobs {
		condition := NewCondition(job)
		var cronJob *BaseCronJob
		if cronJob, err = NewCronJob(cronHpa, job); err != nil {
			condition.State = v1beta1.Failed
			condition.Message = fmt.Sprintf("Failed to create cron hpa job %s in %s namespace %s,because of %v",
				job.Name, cronHpa.Name, cronHpa.Namespace, err)
		} else {
			if err := r.CronManager.jobManager.Add(cronJob); err != nil {
				condition.State = v1beta1.Failed
				condition.Message = fmt.Sprintf("Failed to update cron hpa job %s,because of %v", job.Name, err)
			} else {
				condition.State = v1beta1.Submitted
			}
			condition.JobId = cronJob.ID()
		}
		conditions = append(conditions, condition)
	}

	if err := r.Update(context, cronHpa); err != nil {
		log.Errorf("Failed to update cron hpa %s in namespace %s status, because of %v", cronHpa.Name, cronHpa.Namespace, err)
	}

	log.Infof("%v has been handled completely.", cronHpa.Name)
	return reconcile.Result{}, err
}

func Register(mgr manager.Manager) {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&autoscalingv1beta1.CronHorizontalPodAutoscaler{}).
		Complete(NewReconciler(mgr))
	if err != nil {
		log.Errorf("Failed to set up controller watch loop,because of %v", err)
		os.Exit(1)
	}
}
