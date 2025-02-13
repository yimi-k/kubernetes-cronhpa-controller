package controller

import (
	cronhpav1beta1 "github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"time"
)

type CronManager struct {
	jobExecutor CronJobExecutor
	jobManager  CronJobManager
}

func (cm *CronManager) Run(stopChan chan struct{}) {
	cm.jobManager.Start()
	<-stopChan
	cm.jobManager.Stop()
}

func (cm *CronManager) Clean(cronHpa *cronhpav1beta1.CronHorizontalPodAutoscaler) {
	cm.jobManager.RemoveAllByCronHpa(cronHpa.Name)
	cronHpa.Status.Conditions = make([]cronhpav1beta1.Condition, 0)
}

func NewCronManager(timezone *time.Location, mgr manager.Manager) *CronManager {
	cronJobExecutor = NewCronJobExecutor(mgr)

	return &CronManager{
		jobExecutor: cronJobExecutor,
		jobManager:  NewCronJobManager(timezone, CronJobResultHandler),
	}
}
