package controller

import (
	"github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

func NewCondition(job v1beta1.Job) v1beta1.Condition {
	return v1beta1.Condition{
		Name:          job.Name,
		Schedule:      job.Schedule,
		RunOnce:       job.RunOnce,
		TargetSize:    job.TargetSize,
		LastProbeTime: metav1.Time{Time: time.Now()},
	}
}

func GenerateJobId(cronHpa *v1beta1.CronHorizontalPodAutoscaler, job v1beta1.Job) string {
	return cronHpa.Name + " # " + job.Name
}
