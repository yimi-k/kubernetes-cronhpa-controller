package controller

/*
	Add prometheus metrics to cronHPA controller
	GC Loop update metrics every 10mi

	Total = Successful + Submitted + Failed

	Expired jobs are unique state when cron engine have exceptions

*/

import (
	"github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"strconv"
)

var (
	kubeJobsInCronEngineTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kube_jobs_in_cron_engine_total",
			Help: "Jobs in queue of Cron Engine",
		},
		[]string{"cronhpa"},
	)

	kubeExecuteJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kube_cronhpa_execute_jobs_total",
			Help: "Skipped jobs per deployment",
		},
		[]string{"namespace", "cronhpa", "job", "target_size", "schedule", "status"},
	)
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(kubeJobsInCronEngineTotal)
	metrics.Registry.MustRegister(kubeExecuteJobsTotal)
}

func AddJobs(cronHpaName string, jobNum float64) {
	kubeJobsInCronEngineTotal.WithLabelValues(cronHpaName).Add(jobNum)
}

func IncExecuteJob(cronHpa *v1beta1.CronHorizontalPodAutoscaler, condition v1beta1.Condition) {
	kubeExecuteJobsTotal.WithLabelValues(cronHpa.Namespace, cronHpa.Name, condition.Name, strconv.FormatInt(int64(condition.TargetSize), 10),
		condition.Schedule, string(condition.State))
}
