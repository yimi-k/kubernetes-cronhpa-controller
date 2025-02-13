package controller

import (
	"context"
	"fmt"
	cronhpav1beta1 "github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	"github.com/ringtail/go-cron"
	autoscaling "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/client-go/tools/record"
	log "k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"time"
)

const (
	MaxRetryTimes              = 3
	initialUpdateRetryInterval = 1 * time.Second
	maxUpdateRetryInterval     = 20 * time.Second
)

type NoNeedUpdate struct{}

func (n NoNeedUpdate) Error() string {
	return "NoNeedUpdate"
}

var cronJobExecutor CronJobExecutor

// CronJobExecutor is mainly related to the specific execution logic of CronJob
type CronJobExecutor interface {
	Run(job CronJob) (msg string, err error)
	HandleJobResult(js *cron.JobResult)
	UpdateCronHPAStatusWithRetry(instance *cronhpav1beta1.CronHorizontalPodAutoscaler, deepCopy *cronhpav1beta1.CronHorizontalPodAutoscaler, jobName string) error
	Client() client.Client
	EventRecorder() record.EventRecorder
}

type BaseCronJobExecutor struct {
	scaler        scaleclient.ScalesGetter
	mapper        apimeta.RESTMapper
	client        client.Client
	eventRecorder record.EventRecorder
}

func (je *BaseCronJobExecutor) Run(job CronJob) (msg string, err error) {
	if skip, msg := IsTodayOff(job.ExcludeDates()); skip {
		return msg, nil
	}

	targetRef := job.Ref()
	run := func() (bool, error) {
		if targetRef.RefKind == "HorizontalPodAutoscaler" {
			msg, err = je.ScaleHPA(job)
		} else {
			msg, err = je.ScalePlainRef(job)
		}

		// retry if the resource not found (maybe temporary) or the timeout occurs
		if k8serrors.IsNotFound(err) || k8serrors.IsTimeout(err) {
			return false, nil
		} else {
			return true, err
		}
	}

	backoff := wait.Backoff{
		Duration: initialUpdateRetryInterval,
		Factor:   2,
		Jitter:   0,
		Steps:    MaxRetryTimes,
		Cap:      maxUpdateRetryInterval,
	}
	err = wait.ExponentialBackoff(backoff, run)
	return msg, err
}

func (je *BaseCronJobExecutor) HandleJobResult(js *cron.JobResult) {
	job := js.Ref.(*BaseCronJob)
	cronHpa := &cronhpav1beta1.CronHorizontalPodAutoscaler{}
	if err := je.Client().Get(context.TODO(), types.NamespacedName{
		Namespace: job.CronHPA().Namespace,
		Name:      job.CronHPA().Name,
	}, cronHpa); err != nil {
		log.Errorf("Failed to fetch cronHPA job %s of cronHPA %s in namespace %s, because of %v",
			job.Name(), job.CronHPA().Name, job.CronHPA().Namespace, err)
		return
	}

	deepCopy := cronHpa.DeepCopy()
	var (
		state     cronhpav1beta1.JobState
		message   string
		eventType string
	)

	err := js.Error
	if err != nil {
		state = cronhpav1beta1.Failed
		message = fmt.Sprintf("cron hpa failed to execute, because of %v", err)
		eventType = corev1.EventTypeWarning
	} else {
		state = cronhpav1beta1.Succeed
		message = fmt.Sprintf("cron hpa job %s executed successfully. %s", job.name, js.Msg)
		eventType = corev1.EventTypeNormal
	}

	condition := cronhpav1beta1.Condition{
		Name:          job.Name(),
		JobId:         job.ID(),
		RunOnce:       job.RunOnce(),
		Schedule:      job.SchedulePlan(),
		TargetSize:    job.DesiredSize(),
		LastProbeTime: metav1.Time{Time: time.Now()},
		State:         state,
		Message:       message,
	}
	IncExecuteJob(cronHpa, condition)

	conditions := cronHpa.Status.Conditions
	var found = false
	for index, c := range conditions {
		if c.JobId == job.ID() || c.Name == job.Name() {
			found = true
			cronHpa.Status.Conditions[index] = condition
		}
	}
	if !found {
		cronHpa.Status.Conditions = append(cronHpa.Status.Conditions, condition)
	}

	err = je.UpdateCronHPAStatusWithRetry(cronHpa, deepCopy, job.name)
	if err != nil {
		if _, ok := err.(*NoNeedUpdate); ok {
			log.Warning("No need to update cronHPA, because it is deleted before")
			return
		}
		je.EventRecorder().Event(cronHpa, corev1.EventTypeWarning, "Failed", fmt.Sprintf("Failed to update cronhpa status: %v", err))
	} else {
		je.EventRecorder().Event(cronHpa, eventType, string(state), message)
	}
}

func (je *BaseCronJobExecutor) ScaleHPA(job CronJob) (msg string, err error) {
	ref := job.Ref()

	ctx := context.Background()
	hpa := &autoscaling.HorizontalPodAutoscaler{}
	if err = je.client.Get(ctx, types.NamespacedName{Namespace: ref.RefNamespace, Name: ref.RefName}, hpa); err != nil {
		return "", err
	}

	updateHPA, desiredSize := false, job.DesiredSize()

	if desiredSize > hpa.Spec.MaxReplicas {
		hpa.Spec.MaxReplicas = desiredSize
		updateHPA = true
	}

	if desiredSize < *hpa.Spec.MinReplicas {
		*hpa.Spec.MinReplicas = desiredSize
		updateHPA = true
	}

	if hpa.Status.CurrentReplicas < desiredSize {
		*hpa.Spec.MinReplicas = desiredSize
		updateHPA = true
	}

	if updateHPA {
		err = je.client.Update(ctx, hpa)
		if err != nil {
			return "", err
		}
	}
	msg = fmt.Sprintf("current replicas:%d, desired replicas:%d.", hpa.Status.CurrentReplicas, desiredSize)
	return msg, nil
}

func (je *BaseCronJobExecutor) ScalePlainRef(job CronJob) (msg string, err error) {
	targetRef := job.Ref()

	var scale *autoscaling.Scale
	var targetGR schema.GroupResource

	targetGK := schema.GroupKind{
		Group: targetRef.RefGroup,
		Kind:  targetRef.RefKind,
	}
	mappings, err := je.mapper.RESTMappings(targetGK)
	if err != nil {
		return "", fmt.Errorf("failed to create create mapping,because of %v", err)
	}

	found := false
	for _, mapping := range mappings {
		targetGR = mapping.Resource.GroupResource()
		scale, err = je.scaler.Scales(targetRef.RefNamespace).Get(context.Background(), targetGR, targetRef.RefName, metav1.GetOptions{})
		if err == nil {
			found = true
			log.Infof("%s %s in namespace %s has been scaled successfully. job: %s replicas: %d id: %s", targetRef.RefKind,
				targetRef.RefName, targetRef.RefNamespace, job.Name(), job.DesiredSize(), job.ID())
			break
		}
	}

	if found == false {
		log.Errorf("failed to find source target %s %s in %s namespace", targetRef.RefKind, targetRef.RefName, targetRef.RefNamespace)
		return "", fmt.Errorf("failed to find source target %s %s in %s namespace", targetRef.RefKind, targetRef.RefName, targetRef.RefNamespace)
	}

	msg = fmt.Sprintf("current replicas:%d, desired replicas:%d.", scale.Spec.Replicas, job.DesiredSize())

	scale.Spec.Replicas = job.DesiredSize()
	_, err = je.scaler.Scales(targetRef.RefNamespace).Update(context.Background(), targetGR, scale, metav1.UpdateOptions{})
	return msg, err
}

func (je *BaseCronJobExecutor) UpdateCronHPAStatusWithRetry(instance *cronhpav1beta1.CronHorizontalPodAutoscaler,
	deepCopy *cronhpav1beta1.CronHorizontalPodAutoscaler, jobName string) error {
	var err error
	if instance == nil {
		log.Warning("Failed to patch cronHPA, because instance is deleted")
		return &NoNeedUpdate{}
	}
	for i := 1; i <= MaxRetryTimes; i++ {
		// leave ResourceVersion = empty
		err = je.client.Patch(context.Background(), instance, client.MergeFrom(deepCopy))
		if err != nil {
			if k8serrors.IsNotFound(err) {
				log.Error("Failed to patch cronHPA, because instance is deleted")
				return &NoNeedUpdate{}
			}
			log.Errorf("Failed to patch cronHPA %v, because of %v", instance, err)
			continue
		}
		break
	}
	if err != nil {
		log.Errorf("Failed to update cronHPA job %s of cronHPA %s in %s after %d times, because of %v", jobName, instance.Name, instance.Namespace, MaxRetryTimes, err)
	}
	return err
}

func (je *BaseCronJobExecutor) Client() client.Client {
	return je.client
}

func (je *BaseCronJobExecutor) EventRecorder() record.EventRecorder {
	return je.eventRecorder
}

func NewCronJobExecutor(mgr manager.Manager) CronJobExecutor {
	cfg := mgr.GetConfig()
	hpaClient := clientset.NewForConfigOrDie(cfg)
	discoveryClient := clientset.NewForConfigOrDie(cfg)
	resources, err := restmapper.GetAPIGroupResources(discoveryClient)
	if err != nil {
		log.Fatalf("Failed to get api resources, because of %v", err)
	}
	restMapper := restmapper.NewDiscoveryRESTMapper(resources)
	// change the rest mapper to discovery resources
	scaleKindResolver := scaleclient.NewDiscoveryScaleKindResolver(hpaClient.Discovery())
	scaleClient, err := scaleclient.NewForConfig(cfg, restMapper, dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)

	if err != nil {
		log.Fatalf("Failed to create scaler client,because of %v", err)
	}

	return &BaseCronJobExecutor{
		scaler:        scaleClient,
		mapper:        restMapper,
		client:        mgr.GetClient(),
		eventRecorder: mgr.GetEventRecorderFor("CronHorizontalPodAutoscaler"),
	}
}
