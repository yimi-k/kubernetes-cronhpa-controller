package controller

import (
	"errors"
	"fmt"
	"github.com/AliyunContainerService/kubernetes-cronhpa-controller/pkg/apis/autoscaling/v1beta1"
	"github.com/ringtail/go-cron"
	log "k8s.io/klog/v2"
	"reflect"
	"strings"
	"time"
)

const dateFormat = "01-02-2006"

type CronJob interface {
	ID() string
	Name() string
	SchedulePlan() string
	Ref() *TargetRef
	CronHPA() *v1beta1.CronHorizontalPodAutoscaler
	DesiredSize() int32
	ExcludeDates() []string
	RunOnce() bool
	Equals(Job CronJob) bool
	Run() (msg string, err error)
}

type TargetRef struct {
	RefName      string
	RefNamespace string
	RefKind      string
	RefGroup     string
	RefVersion   string
}

type BaseCronJob struct {
	id           string
	name         string
	plan         string
	desiredSize  int32
	targetRef    *TargetRef
	cronHPA      *v1beta1.CronHorizontalPodAutoscaler
	excludeDates []string
	runOnce      bool
}

func (ch *BaseCronJob) Run() (msg string, err error) {
	return cronJobExecutor.Run(ch)
}

func (ch *BaseCronJob) ID() string {
	return ch.id
}

func (ch *BaseCronJob) SetID(id string) {
	ch.id = id
}

func (ch *BaseCronJob) Name() string {
	return ch.name
}

func (ch *BaseCronJob) SchedulePlan() string {
	return ch.plan
}

func (ch *BaseCronJob) Ref() *TargetRef {
	return ch.targetRef
}

func (ch *BaseCronJob) CronHPA() *v1beta1.CronHorizontalPodAutoscaler {
	return ch.cronHPA
}

func (ch *BaseCronJob) DesiredSize() int32 {
	return ch.desiredSize
}

func (ch *BaseCronJob) ExcludeDates() []string {
	return ch.excludeDates
}

func (ch *BaseCronJob) RunOnce() bool {
	return ch.runOnce
}

func (ch *BaseCronJob) Equals(j CronJob) bool {
	if ch.id != j.ID() || ch.Name() != j.Name() || ch.SchedulePlan() != j.SchedulePlan() || ch.DesiredSize() != j.DesiredSize() ||
		ch.RunOnce() != j.RunOnce() {
		return false
	}

	if !reflect.DeepEqual(ch.Ref(), j.Ref()) {
		return false
	}

	if ch.CronHPA().UID != j.CronHPA().UID {
		return false
	}

	if !reflect.DeepEqual(ch.ExcludeDates(), j.ExcludeDates()) {
		return false
	}

	return false
}

func NewCronJob(cronHpa *v1beta1.CronHorizontalPodAutoscaler, job v1beta1.Job) (*BaseCronJob, error) {
	arr := strings.Split(cronHpa.Spec.ScaleTargetRef.ApiVersion, "/")
	group := arr[0]
	version := arr[1]
	ref := &TargetRef{
		RefName:      cronHpa.Spec.ScaleTargetRef.Name,
		RefKind:      cronHpa.Spec.ScaleTargetRef.Kind,
		RefNamespace: cronHpa.Namespace,
		RefGroup:     group,
		RefVersion:   version,
	}

	if err := checkRefValid(ref); err != nil {
		return nil, err
	}
	if err := checkPlanValid(job.Schedule); err != nil {
		return nil, err
	}

	return &BaseCronJob{
		id:           GenerateJobId(cronHpa, job),
		name:         job.Name,
		plan:         job.Schedule,
		desiredSize:  job.TargetSize,
		targetRef:    ref,
		cronHPA:      cronHpa,
		runOnce:      job.RunOnce,
		excludeDates: cronHpa.Spec.ExcludeDates,
	}, nil
}

func CronJobResultHandler(js *cron.JobResult) {
	cronJobExecutor.HandleJobResult(js)
}

func checkRefValid(ref *TargetRef) error {
	if ref.RefVersion == "" || ref.RefGroup == "" || ref.RefName == "" || ref.RefNamespace == "" || ref.RefKind == "" {
		return errors.New("any properties in ref could not be empty")
	}
	return nil
}

func checkPlanValid(plan string) error {
	_, err := cron.Parse(plan)
	return err
}

func IsTodayOff(excludeDates []string) (bool, string) {

	if excludeDates == nil {
		return false, ""
	}

	now := time.Now()
	for _, date := range excludeDates {
		schedule, err := cron.Parse(date)
		if err != nil {
			log.Warningf("Failed to parse schedule %s,and skip this date,because of %v", date, err)
			continue
		}
		if nextTime := schedule.Next(now); nextTime.Format(dateFormat) == now.Format(dateFormat) {
			return true, fmt.Sprintf("skip scaling activity,because of excludeDate (%s).", date)
		}
	}
	return false, ""
}
