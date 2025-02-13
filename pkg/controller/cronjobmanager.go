package controller

import (
	"github.com/ringtail/go-cron"
	log "k8s.io/klog/v2"
	"sync"
	"time"
)

type JobStatus string

const (
	JobNormal   = JobStatus("JobNormal")
	JobTimeOut  = JobStatus("JobTimeOut")
	JobNotFound = JobStatus("JobNotFound")
)

const maxOutOfDateTimeout = 5 * time.Minute

type CronJobManager interface {
	Start()
	Stop()
	Add(job CronJob) error
	Remove(job CronJob)
	RemoveAllByCronHpa(cronHpaName string)
	Find(job CronJob) (bool, JobStatus)
	ListEntries() []*cron.Entry
	ListJobsByCronHpa(cronHpaName string) map[string]CronJob
}

// BaseCronJobManager is mainly to add, delete and find jobs
type BaseCronJobManager struct {
	sync.Mutex
	Engine *cron.Cron
	jobs   map[string]map[string]CronJob
}

func (jm *BaseCronJobManager) Start() {
	jm.Engine.Start()
}

func (jm *BaseCronJobManager) Stop() {
	jm.Engine.Stop()
}

func (jm *BaseCronJobManager) Add(job CronJob) error {
	jm.Lock()
	defer jm.Unlock()

	err := jm.Engine.AddJob(job.SchedulePlan(), job)
	if err != nil {
		log.Errorf("Failed to add job to engine,because of %v", err)
	}
	AddJobs(job.CronHPA().Name, 1)

	if _, ok := jm.jobs[job.CronHPA().Name]; !ok {
		jm.jobs[job.CronHPA().Name] = make(map[string]CronJob)
	}
	jm.jobs[job.CronHPA().Name][job.ID()] = job

	return err
}

func (jm *BaseCronJobManager) Remove(job CronJob) {
	jm.Lock()
	defer jm.Unlock()

	jm.Engine.RemoveJob(job.ID())
	AddJobs(job.CronHPA().Name, -1)

	delete(jm.jobs[job.CronHPA().Name], job.ID())
	if len(jm.jobs[job.CronHPA().Name]) == 0 {
		delete(jm.jobs, job.CronHPA().Name)
	}

	return
}

func (jm *BaseCronJobManager) RemoveAllByCronHpa(cronHpaName string) {
	jm.Lock()
	defer jm.Unlock()

	if _, ok := jm.jobs[cronHpaName]; !ok {
		return
	}

	for _, job := range jm.jobs[cronHpaName] {
		jm.Engine.RemoveJob(job.ID())
		AddJobs(cronHpaName, -1)
	}
	delete(jm.jobs, cronHpaName)

	return
}

func (jm *BaseCronJobManager) Find(job CronJob) (bool, JobStatus) {
	entries := jm.Engine.Entries()
	for _, e := range entries {
		if e.Job.ID() == job.ID() {
			// clean up out of date jobs when it reached maxOutOfDateTimeout
			if e.Next.Add(maxOutOfDateTimeout).After(time.Now()) {
				return true, JobNormal
			}
			log.Warningf("The job %s(job id %s) in cronhpa %s namespace %s is out of date.", job.Name(), job.ID(), job.CronHPA().Name, job.CronHPA().Namespace)
			return true, JobTimeOut
		}
	}
	return false, JobNotFound
}

func (jm *BaseCronJobManager) ListEntries() []*cron.Entry {
	return jm.Engine.Entries()
}

func (jm *BaseCronJobManager) ListJobsByCronHpa(cronHpaName string) map[string]CronJob {
	return jm.jobs[cronHpaName]
}

func NewCronJobManager(timezone *time.Location, handler func(job *cron.JobResult)) CronJobManager {
	if timezone == nil {
		timezone = time.Now().Location()
	}
	jm := &BaseCronJobManager{
		Engine: cron.NewWithLocation(timezone),
		jobs:   make(map[string]map[string]CronJob),
	}
	jm.Engine.AddResultHandler(handler)
	return jm
}
