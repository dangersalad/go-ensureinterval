package ensureinterval // import "github.com/dangersalad/go-ensureinterval"

import (
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// Job is a job to run at a specified interval
type Job struct {
	Name      string
	Exec      ExecFunc
	Frequency time.Duration
}

// JobLoader is a function to load in jobs before run
type JobLoader func() ([]*Job, error)

// ExecFunc is a function to be run at intervals by Run
type ExecFunc func() error

// the default max catchups to allow
var maxKetchups = 20

// SetMaxCatchup sets the max intervals this is allowed to try to
// catch up. The default is 20.
func SetMaxCatchup(max int) {
	maxKetchups = max
}

// Run will run the Jobs provided at the specified interval,
// catching up missed interval runs if the time to execute the
// provided Jobs takes longer than the interval provided.
//
// If the catchup attempts reach the value set by SetMaxInterval
// (default 20) then it will return en error.
//
// An error will also be returned if the exec function returns an
// error. In this case you will need to restart the runner manually.
func Run(interval time.Duration, getJobs JobLoader) error {
	for {
		now := time.Now().Truncate(interval)
		jobs, err := getJobs()
		if err != nil {
			return errors.Wrap(err, "getting jobs")
		}
		if err := processJobs(now, interval, jobs); err != nil {
			return err
		}
		lastElapsed := time.Now().Sub(now)
		for elapsed, totalInterval := lastElapsed, interval; elapsed > totalInterval; elapsed, totalInterval = elapsed+lastElapsed, totalInterval+interval {
			nowKetchup := now.Add(totalInterval)
			if err := processJobs(nowKetchup, interval, jobs); err != nil {
				return err
			}
			lastElapsed = time.Now().Sub(nowKetchup)
			if totalInterval > interval*time.Duration(maxKetchups) {
				return &errMaxCatchups{}
			}
		}
		sleepTime := interval - lastElapsed
		time.Sleep(sleepTime)
	}
}

func runJob(job *Job, interval time.Duration, complete chan error) {
	debugf("running job %s (%s)", job.Name, job.Frequency*interval)
	err := job.Exec()
	if err != nil {
		// signal complete with error
		complete <- errors.Wrapf(err, "executing job %s", job.Name)
	}
	// signal complete without error
	complete <- nil
}

func processJobs(now time.Time, interval time.Duration, jobs []*Job) error {
	completes := []chan error{}
	jCount := 0
	for _, j := range jobs {
		if now.Truncate(j.Frequency*interval) == now {
			complete := make(chan error)
			completes = append(completes, complete)
			jCount++
			go runJob(j, interval, complete)
		}
	}
	debugf("started %d jobs", jCount)
	errs := []error{}
	for _, c := range completes {
		err := <-c
		if err != nil {
			logf("%+v", err)
			errs = append(errs, err)
		}
		debug("job finished")
	}

	// if none of the jobs returned an errors, just proceed
	if len(errs) == 0 {
		return nil
	}

	errStr := ""
	for _, err := range errs {
		errStr = fmt.Sprintf("%s, %s", errStr, err)
	}
	return errors.New(errStr)
}

type errMaxCatchups struct {
}

func (e *errMaxCatchups) Error() string {
	return "max catchups reached"
}

func (e *errMaxCatchups) String() string {
	return e.Error()
}

func (e *errMaxCatchups) Temporary() bool {
	return true
}
