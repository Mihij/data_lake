package config

import (
	"context"
	"log"
	"time"

	"github.com/go-kit/kit/scheduler"
	"github.com/go-kit/kit/scheduler/job"
)

func main() {
	ctx := context.Background()

	repo, err := NewDeltaLakeRepository("/path/to/current/db", "/path/to/new/db")
	if err != nil {
		log.Fatalf("Failed to initialize Delta Lake repository: %v", err)
	}

	dbManager := NewDatabaseManager(repo)

	updater := &DeltaLakeUpdater{
		updateCurrentDatabase: dbManager.UpdateCurrentDatabase,
		createNewDatabase:     dbManager.CreateNewDatabase,
	}

	sched := scheduler.NewScheduler(job.NewJobStore())

	sched.Schedule(updater.updateCurrentDatabase, job.RepeatEvery(time.Hour))
	sched.Schedule(updater.createNewDatabase, job.Cron("0 0 0 * * *")) // Daily at midnight

	sched.Run()
}
