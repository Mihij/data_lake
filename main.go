package main

import (
	"fmt"
	"log"

	"github.com/robfig/cron"
)

type DeltaLakeUpdater struct{}

func (u *DeltaLakeUpdater) updateCurrentDatabase() error {
	log.Println("Updating current database...")
	return nil
}
func (u *DeltaLakeUpdater) createNewDatabase() error {
	log.Println("Creating new database...")
	return nil
}

func main() {
	updater := &DeltaLakeUpdater{}
	c := cron.New()

	// Обновление базы данных каждые 24 часа
	c.AddFunc("@daily", func() { updater.updateCurrentDatabase() })

	// Создание новой базы данных ежедневно в полночь
	c.AddFunc("0 0 * * *", func() { updater.createNewDatabase() })

	c.Start()

	fmt.Println("Delta Lake Updater started")
}
