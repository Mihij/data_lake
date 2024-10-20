package datamanager

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

type DatabaseManager interface {
	UpdateCurrentDatabase(ctx context.Context) error
	CreateNewDatabase(ctx context.Context) error
}

type databaseManager struct {
	deltaRepo DeltaLakeRepository
}

func NewDatabaseManager(repo DeltaLakeRepository) *databaseManager {
	return &databaseManager{repo: repo}
}

func (m *databaseManager) UpdateCurrentDatabase(ctx context.Context) error {
	// Логика обновления текущей базы
	// Например, загрузка новых данных из источника
	newRecords := generateNewRecords()
	err := m.repo.addAllToCurrentDatabase(ctx, newRecords)
	if err != nil {
		return fmt.Errorf("failed to add new records to current database: %w", err)
	}
	return nil
}

func (m *databaseManager) CreateNewDatabase(ctx context.Context) error {
	// Создание новой базы
	records, err := m.repo.GetAllRecordsFromCurrentDatabase(ctx)
	if err != nil {
		return fmt.Errorf("failed to get records from current database: %w", err)
	}
	err = m.repo.saveAllToNewDatabase(ctx, records)
	if err != nil {
		return fmt.Errorf("failed to save records to new database: %w", err)
	}
	return nil
}

func generateNewRecords() []Record {
	// Здесь должна быть логика генерации новых записей
	return []Record{}
}

func (m *databaseManager) addAllToCurrentDatabase(ctx context.Context, records []Record) error {
	for _, record := range records {
		fileName := fmt.Sprintf("%d.json", record.ID)
		filePath := filepath.Join(m.repo.currentPath, fileName)
		data, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal record to JSON: %w", err)
		}
		err = ioutil.WriteFile(filePath, data, 0644)
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
	}
	return nil
}

func (m *databaseManager) saveAllToNewDatabase(ctx context.Context, records []Record) error {
	for _, record := range records {
		fileName := fmt.Sprintf("%d.json", record.ID)
		filePath := filepath.Join(m.repo.newPath, fileName)
		data, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal record to JSON: %w", err)
		}
		err = ioutil.WriteFile(filePath, data, 0644)
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
	}
	return nil
}
