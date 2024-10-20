package repository

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type DeltaLakeRepository interface {
	GetAllRecordsFromCurrentDatabase(ctx context.Context) ([]Record, error)
	GetAllRecordsFromNewDatabase(ctx context.Context) ([]Record, error)
}

type deltaLakeRepository struct {
	currentPath string
	newPath     string
}

func NewDeltaLakeRepository(currentPath, newPath string) (*deltaLakeRepository, error) {
	if err := os.MkdirAll(currentPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create current directory")
	}
	if err := os.MkdirAll(newPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create new directory")
	}
	return &deltaLakeRepository{
		currentPath: currentPath,
		newPath:     newPath,
	}, nil
}

func (r *deltaLakeRepository) GetAllRecordsFromCurrentDatabase(ctx context.Context) ([]Record, error) {
	files, err := ioutil.ReadDir(r.currentPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read current directory")
	}

	var records []Record
	for _, file := range files {
		path := filepath.Join(r.currentPath, file.Name())
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read file")
		}
		var record Record
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal JSON")
		}
		records = append(records, record)
	}
	return records, nil
}

func (r *deltaLakeRepository) GetAllRecordsFromNewDatabase(ctx context.Context) ([]Record, error) {
	files, err := ioutil.ReadDir(r.newPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read new directory")
	}

	var records []Record
	for _, file := range files {
		path := filepath.Join(r.newPath, file.Name())
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read file")
		}
		var record Record
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal JSON")
		}
		records = append(records, record)
	}
	return records, nil
}
