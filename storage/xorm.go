package storage

import (
	"github.com/go-xorm/xorm"
	"log"
)

// XormDBConfig holds configuration for connecting to the database
type XormDBConfig struct {
	Engine    string
	DbHostURL string
}

type xormStorage struct {
	config XormDBConfig
	e      *xorm.Engine
}

// NewXormStorage creates new instance of the xorm Adapter
func NewXormStorage(config XormDBConfig) (TaskStore, error) {
	// try to connect to given DB.
	e, err := xorm.NewEngine(config.Engine, config.DbHostURL)
	if err != nil {
		log.Printf("Unable to connect to DB %q: %v", config.DbHostURL, err)
		return nil, err
	}
	// lets initialize the DB as needed.
	if err := e.Sync(new(TaskAttributes)); err != nil {
		log.Printf("Couldn't initialize the DB: %v", err)
		return nil, err
	}
	return &xormStorage{config: config, e: e}, nil
}

// Close closes the connection to the TaskStore
func (db *xormStorage) Close() error {
	return db.e.Close()
}

// Add adds a task to the TaskStore
func (db *xormStorage) Add(task TaskAttributes) error {
	session := db.e.NewSession()
	defer session.Close()
	_, err := session.Insert(task)
	return err
}

// Fetch fetches all tasks from the TaskStore
func (db *xormStorage) Fetch() ([]TaskAttributes, error) {
	var tasks []TaskAttributes
	if err := db.e.Find(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Remove removes a task from the TaskStore
func (db *xormStorage) Remove(task TaskAttributes) error {
	_, err := db.e.Delete(task)
	return err
}
