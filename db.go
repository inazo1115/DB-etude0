package etude0

import (
	"os"
)

type DB struct {
	idx *Index
	//stats *Stats
}

func (db *DB) Open(fname string) error {

	idxFile, err := os.OpenFile(fname+".idx", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	datFile, err := os.OpenFile(fname+".dat", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		idxFile.Close() // Ignore this error
		return err
	}

	db.idx, err = NewIndex(idxFile, datFile)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Close() error {
	if err := db.idx.Close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Get(key string) (string, error) {
	value, err := db.idx.Get(key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *DB) Put(key string, value string) error {
	if err := db.idx.Put(key, value); err != nil {
		return err
	}
	return nil
}

func (db *DB) Optimize() error {
	return nil
}

func (db *DB) Dump() {
}
