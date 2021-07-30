package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrNotfound = errors.New("key not found in store")
)

// Store wrap for bbolt
type Store struct {
	db *bolt.DB
}

// NewStore returns new store
func NewStore(dbName string) *Store {
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &Store{db: db}
}

// Close store
func (s *Store) Close() error {
	return s.db.Close()
}

// CreateBucketIfNotExist create bucket if not exist
func (s *Store) CreateBucketIfNotExist(bucket []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			panic(err)
		}
		return err
	})
}

// Incr increase a number
func (s *Store) Incr(bucket, key []byte) (n uint64, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		data := make([]byte, 8)
		b := tx.Bucket(bucket)
		if old := b.Get(key); old != nil {
			n = binary.BigEndian.Uint64(old)
		}
		// if n is max, keep it
		if n == math.MaxUint64 {
			return nil
		}
		n += 1
		binary.BigEndian.PutUint64(data, n)
		return b.Put(key, data)
	})
	return
}

// Save key and val to bucket
func (s *Store) Save(bucket, key, val []byte) (err error) {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.Put(key, val)
	})
}

// Get val by key from bucket
func (s *Store) Get(bucket, key []byte) (val []byte, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		val = b.Get(key)
		if val == nil {
			return ErrNotfound
		}
		return nil
	})
	return
}

func (s *Store) Delete(bucket, key []byte) (err error) {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.Delete(key)
	})
}

// Scan for bucket
func (s *Store) Scan(bucket []byte, next func(key, val []byte) bool) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.ForEach(func(k, v []byte) error {
			if !next(k, v) {
				return io.EOF
			}
			return nil
		})
	})
}

// FindPrefix find val by prefix from bucket
func (s *Store) FindPrefix(bucket, prefix []byte, next func(key, val []byte) bool) error {
	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if !next(k, v) {
				return io.EOF
			}
		}
		return nil
	})
}

// FindBetween find val between start and end from bucket
func (s *Store) FindBetween(bucket, start, end []byte, next func(key, val []byte) bool) error {
	return s.db.View(func(tx *bolt.Tx) error {
		if bytes.Compare(start, end) > 0 {
			start, end = end, start
		}

		c := tx.Bucket(bucket).Cursor()
		for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) <= 0; k, v = c.Next() {
			if !next(k, v) {
				return io.EOF
			}
		}
		return nil
	})
}
