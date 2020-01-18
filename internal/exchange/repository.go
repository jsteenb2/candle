package exchange

import (
	"strconv"
	"time"

	"github.com/etcd-io/bbolt"
)

type LastSeenRepo struct {
	db *bbolt.DB
}

func NewLastSeenRepo(db *bbolt.DB) (*LastSeenRepo, error) {
	l := &LastSeenRepo{db: db}

	if err := l.init(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *LastSeenRepo) GetLast(key string) time.Time {
	var last time.Time
	l.db.View(func(tx *bbolt.Tx) error {
		val := tx.
			Bucket(l.bucket()).
			Get([]byte(key))
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err == nil {
			last = time.Unix(0, i)
		}
		return err
	})
	return last
}

func (l *LastSeenRepo) PutLast(key string, val time.Time) error {
	return l.db.Update(func(tx *bbolt.Tx) error {
		return tx.
			Bucket(l.bucket()).
			Put([]byte(key), []byte(strconv.FormatInt(val.UnixNano(), 10)))
	})
}

func (l *LastSeenRepo) bucket() []byte {
	return []byte("last_seen")
}

func (l *LastSeenRepo) init() error {
	return l.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(l.bucket())
		return err
	})
}
