package redifu

import "testing"

func TestJoinParam(t *testing.T) {
	key := "joinparams"
	joinParam(key, nil)
}

func TestSetFirstPage(t *testing.T) {
	type Car struct {
		*MongoItem
		car string `json:"car" bson:"car"`
	}
	key := "voucher"
	paginate := NewTimeline[Car](nil, nil, key, 10, Descending)
	paginate.SetFirstPage(nil)
}
