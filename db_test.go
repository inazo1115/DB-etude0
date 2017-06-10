package etude0

import (
	"fmt"
	"testing"
)

func TestGetAndPut(t *testing.T) {

	db := &DB{}
	db.Open("./tmp/foo")

	err := db.Put("aaa", "foo")
	if err != nil {
		t.Errorf(err.Error())
	}

	err = db.Put("bbb", "bar")
	if err != nil {
		t.Errorf(err.Error())
	}

	s, err := db.Get("aaa")
	if err != nil {
		t.Errorf(err.Error())
	}
	if s != "foo" {
		t.Errorf("Get(\"aaa\") = \"%s\", want \"foo\"", s)
	}

	s, err = db.Get("bbb")
	if err != nil {
		t.Errorf(err.Error())
	}
	if s != "bar" {
		t.Errorf("Get(\"bbb\") = \"%s\", want \"bar\"", s)
	}
}

func TestMoreGetAndPut(t *testing.T) {

	db := &DB{}
	db.Open("./tmp/bar")

	n := 10000

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		value := fmt.Sprintf("%010d", i)
		db.Put(key, value)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		value := fmt.Sprintf("%010d", i)
		s, err := db.Get(key)
		if err != nil {
			t.Errorf(err.Error())
		}
		if s != value {
			t.Errorf("Get(\"%s\") = \"%s\", want \"%s\"", key, s, value)
		}
	}
}
