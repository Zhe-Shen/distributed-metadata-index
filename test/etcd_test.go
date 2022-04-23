package test

import (
	dmi "distributed-metadata-index/pkg"
	"testing"
)

func TestDeleteAll(t *testing.T) {
	err := dmi.PutIndex("cpu", []byte{1, 2, 3})
	if err != nil {
		t.Errorf(err.Error())
	}
	err = dmi.PutIndex("gpu", []byte{1, 2, 3})
	if err != nil {
		t.Errorf(err.Error())
	}
	err = dmi.DeleteAll()
	if err != nil {
		t.Errorf(err.Error())
	}
	resp, err := dmi.GetIndex("gpu")
	if err != nil {
		t.Errorf(err.Error())
	}
	if resp != nil {
		t.Errorf("Should not find gpu")
	}
}
