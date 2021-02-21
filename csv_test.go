package main

import (
	"encoding/csv"
	"os"
	"testing"
	"time"
)

func TestReadCSV(t *testing.T) {
	file, err := os.Open("test.csv")
	if err != nil {
		t.Errorf("open file fail, err=%s", err)
		return
	}
	r := csv.NewReader(file)

	start := time.Now()
	println("start:", start.Unix())
	res, err := ReadCSV(r, 1, func(rows *RowResult) error {
		time.Sleep(time.Millisecond * 1)
		return nil
	})
	if err != nil {
		t.Errorf("ReadCSV fail, err=%s", err)
		t.Logf("res cnt=%d", len(res))
	}
	end := time.Now()
	println("end:", end.Unix(), " duration:", int64(end.Sub(start).Seconds()))
}
