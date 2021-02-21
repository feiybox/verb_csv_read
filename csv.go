package main

import (
	"encoding/csv"
	"errors"
	"io"
	"sync"
)

var (
	NotAllSuccess = errors.New("not all success")
)

type RowResult struct {
	Row []string
	Err error
}

// 处理csv
// 输入
//  reader: 读取文件
//  conNum: 处理文件行，并发粒度
//  doRow: 处理每行函数，返回error
// 输出
//  错误行信息
//  error信息
func ReadCSV(reader *csv.Reader, conNum int, doRow func(*RowResult) error) ([]*RowResult, error) {
	rowChan := make(chan *RowResult, conNum)
	faiChan := make(chan *RowResult, conNum)
	go func() {
		wg := new(sync.WaitGroup)
		wg.Add(conNum)
		for i := 0; i < conNum; i++ {
			go func() {
				defer wg.Done()
				for true {
					row, ok := <-rowChan
					if !ok {
						break
					}
					// 处理读取错误
					if row.Err != nil {
						faiChan <- row
						continue
					}
					// 处理数据
					err := doRow(row)
					if err != nil {
						row.Err = err
						faiChan <- row
					}
				}
			}()
		}
		wg.Wait()
		close(faiChan)
	}()

	go func() {
		for true {
			record, err := reader.Read()
			if err != nil && err == io.EOF {
				break
			}
			rowChan <- &RowResult{
				Row: record,
				Err: err,
			}
		}
		close(rowChan)
	}()

	faiRes := make([]*RowResult, 0, conNum)
	for true {
		fail, ok := <-faiChan
		if !ok {
			break
		}
		if fail != nil {
			faiRes = append(faiRes, fail)
		}
	}
	var err error
	if len(faiRes) > 0 {
		err = NotAllSuccess
	}

	return faiRes, err
}
