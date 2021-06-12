package otstool

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type People struct {
	Name string `ots:"name"`
	Age  int    `ots:"age"`
}

type Student struct {
	*People
	No string `ots:"no"`
}

func TestScanRow(t *testing.T) {
	rows := Rows{
		len:    2,
		cursor: 0,
		rows: []Row{
			{
				PrimaryKeys: nil,
				Columns: []*tablestore.AttributeColumn{
					{ColumnName: "name", Value: "yuzj"},
					{ColumnName: "age", Value: 28},
					{ColumnName: "no", Value: "1"},
				},
			}, {
				PrimaryKeys: nil,
				Columns: []*tablestore.AttributeColumn{
					{ColumnName: "name", Value: "zhouhan"},
					{ColumnName: "age", Value: 23},
					{ColumnName: "no", Value: "2"},
				},
			},
		},
	}

	var obj []Student
	err := ScanRows(&rows, &obj)
	if err != nil {
		t.Error(err)
	}

	// t.Log(obj.People, obj.No)
	// for i := range obj {
	// 	t.Log(obj[i].People, obj[i].No)
	// }
}
