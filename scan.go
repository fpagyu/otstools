package ots

import (
	"errors"
	"reflect"
)

func indirect(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

func scanRowToMap(pks PrimaryKeyCols, cols AttributeCols, elem reflect.Value, _ map[string]string) error {
	for _, col := range pks {
		elem.SetMapIndex(reflect.ValueOf(col.ColumnName),
			reflect.ValueOf(col.Value))
	}

	for _, col := range cols {
		elem.SetMapIndex(reflect.ValueOf(col.ColumnName),
			reflect.ValueOf(col.Value))
	}
	return nil
}

func parseTags(elem reflect.Value, keyMap map[string]string) {
	t := elem.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		switch f.Type.Kind() {
		case reflect.Struct:
			parseTags(elem.Field(i), keyMap)
		default:
			if tag := f.Tag.Get("ots"); tag != "" {
				keyMap[tag] = f.Name
			}
		}
	}
}

func scanRowToStruct(pks PrimaryKeyCols, cols AttributeCols, elem reflect.Value, keyMap map[string]string) error {
	if len(keyMap) == 0 {
		parseTags(elem, keyMap)
	}

	if len(keyMap) == 0 {
		return errors.New("not set struct tag `ots:\"?\"`")
	}

	for _, col := range pks {
		if key, has := keyMap[col.ColumnName]; has {
			elem.FieldByName(key).Set(reflect.ValueOf(col.Value))
		}
	}

	for _, col := range cols {
		if key, has := keyMap[col.ColumnName]; has {
			elem.FieldByName(key).Set(reflect.ValueOf(col.Value))
		}
	}

	return nil
}

func scanRow(rows IRow, results reflect.Value) error {
	var isMap bool
	kind := results.Kind()
	switch kind {
	case reflect.Map:
		isMap = true
	case reflect.Struct:
	default:
		return errors.New("unsupported destination, should be slice or struct")
	}

	rows.Reset()
	pks, cols, ok := rows.Next()
	if !ok {
		return errors.New("empty rows")
	}

	if isMap {
		return scanRowToMap(pks, cols, results, nil)
	} else {
		return scanRowToStruct(pks, cols, results, make(map[string]string))
	}
}

func scanRows(rows IRow, results reflect.Value) (err error) {
	var isPtr bool
	var resultType reflect.Type

	resultType = results.Type().Elem()
	results.Set(reflect.MakeSlice(results.Type(), 0, rows.Len()))
	if resultType.Kind() == reflect.Ptr {
		isPtr = true
		resultType = resultType.Elem()
	}

	var newfc func(reflect.Type) reflect.Value
	var scanfc func(PrimaryKeyCols, AttributeCols, reflect.Value, map[string]string) error
	var structFieldsMap map[string]string
	isMap := resultType.Kind() == reflect.Map
	if isMap {
		newfc = reflect.MakeMap
		scanfc = scanRowToMap
		structFieldsMap = nil
	} else {
		newfc = func(t reflect.Type) reflect.Value { return reflect.New(t).Elem() }
		scanfc = scanRowToStruct
		structFieldsMap = make(map[string]string)
	}

	rows.Reset()
	loopcnt := rows.Len()
	for i := 0; i < loopcnt; i++ {
		pks, cols, ok := rows.Next()
		if !ok {
			continue
		}

		elem := results
		elem = newfc(resultType)

		err = scanfc(pks, cols, elem, structFieldsMap)
		if err != nil {
			break
		}
		if isPtr {
			results.Set(reflect.Append(results, elem.Addr()))
		} else {
			results.Set(reflect.Append(results, elem))
		}
	}

	return
}

// ScanRow: v是一个Map或Struct
func ScanRow(rows IRow, v interface{}) error {
	if rows.Len() == 0 {
		return nil
	}

	results := indirect(reflect.ValueOf(v))

	return scanRow(rows, results)
}

// ScanRows: v是一个Slice
func ScanRows(rows IRow, v interface{}) (err error) {
	if rows.Len() == 0 {
		return nil
	}

	results := indirect(reflect.ValueOf(v))

	if kind := results.Kind(); kind != reflect.Slice {
		err = errors.New("must be slice")
		return
	}

	return scanRows(rows, results)
}

func Unmarshal(rows IRow, v interface{}) (err error) {
	if rows.Len() == 0 {
		return nil
	}

	results := indirect(reflect.ValueOf(v))
	kind := results.Kind()
	switch kind {
	case reflect.Slice:
		return scanRows(rows, results)
	default:
		return scanRow(rows, results)
	}
}
