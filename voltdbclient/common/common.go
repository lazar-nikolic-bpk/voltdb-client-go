package common

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

func TypeLen(param driver.Value) int {
	v := reflect.ValueOf(param)
	switch v.Kind() {
	case reflect.Bool:
		return 1
	case reflect.Int8:
		return 1
	case reflect.Int16:
		return 2
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.Float64:
		return 8
	case reflect.String:
		return 4 + v.Len()
	case reflect.Slice:
		// len + actual size
		return 4 + v.Len()
	case reflect.Struct:
		if _, ok := v.Interface().(time.Time); ok {
			return 8
		}
		panic("Can't determine length of struct")

	case reflect.Ptr:
		panic("Can't marshal a pointer")
	default:
		panic(fmt.Sprintf("Can't marshal %v-type parameters", v.Kind()))
	}
}
