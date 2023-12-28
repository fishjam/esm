package main

import (
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func init() {
	setInitLogging("info")
}

func compareStructs(oldStruct, newStruct interface{}) map[string]interface{} {
	updates := make(map[string]interface{})

	log.Infof("will compare %+v <==> %+v", oldStruct, newStruct)

	oldValue := reflect.ValueOf(oldStruct)
	newValue := reflect.ValueOf(newStruct)

	log.Infof("oldValue.Type()=%+v, newValue.Type()=%+v", oldValue.Type(), newValue.Type())

	// 检查两个结构体是否具有相同的类型
	if oldValue.Type() != newValue.Type() {
		return updates // 结构体类型不同，无法比较
	}

	switch oldValue.Kind() {
	case reflect.Slice, reflect.Array:
		if oldValue.Len() != newValue.Len() {
			return updates
		}
		for i := 0; i < oldValue.Len(); i++ {
			arrayDiff := compareStructs(oldValue.Index(i), newValue.Index(i))
			if len(arrayDiff) > 0 {
				panic("TODO")
				return updates
			}
		}
		return updates

	case reflect.Map:
		if oldValue.Len() != newValue.Len() {
			panic("TODO")
			return updates
		}
		for _, key := range oldValue.MapKeys() {
			mapUpdates := compareStructs(oldValue.MapIndex(key), newValue.MapIndex(key))
			if len(mapUpdates) > 0 {
				panic("TODO")
				return updates
			}
		}
		return updates

	case reflect.Struct:
		for i := 0; i < oldValue.NumField(); i++ {
			oldFieldValue := oldValue.Field(i).Interface()
			newFieldValue := newValue.Field(i).Interface()

			// 比较字段值是否相等
			if !reflect.DeepEqual(oldFieldValue, newFieldValue) {
				updates[oldValue.Type().Field(i).Name] = newFieldValue
			}
			//structUpdates := compareStructs(oldValue.Field(i), newValue.Field(i))
			//if len(structUpdates) > 0 {
			//	panic("TODO")
			//	return updates
			//}
		}
		return updates

	default:
		panic("TODO")
		//return reflect.DeepEqual(oldValue.Interface(), newValue.Interface())
	}

	return updates
}

func GetConfig() *Config {
	cfg := &Config{
		SourceEs:         "http://127.0.0.1:9200",
		SourceIndexNames: "bank",
		SourceProxy:      "http://127.0.0.1:8888",

		TargetEs:        "http://127.0.0.1:9200",
		TargetIndexName: "bank_esm",
		TargetProxy:     "http://127.0.0.1:8888",

		ScrollTime:      "10m",
		DocBufferCount:  20,
		Query:           "",
		SortField:       "_id",
		ScrollSliceSize: 1,
		Fields:          "",
	}
	return cfg
}

func TestEsApi(t *testing.T) {
	cfg := GetConfig()
	//srcAuth := &Auth{User: "user", Pass: "passwd"}
	//dstAuth := &Auth{User: "user", Pass: "passwd"}

	migrator := Migrator{}
	srcEsApi := migrator.ParseEsApi(true, cfg.SourceEs, "", cfg.SourceProxy, false)
	dstEsApi := migrator.ParseEsApi(false, cfg.TargetEs, "", cfg.TargetProxy, false)

	srcVersion := srcEsApi.ClusterVersion()
	dstVersion := dstEsApi.ClusterVersion()
	log.Infof("src version=%+v, dst version=%+v", srcVersion, dstVersion)

	migrator.SyncBetweenIndex(srcEsApi, dstEsApi, cfg)

	fmt.Printf("\n")
	//dstApi.NewScroll()
	assert.Equal(t, 1, 1)
}
