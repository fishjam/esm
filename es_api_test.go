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

func compareEsData(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// _id => value
	srcDocMaps := make(map[string]interface{})
	dstDocMaps := make(map[string]interface{})
	diffDocMaps := make(map[string]interface{})
	srcRecordIndex := 0
	dstRecordIndex := 0

	var srcScroll ScrollAPI = nil
	var dstScroll ScrollAPI = nil

	for {
		if srcScroll == nil {
			srcScroll = VerifyWithResult(srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)).(ScrollAPI)
			log.Infof("src total count=%d", srcScroll.GetHitsTotal())
		} else {
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}

		if dstScroll == nil {
			dstScroll = VerifyWithResult(dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)).(ScrollAPI)
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
		} else {
			dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
		}

		//先将当前批次查出的放入 map
		for idx, srcDocI := range srcScroll.GetDocs() {
			log.Infof("src [%d]: docI=%+v", srcRecordIndex+idx, srcDocI)
			srcId := srcDocI.(map[string]interface{})["_id"].(string)
			srcSource := srcDocI.(map[string]interface{})["_source"]
			srcDocMaps[srcId] = srcSource
		}

		//从目标 index 中查询,比较是否有变化
		for idx, dstDocI := range dstScroll.GetDocs() {
			dstId := dstDocI.(map[string]interface{})["_id"].(string)
			dstSource := dstDocI.(map[string]interface{})["_source"]
			log.Infof("dst [%d]: dstId=%s", dstRecordIndex+idx, dstDocI)

			//从 srcMap 中找到 id 和 dst 中相同的
			if srcSource, ok := srcDocMaps[dstId]; ok {
				updates := compareStructs(srcSource, dstSource)
				if len(updates) > 0 {
					log.Infof("dstId=%s 有变化: %+v", dstId, updates)
					diffDocMaps[dstId] = updates
				} else {
					log.Infof("dstId=%s 相同", dstId)
				}
				//从 srcDocMaps 中删除
				delete(srcDocMaps, dstId)
			} else {
				//从 srcMap 中没找到, 可能是 dst中特有的,或者 src 中还没有查出来
				log.Infof("没有找到 dstId=%s", dstId)
				dstDocMaps[dstId] = dstDocI
			}
		}
		srcRecordIndex += len(srcScroll.GetDocs())
		dstRecordIndex += len(dstScroll.GetDocs())

		//如果 src 和 dst 都遍历完毕, 才退出
		if (len(srcScroll.GetDocs()) == 0 || len(srcScroll.GetDocs()) < cfg.DocBufferCount) &&
			(len(dstScroll.GetDocs()) == 0 || len(dstScroll.GetDocs()) < cfg.DocBufferCount) {
			log.Warnf("can not find more, will quit")
			break
		}
	}

	log.Infof("srcRecordIndex=%d, dstRecordIndex=%d", srcRecordIndex, dstRecordIndex)
	log.Infof("diffDocMaps=%+v", diffDocMaps)
	//srcLength := len(srcScroll.GetDocs())
	//dstLength := len(dstScroll.GetDocs())
	//minLength := int(math.Min(float64(srcLength), float64(dstLength)))
	//
	//for idx := 0; idx < minLength; idx++ {
	//	srcDocI := srcScroll.GetDocs()[srcRecordIndex]
	//	dstDocI := dstScroll.GetDocs()[dstRecordIndex]
	//
	//	srcId := srcDocI.(map[string]interface{})["_id"].(string)
	//	dstId := dstDocI.(map[string]interface{})["_id"].(string)
	//
	//	log.Infof("src: [%d]'s id=%+v, dst: [%d]'s id=%+v", srcRecordIndex, srcId, dstRecordIndex, dstId)
	//	if srcId == dstId {
	//		srcRecordIndex++
	//		dstRecordIndex++
	//	} else if srcId < dstId {
	//		srcRecordIndex++
	//	} else {
	//		dstRecordIndex++
	//	}
	//}
}

func TestEsApi(t *testing.T) {
	cfg := &Config{
		SourceEs:         "http://127.0.0.1:9200",
		SourceIndexNames: "srcIndex",
		SourceProxy:      "http://127.0.0.1:8888",

		TargetEs:        "http://127.0.0.1:9200",
		TargetIndexName: "dstIndex",
		TargetProxy:     "http://127.0.0.1:8888",

		ScrollTime:      "10m",
		DocBufferCount:  20,
		Query:           "",
		SortField:       "_id",
		ScrollSliceSize: 1,
		Fields:          "",
	}
	srcAuth := &Auth{User: "user", Pass: "passwd"}
	dstAuth := &Auth{User: "user", Pass: "passwd"}

	migrator := Migrator{}
	srcEsApi := migrator.ParseEsApi(true, cfg.SourceEs, srcAuth, cfg.SourceProxy, false)
	dstEsApi := migrator.ParseEsApi(false, cfg.TargetEs, dstAuth, cfg.TargetProxy, false)

	srcVersion := srcEsApi.ClusterVersion()
	dstVersion := dstEsApi.ClusterVersion()
	log.Infof("src version=%+v, dst version=%+v", srcVersion, dstVersion)

	if false && srcEsApi != nil && dstEsApi != nil {
		curRecordIndex := 0
		var scroll ScrollAPI = nil
		for {
			if scroll == nil {
				scroll = VerifyWithResult(srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
					cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)).(ScrollAPI)
				log.Infof("scroll: id=%s ", scroll.GetScrollId())
			} else {
				scroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, scroll.GetScrollId())).(ScrollAPI)
			}

			for idx, docI := range scroll.GetDocs() {
				log.Infof(" [%d]: docI=%+v", curRecordIndex+idx, docI)
			}
			curRecordIndex += len(scroll.GetDocs())

			if scroll == nil || len(scroll.GetDocs()) == 0 || len(scroll.GetDocs()) < cfg.DocBufferCount {
				log.Warnf("can not find more, will quit")
				break
			}
		}

		assert.Equal(t, scroll.GetHitsTotal(), curRecordIndex, "遍历完所有的数据")
		srcEsApi.DeleteScroll(scroll.GetScrollId())
	}

	compareEsData(srcEsApi, dstEsApi, cfg)

	fmt.Printf("\n")
	//dstApi.NewScroll()
	assert.Equal(t, 1, 1)
}
