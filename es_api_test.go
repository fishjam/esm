package main

import (
	"bytes"
	"encoding/json"
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

type BulkOperation uint8

const (
	opIndex BulkOperation = iota
	opDelete
)

func (op BulkOperation) String() string {
	switch op {
	case opIndex:
		return "opIndex"
	case opDelete:
		return "opDelete"
	default:
		return fmt.Sprintf("unknown:%d", op)
	}
}

func bulkRecords(bulkOp BulkOperation, dstEsApi ESAPI, targetIndex string, targetType string, diffDocMaps map[string]interface{}) error {
	//var err error
	docCount := 0
	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	//var tempDestIndexName string
	//var tempTargetTypeName string

	for docId, docData := range diffDocMaps {
		docI := docData.(map[string]interface{})
		log.Infof("now will bulk %s docId=%s, docData=%+v", bulkOp, docId, docData)
		//tempDestIndexName = docI["_index"].(string)
		//tempTargetTypeName = docI["_type"].(string)
		var strOperation string
		doc := Document{
			Index: targetIndex,
			Type:  targetType,
			Id:    docId, // docI["_id"].(string),
		}

		switch bulkOp {
		case opIndex:
			doc.source = docI // docI["_source"].(map[string]interface{}),
			strOperation = "index"
		case opDelete:
			strOperation = "delete"
			//do nothing
		}

		// encode the doc and and the _source field for a bulk request

		post := map[string]Document{
			strOperation: doc,
		}
		_ = Verify(docEnc.Encode(post))
		if bulkOp == opIndex {
			_ = Verify(docEnc.Encode(doc.source))
		}
		// append the doc to the main buffer
		mainBuf.Write(docBuf.Bytes())
		// reset for next document
		bulkItemSize++
		docCount++
		docBuf.Reset()
	}

	if mainBuf.Len() > 0 {
		dstEsApi.Bulk(&mainBuf)
	}

	return nil
}

func compareEsData(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// _id => value
	srcDocMaps := make(map[string]interface{})
	dstDocMaps := make(map[string]interface{})
	diffDocMaps := make(map[string]interface{})

	srcRecordIndex := 0
	dstRecordIndex := 0
	var err error
	srcType := ""
	var srcScroll ScrollAPI = nil
	var dstScroll ScrollAPI = nil
	var emptyScroll = &EmptyScroll{}
	lastSrcId := ""
	lastDestId := ""
	needScrollSrc := true
	needScrollDest := true

	for {
		if srcScroll == nil {
			srcScroll = VerifyWithResult(srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)).(ScrollAPI)
			log.Infof("src total count=%d", srcScroll.GetHitsTotal())
		} else if needScrollSrc {
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}

		if dstScroll == nil {
			dstScroll, err = dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				log.Infof("can not scroll for %s, reason:%s", cfg.TargetIndexName, err.Error())

				//生成一个 empty 的, 相当于直接bulk?
				dstScroll = emptyScroll
			}
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
		} else if needScrollDest {
			dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
		}

		//从目标 index 中查询,并放入 destMap, 如果没有则是空
		if needScrollDest {
			for idx, dstDocI := range dstScroll.GetDocs() {
				destId := dstDocI.(map[string]interface{})["_id"].(string)
				dstSource := dstDocI.(map[string]interface{})["_source"]
				lastDestId = destId
				log.Debugf("dst [%d]: dstId=%s", dstRecordIndex+idx, destId)

				if srcSource, found := srcDocMaps[destId]; found {
					delete(srcDocMaps, destId)

					//如果从 src 的 map 中找到匹配地项
					if !reflect.DeepEqual(srcSource, dstSource) {
						//不相等, 则需要更新
						diffDocMaps[destId] = srcSource
					} else {
						//完全相等, 则不需要处理
					}
				} else {
					dstDocMaps[destId] = dstSource
				}
			}
			dstRecordIndex += len(dstScroll.GetDocs())
		}

		//先将 src 的当前批次查出并放入 map
		if needScrollSrc {
			for idx, srcDocI := range srcScroll.GetDocs() {
				srcId := srcDocI.(map[string]interface{})["_id"].(string)
				srcSource := srcDocI.(map[string]interface{})["_source"]
				srcType = srcDocI.(map[string]interface{})["_type"].(string)
				lastSrcId = srcId
				log.Debugf("src [%d]: srcId=%s", srcRecordIndex+idx, srcId)

				if len(lastDestId) == 0 {
					//没有 destId, 表示 目标 index 中没有数据, 直接全部更新
					diffDocMaps[srcId] = srcSource
				} else if dstSource, ok := dstDocMaps[srcId]; ok { //能从 dstDocMaps 中找到相同ID的数据
					if !reflect.DeepEqual(srcSource, dstSource) {
						//不完全相同,需要更新,否则忽略
						diffDocMaps[srcId] = srcSource
					} else {
						//从 dst 中删除相同的
						delete(dstDocMaps, srcId)
					}
				} else {
					//找不到相同的 id, 可能是 dst 还没找到, 或者 dst 中不存在
					if srcId < lastDestId {
						//dest 已经超过当前的 srcId, 表示 dst 中不存在
						diffDocMaps[srcId] = srcSource
					} else {
						srcDocMaps[srcId] = srcSource
					}
				}
			}
			srcRecordIndex += len(srcScroll.GetDocs())
		}

		if len(diffDocMaps) > 0 {
			log.Infof("now will bulk index %d records", len(diffDocMaps))
			_ = Verify(bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, diffDocMaps))
			diffDocMaps = make(map[string]interface{})
		}

		if lastSrcId == lastDestId {
			needScrollSrc = true
			needScrollDest = true
		} else if len(lastDestId) == 0 || (lastSrcId < lastDestId || (needScrollDest == true && len(dstScroll.GetDocs()) == 0)) {
			//上一次要求遍历 dest,但遍历出空
			needScrollSrc = true
			needScrollDest = false
		} else if lastSrcId > lastDestId || (needScrollSrc == true && len(srcScroll.GetDocs()) == 0) {
			//上一次要求遍历 src, 但遍历出空
			needScrollSrc = false
			needScrollDest = true
		} else {
			panic("TODO:")
		}

		//如果 src 和 dst 都遍历完毕, 才退出
		log.Debugf("lastSrcId=%s, lastDestId=%s, "+
			"needScrollSrc=%t, len(srcScroll.GetDocs()=%d, "+
			"needScrollDest=%t, len(dstScroll.GetDocs())=%d",
			lastSrcId, lastDestId,
			needScrollSrc, len(srcScroll.GetDocs()),
			needScrollDest, len(dstScroll.GetDocs()))

		if (!needScrollSrc || (len(srcScroll.GetDocs()) == 0 || len(srcScroll.GetDocs()) < cfg.DocBufferCount)) &&
			(!needScrollDest || (len(dstScroll.GetDocs()) == 0 || len(dstScroll.GetDocs()) < cfg.DocBufferCount)) {
			log.Warnf("can not find more, will quit, and index %d, delete %d", len(srcDocMaps), len(dstDocMaps))

			if len(srcDocMaps) > 0 {
				_ = Verify(bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, srcDocMaps))
			}
			if len(dstDocMaps) > 0 {
				//最后在 dst 中还有遗留的,表示 dst 中多的.需要删除
				_ = Verify(bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, dstDocMaps))
			}
			break
		}

		//( { //
		//目标不存在 或 src 还没有查询到和 dest 一样的地方
	}
	_ = Verify(srcEsApi.DeleteScroll(srcScroll.GetScrollId()))
	_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))

	log.Infof("srcRecordIndex=%d, dstRecordIndex=%d", srcRecordIndex, dstRecordIndex)
	log.Infof("diffDocMaps=%+v", diffDocMaps)
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
	srcEsApi := migrator.ParseEsApi(true, cfg.SourceEs, nil, cfg.SourceProxy, false)
	dstEsApi := migrator.ParseEsApi(false, cfg.TargetEs, nil, cfg.TargetProxy, false)

	srcVersion := srcEsApi.ClusterVersion()
	dstVersion := dstEsApi.ClusterVersion()
	log.Infof("src version=%+v, dst version=%+v", srcVersion, dstVersion)

	compareEsData(srcEsApi, dstEsApi, cfg)

	fmt.Printf("\n")
	//dstApi.NewScroll()
	assert.Equal(t, 1, 1)
}
