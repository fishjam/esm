/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cheggaaa/pb"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
)

func (c *Migrator) recoveryIndexSettings(sourceIndexRefreshSettings map[string]interface{}) {
	//update replica and refresh_interval
	for name, interval := range sourceIndexRefreshSettings {
		tempIndexSettings := getEmptyIndexSettings()
		tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"] = interval
		//tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = 1
		c.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)
		if c.Config.Refresh {
			c.TargetESAPI.Refresh(name)
		}
	}
}

func (c *Migrator) ClusterVersion(host string, auth *Auth, proxy string) (*ClusterVersion, []error) {

	url := fmt.Sprintf("%s", host)
	resp, body, errs := Get(url, auth, proxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs
	}

	log.Debug(body)

	version := &ClusterVersion{}
	err := json.Unmarshal([]byte(body), version)

	if err != nil {
		log.Error(body, errs)
		return nil, errs
	}
	return version, nil
}

func (c *Migrator) ParseEsApi(isSource bool, host string, auth *Auth, proxy string, compress bool) ESAPI {

	esVersion, errs := c.ClusterVersion(host, auth, proxy)
	if errs != nil {
		return nil
	}

	esInfo := "dest"
	if isSource {
		esInfo = "source"
	}

	log.Infof("%s es version: %s", esInfo, esVersion.Version.Number)
	if strings.HasPrefix(esVersion.Version.Number, "7.") {
		log.Debug("es is V7,", esVersion.Version.Number)
		api := new(ESAPIV7)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "6.") {
		log.Debug("es is V6,", esVersion.Version.Number)
		api := new(ESAPIV6)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "5.") {
		log.Debug("es is V5,", esVersion.Version.Number)
		api := new(ESAPIV5)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else {
		log.Debug("es is not V5,", esVersion.Version.Number)
		api := new(ESAPIV0)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
	}
}

func (c *Migrator) ClusterReady(api ESAPI) (*ClusterHealth, bool) {
	health := api.ClusterHealth()

	if !c.Config.WaitForGreen {
		return health, true
	}

	if health.Status == "red" {
		return health, false
	}

	if c.Config.WaitForGreen == false && health.Status == "yellow" {
		return health, true
	}

	if health.Status == "green" {
		return health, true
	}

	return health, false
}

func (c *Migrator) NewBulkWorker(docCount *int, pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	idleDuration := 5 * time.Second
	idleTimeout := time.NewTimer(idleDuration)
	defer idleTimeout.Stop()

	taskTimeOutDuration := 5 * time.Minute
	taskTimeout := time.NewTimer(taskTimeOutDuration)
	defer taskTimeout.Stop()

READ_DOCS:
	for {
		idleTimeout.Reset(idleDuration)
		taskTimeout.Reset(taskTimeOutDuration)
		select {
		case docI, open := <-c.DocChan:
			var err error
			log.Trace("read doc from channel,", docI)
			// this check is in case the document is an error with scroll stuff
			if status, ok := docI["status"]; ok {
				if status.(int) == 404 {
					log.Error("error: ", docI["response"])
					continue
				}
			}

			// sanity check
			for _, key := range []string{"_index", "_type", "_source", "_id"} {
				if _, ok := docI[key]; !ok {
					break READ_DOCS
				}
			}

			var tempDestIndexName string
			var tempTargetTypeName string
			tempDestIndexName = docI["_index"].(string)
			tempTargetTypeName = docI["_type"].(string)

			if c.Config.TargetIndexName != "" {
				tempDestIndexName = c.Config.TargetIndexName
			}

			if c.Config.OverrideTypeName != "" {
				tempTargetTypeName = c.Config.OverrideTypeName
			}

			doc := Document{
				Index:  tempDestIndexName,
				Type:   tempTargetTypeName,
				source: docI["_source"].(map[string]interface{}),
				Id:     docI["_id"].(string),
			}

			if c.Config.RegenerateID {
				doc.Id = ""
			}

			if c.Config.RenameFields != "" {
				kvs := strings.Split(c.Config.RenameFields, ",")
				for _, i := range kvs {
					fvs := strings.Split(i, ":")
					oldField := strings.TrimSpace(fvs[0])
					newField := strings.TrimSpace(fvs[1])
					if oldField == "_type" {
						doc.source[newField] = docI["_type"].(string)
					} else {
						v := doc.source[oldField]
						doc.source[newField] = v
						delete(doc.source, oldField)
					}
				}
			}

			// add doc "_routing" if exists
			if _, ok := docI["_routing"]; ok {
				str, ok := docI["_routing"].(string)
				if ok && str != "" {
					doc.Routing = str
				}
			}

			// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

			// sanity check
			if len(doc.Index) == 0 || len(doc.Type) == 0 {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

			// encode the doc and and the _source field for a bulk request
			post := map[string]Document{
				"index": doc,
			}
			if err = docEnc.Encode(post); err != nil {
				log.Error(err)
			}
			if err = docEnc.Encode(doc.source); err != nil {
				log.Error(err)
			}

			// append the doc to the main buffer
			mainBuf.Write(docBuf.Bytes())
			// reset for next document
			bulkItemSize++
			(*docCount)++
			docBuf.Reset()

			// if we approach the 100mb es limit, flush to es and reset mainBuf
			if mainBuf.Len()+docBuf.Len() > (c.Config.BulkSizeInMB * 1024 * 1024) {
				goto CLEAN_BUFFER
			}

		case <-idleTimeout.C:
			log.Debug("5s no message input")
			goto CLEAN_BUFFER
		case <-taskTimeout.C:
			log.Warn("5m no message input, close worker")
			goto WORKER_DONE
		}

		goto READ_DOCS

	CLEAN_BUFFER:
		c.TargetESAPI.Bulk(&mainBuf)
		log.Trace("clean buffer, and execute bulk insert")
		pb.Add(bulkItemSize)
		bulkItemSize = 0
		if c.Config.SleepSecondsAfterEachBulk > 0 {
			time.Sleep(time.Duration(c.Config.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
WORKER_DONE:
	if docBuf.Len() > 0 {
		mainBuf.Write(docBuf.Bytes())
		bulkItemSize++
	}
	c.TargetESAPI.Bulk(&mainBuf)
	log.Trace("bulk insert")
	pb.Add(bulkItemSize)
	bulkItemSize = 0
	wg.Done()
}
