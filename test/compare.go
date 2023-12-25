package main

import (
	"fmt"
	"reflect"
)

type Person struct {
	Name string
	Age  int
}

func compareStructs(oldStruct, newStruct interface{}) map[string]interface{} {
	updates := make(map[string]interface{})

	oldValue := reflect.ValueOf(oldStruct)
	newValue := reflect.ValueOf(newStruct)

	// 检查两个结构体是否具有相同的类型
	if oldValue.Type() != newValue.Type() {
		return nil // 结构体类型不同，无法比较
	}

	// 遍历结构体的字段
	for i := 0; i < oldValue.NumField(); i++ {
		oldFieldValue := oldValue.Field(i).Interface()
		newFieldValue := newValue.Field(i).Interface()

		// 比较字段值是否相等
		if !reflect.DeepEqual(oldFieldValue, newFieldValue) {
			updates[oldValue.Type().Field(i).Name] = newFieldValue
		}
	}

	return updates
}

func main() {
	// 创建两个结构体实例
	oldPerson := Person{"John", 30}
	newPerson := Person{"John", 29}

	// 比较结构体值并找出更新的字段
	updates := compareStructs(oldPerson, newPerson)

	// 打印更新的字段
	if len(updates) > 0 {
		fmt.Println("更新的字段:")
		for field, value := range updates {
			fmt.Printf("%s: %v\n", field, value)
		}
	} else {
		fmt.Println("结构体值相等，没有更新的字段")
	}
}
