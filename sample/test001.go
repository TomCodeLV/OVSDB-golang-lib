package main

import (
	"fmt"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/ovsdb"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbmonitor"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/ovshelper"
	//"../ovsdb/entities"
	"encoding/json"
	//"time"
	"time"
)

func updateCallback(response json.RawMessage) {
	var update ovshelper.Update
	json.Unmarshal(response, &update)

	if update.Bridge != nil {
		for uuid, change := range update.Bridge {
			fmt.Println("Bridge " + change.New.Name + " inserted (" + uuid + ")")
		}
	}
}

func deleteCallback(response interface{}) {
	fmt.Println("delete:")
	fmt.Println(response)
}

func lockedNotification(id string) {
	fmt.Println("")
	fmt.Println(id)
}

func stolenNotification(id string) {
	fmt.Println("")
	fmt.Println(id)
}

func main() {
	fmt.Println("Client started!")

	// dial in
	db, err := ovsdb.Dial("tcp", ":12345") //db, err := ovsdb.Dial("unix", "/run/openvswitch/db.sock")
	if err != nil {
		fmt.Println("unable to dial: " + err.Error())
		return
	}

	// register lock notification
	//db.RegisterLockedCallback(lockedNotification)
	//db.RegisterStolenCallback(stolenNotification)

	//fmt.Println("databases:", db.ListDbs())

	//response, err := db.GetSchema("Open_vSwitch")
	//schema := ovshelper.Schema{}
	//json.Unmarshal(response, &schema)
	//fmt.Println("bridge:", schema.Tables["Bridge"])

	loop := true
	updateCount := 0

	monitor := db.Monitor("Open_vSwitch")
	monitor.Register("Open_vSwitch", dbmonitor.Table{
		Columns:[]string{
			"next_cfg",
		},
		Select: dbmonitor.Select{ Modify: true, },
	})
	monitor.Start(func(response json.RawMessage) {
		updateCount += 1
		var update struct{
			Open_vSwitch map[string]interface{}
		}
		json.Unmarshal(response, &update)

		if update.Open_vSwitch != nil {
			loop = false
		}
	})

	// dial in
	//db2, err := ovsdb.Dial("tcp", ":12345") //db, err := ovsdb.Dial("unix", "/run/openvswitch/db.sock")
	//if err != nil {
	//	fmt.Println("unable to dial: " + err.Error())
	//	return
	//}

	txn := db.Transaction("Open_vSwitch")
	txn.Mutate("Open_vSwitch", [][]string{}, [][]interface{}{{"next_cfg", "+=", 1}})
	resp, err := txn.Commit()
	fmt.Println(resp, err)

	time.AfterFunc(time.Millisecond * 300, func(){
		//t.Error("Transaction monitor timeout")
		fmt.Println("error")
		loop = false
	})

	monitor.Cancel()

	txn2 := db.Transaction("Open_vSwitch")
	txn2.Mutate("Open_vSwitch", [][]string{}, [][]interface{}{{"next_cfg", "+=", 1}})
	resp2, err2 := txn2.Commit()
	fmt.Println(resp2, err2)

	for loop {

	}

	if updateCount > 1 {
		fmt.Println("errors 2")
	}

	fmt.Println("jo")

	//
	//txn0 := db.Transaction("Open_vSwitch")
	//txn0.Select("Open_vSwitch", []string{"bridges"})
	//res, _ := txn0.Commit()
	//fmt.Println("select: ", res)
	//
	//var bridges []interface{}
	//bridgeResult := res[0].Rows[0].(map[string]interface{})["bridges"].([]interface{})
	//if bridgeResult[0] == "set" {
	//	bridges = bridgeResult[1].([]interface{})
	//} else { // single entry
	//	bridges = []interface{}{bridgeResult}
	//}
	//bridge := ovshelper.Bridge{
	//	Name: "asd7",
	//}
	//
	//
	//txn := db.Transaction("Open_vSwitch")
	//
	//bridgeTempId := txn.Insert("Bridge", bridge)
	//bridges = append(bridges, []interface{}{"named-uuid", bridgeTempId})
	//txn.Update("Open_vSwitch", map[string]interface{}{
	//	"bridges": []interface{}{
	//		"set",
	//		bridges,
	//	},
	//})
	//
	//resp, err := txn.Commit()
	//fmt.Println(resp, err)

	//txn3 := db.Transaction("Open_vSwitch")
	//txn3.Delete("Bridge", [][]string{{"name", "==", "asd7"}})
	//resp3, err3 := txn3.Commit()
	//fmt.Println(resp3, err3)

	//txn2 := db.Transaction("Open_vSwitch")
	//txn2.Insert("Port", ovshelper.Port{Name: "18"})
	//txn2.Commit()


	//fmt.Println(monitor.Cancel())
	//db.Lock("some")

	//txn2 := db.Transaction("Open_vSwitch")
	//txn2.Insert(entities.Bridge{Name: "19"})
	//res2, err2 := txn2.Commit()
	//if err2 != nil {
	//	fmt.Println(err2)
	//} else {
	//	fmt.Println(res2)
	//}

	//for true {
	//
	//}

	fmt.Println(db.Close())
}
