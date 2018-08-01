package main

import (
	"fmt"
	"../ovsdb"
	//"../ovsdb/dbmonitor"
	"../ovsdb/ovshelper"
	//"../ovsdb/entities"
	"encoding/json"
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


	// fetch references
	txn := db.Transaction("Open_vSwitch")
	txn.Wait(2000, "Open_vSwitch", [][]string{}, []string{"bridges"}, "!=", []interface{}{})
	txn.Select("Open_vSwitch", []string{"bridges"}, [][]string{})
	txn.Select("Bridge", []string{"_uuid"}, [][]string{{"name", "==", "Test Bridge"}})
	go func(){
		txn.Commit()
	}()

	time.AfterFunc(500, func(){
		txn.Cancel()
	})
	time.AfterFunc(1000, func(){
		db.ListDbs()
	})

	//// build data
	//var bridges []interface{}
	//bridgeResult := res[0].Rows[0].(map[string]interface{})["bridges"].([]interface{})
	//if bridgeResult[0] == "set" {
	//	bridges = bridgeResult[1].([]interface{})
	//} else { // single entry
	//	bridges = []interface{}{bridgeResult}
	//}
	//
	//// delete bridge
	//var bridgeUUID string
	//if len(res[1].Rows) == 1 {
	//	bridgeUUID = res[1].Rows[0].(map[string]interface{})["_uuid"].([]interface{})[1].(string)
	//	for idx, bridge := range bridges {
	//		if bridge.([]interface{})[1] == bridgeUUID {
	//			bridges[idx] = bridges[len(bridges)-1]
	//			bridges = bridges[:len(bridges)-1]
	//		}
	//	}
	//
	//	txn2 := db.Transaction("Open_vSwitch")
	//	txn2.Update("Open_vSwitch", map[string]interface{}{
	//		"bridges": []interface{}{
	//			"set",
	//			bridges,
	//		},
	//	})
	//	txn2.Commit()
	//}
	//
	//bridge := ovshelper.Bridge{
	//	Name: "Test Bridge",
	//}
	//
	//// store bridge and reference
	//txn2 := db.Transaction("Open_vSwitch")
	//bridgeTempId := txn2.Insert("Bridge", bridge)
	//bridges = append(bridges, []interface{}{"named-uuid", bridgeTempId})
	//txn2.Update("Open_vSwitch", map[string]interface{}{
	//	"bridges": []interface{}{
	//		"set",
	//		bridges,
	//	},
	//})
	//fmt.Println(txn2.Commit())


	// register lock notification
	//db.RegisterLockedCallback(lockedNotification)
	//db.RegisterStolenCallback(stolenNotification)

	//fmt.Println("databases:", db.ListDbs())

	//response, err := db.GetSchema("Open_vSwitch")
	//schema := ovshelper.Schema{}
	//json.Unmarshal(response, &schema)
	//fmt.Println("bridge:", schema.Tables["Bridge"])

	//monitor := db.Monitor("Open_vSwitch")
	//monitor.Register("Bridge", dbmonitor.Table{
	//	Columns:[]string{
	//		"name",
	//	},
	//	Select: dbmonitor.Select{ Insert: true, },
	//})
	//monitor.Start(updateCallback)
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

	for true {

	}

	fmt.Println(db.Close())
}
