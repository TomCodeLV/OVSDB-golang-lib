package ovsdb

import (
	"testing"
	"encoding/json"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/ovshelper"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbmonitor"
	"time"
)

var network = "tcp" 	// "unix"
var address = ":12345" 	// "/run/openvswitch/db.sock"

func TestDial(t *testing.T) {
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		db.Close()
	}
}

func TestDialDouble(t *testing.T) {
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	db2, err2 := Dial(network, address)
	if err2 != nil {
		t.Error("Dial failed")
	} else {
		defer db2.Close()
	}
}

func TestOVSDB_ListDbs(t *testing.T) {
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	found := false
	dbs := db.ListDbs()
	for _, db := range dbs {
		if db == "Open_vSwitch" {
			found = true
		}
	}

	if found == false {
		t.Error("Open_vSwitch not found")
	}
}

func TestOVSDB_GetSchema(t *testing.T) {
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	response, err := db.GetSchema("Open_vSwitch")
	if err != nil {
		t.Error("Get schema failed")
		return
	}

	schema := ovshelper.Schema{}
	err2 := json.Unmarshal(response, &schema)
	if err2 != nil {
		t.Error("Unmarshal error")
		return
	}

	if schema.Name != "Open_vSwitch" {
		t.Error("Wrong schema name")
	}
}

func TestOVSDB_Transaction(t *testing.T) {
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	// fetch references
	txn := db.Transaction("Open_vSwitch")
	txn.Select("Open_vSwitch", []string{"bridges"}, [][]string{})
	txn.Select("Bridge", []string{"_uuid"}, [][]string{{"name", "==", "Test Bridge"}})
	res, err := txn.Commit()
	if err != nil {
		t.Error("Select failed")
		return
	}

	// build data
	var bridges []interface{}
	bridgeResult := res[0].Rows[0].(map[string]interface{})["bridges"].([]interface{})
	if bridgeResult[0] == "set" {
		bridges = bridgeResult[1].([]interface{})
	} else { // single entry
		bridges = []interface{}{bridgeResult}
	}

	// delete bridge
	var bridgeUUID string
	if len(res[1].Rows) == 1 {
		bridgeUUID = res[1].Rows[0].(map[string]interface{})["_uuid"].([]interface{})[1].(string)
		for idx, bridge := range bridges {
			if bridge.([]interface{})[1] == bridgeUUID {
				bridges[idx] = bridges[len(bridges)-1]
				bridges = bridges[:len(bridges)-1]
			}
		}

		txn2 := db.Transaction("Open_vSwitch")
		txn2.Update("Open_vSwitch", map[string]interface{}{
			"bridges": []interface{}{
				"set",
				bridges,
			},
		})
		_, err2 := txn.Commit()
		if err2 != nil {
			t.Error("Delete failed")
			return
		}
	}

	bridge := ovshelper.Bridge{
		Name: "Test Bridge",
	}

	// store bridge and reference
	txn3 := db.Transaction("Open_vSwitch")
	bridgeTempId := txn3.Insert("Bridge", bridge)
	bridges = append(bridges, []interface{}{"named-uuid", bridgeTempId})
	txn3.Update("Open_vSwitch", map[string]interface{}{
		"bridges": []interface{}{
			"set",
			bridges,
		},
	})
	_, err3 := txn3.Commit()
	if err3 != nil {
		t.Error("Insert failed")
	}
}

func TestOVSDB_Transaction_Cancel(t *testing.T) {
	loop := true
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	txn := db.Transaction("Open_vSwitch")
	txn.Wait(200, "Open_vSwitch", [][]string{}, []string{"bridges"}, "==", []interface{}{})
	go func(){
		txn.Commit()
		t.Error("Transaction cancel failed")
		loop = false
	}()

	time.AfterFunc(time.Millisecond * 100, func(){
		txn.Cancel()
	})

	time.AfterFunc(time.Millisecond * 300, func(){
		t.Error("Transaction cancel timeout")
		loop = false
	})

	for loop {}
}

func TestOVSDB_Monitor_And_Mutate(t *testing.T) {
	loop := true
	updateCount := 0

	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	// start monitor
	monitor := db.Monitor("Open_vSwitch")
	monitor.Register("Open_vSwitch", dbmonitor.Table{
		Columns:[]string{
			"next_cfg",
		},
		Select: dbmonitor.Select{ Modify: true, },
	})
	monitor.Start(func(response json.RawMessage) {
		// process update notification
		updateCount += 1
		var update struct{
			Open_vSwitch map[string]interface{}
		}
		json.Unmarshal(response, &update)

		if update.Open_vSwitch != nil {
			loop = false
		} else {
			t.Error("Update notification failed")
		}
	})

	// first change
	txn := db.Transaction("Open_vSwitch")
	txn.Mutate("Open_vSwitch", [][]string{}, [][]interface{}{{"next_cfg", "+=", 1}})
	_, err2 := txn.Commit()
	if err2 != nil {
		t.Error("Mutate failed")
	}

	time.AfterFunc(time.Millisecond * 300, func(){
		t.Error("Monitor timeout")
		loop = false
	})

	_, err3 := monitor.Cancel()
	if err3 != nil {
		t.Error("Monitor cancel request failed")
	}

	// second change
	txn2 := db.Transaction("Open_vSwitch")
	txn2.Mutate("Open_vSwitch", [][]string{}, [][]interface{}{{"next_cfg", "+=", 1}})
	txn2.Commit()

	for loop {

	}

	if updateCount > 1 {
		t.Error("Monitor cancel failed")
	}
}


