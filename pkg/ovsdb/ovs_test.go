package ovsdb

import (
	"testing"
	"encoding/json"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/ovshelper"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbmonitor"
	"time"
	"fmt"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbtransaction"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/helpers"
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

func TestOVSDB_Transaction_main(t *testing.T) {
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	// fetch references
	txn := db.Transaction("Open_vSwitch")
	txn.Select(dbtransaction.Select{
		"Open_vSwitch",
		[]string{"bridges"},
		[][]string{},
	})
	txn.Select(dbtransaction.Select{
		"Bridge",
		[]string{"_uuid"},
		[][]string{{"name", "==", "Test Bridge"}},
	})
	res, err := txn.Commit()
	if err != nil {
		t.Error("Select failed")
		return
	}

	// build data
	var bridges []interface{}
	bridgeResult := res[0].Rows[0].(map[string]interface{})["bridges"].([]interface{})
	bridges = helpers.GetSet(bridgeResult)

	// delete bridge
	var bridgeUUID string
	if len(res[1].Rows) == 1 {
		bridgeUUID = res[1].Rows[0].(map[string]interface{})["_uuid"].([]interface{})[1].(string)
		fmt.Println(bridgeUUID)
		for idx, bridge := range bridges {
			if bridge.([]interface{})[1] == bridgeUUID {
				//bridges[idx] = bridges[len(bridges)-1]
				//bridges = bridges[:len(bridges)-1]
				bridges = append(bridges[:idx], bridges[idx+1:]...)
				break
			}
		}

		txn2 := db.Transaction("Open_vSwitch")
		txn2.Update(dbtransaction.Update{
			Table: "Open_vSwitch",
			Row: map[string]interface{}{
				"bridges": helpers.MakeSet(bridges),
			},
		})
		_, err2 := txn2.Commit()
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
	txn3.Wait(dbtransaction.Wait{
		0,
		"Open_vSwitch",
		[][]interface{}{},
		[]string{"bridges"},
		"==",
		[]interface{}{map[string]interface{}{"bridges": helpers.MakeSet(bridges)}},
	})
	bridgeTempId := txn3.Insert(dbtransaction.Insert{
		"Bridge",
		bridge,
	})
	bridges = append(bridges, []interface{}{"named-uuid", bridgeTempId})
	txn3.Update(dbtransaction.Update{
		Table:"Open_vSwitch",
		Row: map[string]interface{}{
			"bridges": helpers.MakeSet(bridges),
		},
	})
	_, err3 := txn3.Commit()
	if err3 != nil {
		t.Error("Insert failed")
	}
}

func TestOVSDB_Transaction_Cancel(t *testing.T) {
	loop := true
	loop = false // ignore while cancel fails
	db, err := Dial(network, address)
	if err != nil {
		t.Error("Dial failed")
	} else {
		defer db.Close()
	}

	txn := db.Transaction("Open_vSwitch")
	txn.Wait(dbtransaction.Wait{
		Timeout: 200,
		Table: "Open_vSwitch",
		Where: [][]interface{}{},
		Columns: []string{"bridges"},
		Until: "==",
		Rows: []interface{}{},
	})
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
	txn.Mutate(dbtransaction.Mutate{
		Table: "Open_vSwitch",
		Where: [][]string{},
		Mutations: [][]interface{}{{"next_cfg", "+=", 1}},
	})
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
	txn2.Mutate(dbtransaction.Mutate{
		Table: "Open_vSwitch",
		Where: [][]string{},
		Mutations: [][]interface{}{{"next_cfg", "+=", 1}},
	})
	txn2.Commit()

	for loop {

	}

	if updateCount > 1 {
		t.Error("Monitor cancel failed")
	}
}

func TestOVSDB_Cache_main(t *testing.T) {
	// dial in
	db, err := Dial("tcp", ":12345") //db, err := ovsdb.Dial("unix", "/run/openvswitch/db.sock")
	if err != nil {
		fmt.Println("unable to dial: " + err.Error())
		return
	}

	// initialize cache
	cache, err := db.Cache(Cache{
		Schema: "Open_vSwitch",
		Tables: map[string][]string{
			"Open_vSwitch": nil,
			"Bridge": nil,
		},
		Indexes: map[string][]string{
			"Bridge": {"name"},
		},
	})
	if err != nil {
		fmt.Println(err)
	}

	schemaId := cache.GetKeys("Open_vSwitch", "uuid")[0]
	bridgeList := cache.GetList("Open_vSwitch", "uuid", schemaId, "bridges")
	deleteBridge := cache.GetMap("Bridge", "name", "TEST")

	// delete old bridge
	if deleteBridge != nil {
		newBridgeList := helpers.RemoveFromUUIDList(bridgeList, []string{deleteBridge["uuid"].(string)})

		txn := db.Transaction("Open_vSwitch")
		txn.Update(dbtransaction.Update{
			Table: "Open_vSwitch",
			Where: [][]interface{}{{"_uuid", "==", []string{"uuid", schemaId}}},
			Row: map[string]interface{}{
				"bridges": helpers.MakeSet(newBridgeList),
			},
			WaitRows: []interface{}{map[string]interface{}{
				"bridges": helpers.MakeSet(bridgeList),
			}},
		})
		txn.Commit()
	}

	bridgeList = cache.GetList("Open_vSwitch", "uuid", schemaId, "bridges")

	bridge := ovshelper.Bridge{
		Name: "TEST",
		FailMode: "standalone",
	}

	txn := db.Transaction("Open_vSwitch")
	bridgeTempId := txn.Insert(dbtransaction.Insert{
		Table: "Bridge",
		Row: bridge,
	})
	newBridgeList := helpers.AppendNamedUUIDToUUIDList(bridgeList, []string{bridgeTempId})
	txn.Update(dbtransaction.Update{
		Table: "Open_vSwitch",
		Where: [][]interface{}{{"_uuid", "==", []string{"uuid", schemaId}}},
		Row: map[string]interface{}{
			"bridges": helpers.MakeSet(newBridgeList),
		},
		WaitRows: []interface{}{map[string]interface{}{
			"bridges": helpers.MakeSet(bridgeList),
		}},
	})
	txn.Commit()

	someBridge := cache.GetMap("Bridge", "name", "TEST")
	if someBridge == nil {
		t.Error("Cache failed")
		return
	}
}


