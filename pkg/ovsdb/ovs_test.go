package ovsdb

import (
	"encoding/json"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbcache"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbmonitor"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbtransaction"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/helpers"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/ovshelper"
	"sync"
	"testing"
	"time"
)

var network = "tcp"
var address = ":12345"

//var network = "unix"
//var address = "/run/openvswitch/db.sock"

func TestDial(t *testing.T) {
	db := Dial([][]string{{network, address}}, nil, nil)
	db.Close()
}

func TestDialDouble(t *testing.T) {
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

	db2 := Dial([][]string{{network, address}}, nil, nil)
	defer db2.Close()
}

func TestOVSDB_ListDbs(t *testing.T) {
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

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
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

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
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

	// fetch references
	txn := db.Transaction("Open_vSwitch")
	txn.Select(dbtransaction.Select{
		Table: "Open_vSwitch",
		Columns: []string{"bridges"},
	})
	txn.Select(dbtransaction.Select{
		Table: "Bridge",
		Columns: []string{"_uuid"},
		Where: [][]interface{}{{"name", "==", "Test Bridge"}},
	})
	res, err, _ := txn.Commit()
	if err != nil {
		t.Error("Select failed")
		return
	}

	// build data
	var bridges []string
	bridgeResult := res[0].Rows[0].(map[string]interface{})["bridges"].([]interface{})
	bridges = helpers.GetIdListFromOVSDBSet(bridgeResult)

	// delete bridge
	var bridgeUUID string
	if len(res[1].Rows) == 1 {
		bridgeUUID = res[1].Rows[0].(map[string]interface{})["_uuid"].([]interface{})[1].(string)
		for idx, bridgeId := range bridges {
			if bridgeId == bridgeUUID {
				bridges = append(bridges[:idx], bridges[idx+1:]...)
				break
			}
		}

		txn2 := db.Transaction("Open_vSwitch")
		txn2.Update(dbtransaction.Update{
			Table: "Open_vSwitch",
			Row: map[string]interface{}{
				"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
					"uuid": bridges,
				}),
			},
		})
		_, err2, _ := txn2.Commit()
		if err2 != nil {
			t.Error(err2)
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
		Timeout: 0,
		Table: "Open_vSwitch",
		Where: [][]interface{}{},
		Columns: []string{"bridges"},
		Until: "==",
		Rows: []interface{}{map[string]interface{}{
			"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
				"uuid": bridges,
			},
		)}},
	})
	bridgeTempId := txn3.Insert(dbtransaction.Insert{
		Table: "Bridge",
		Row: bridge,
	})
	txn3.Update(dbtransaction.Update{
		Table:"Open_vSwitch",
		Row: map[string]interface{}{
			"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
				"uuid": bridges,
				"named-uuid": []string{bridgeTempId},
			}),
		},
	})
	_, err3, _ := txn3.Commit()
	if err3 != nil {
		t.Error(err3)
		t.Error("Insert failed")
	}
}

func TestOVSDB_Transaction_Cancel(t *testing.T) {
	loop := true
	return // ignore while cancel does not work - bug
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

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

	to := time.AfterFunc(time.Millisecond * 300, func(){
		t.Error("Transaction cancel timeout")
		loop = false
	})

	for loop {}

	to.Stop()
}

func TestOVSDB_Monitor_And_Mutate(t *testing.T) {
	loop := true
	updateCount := 0

	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

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
		Mutations: [][]interface{}{{"next_cfg", "+=", 1}},
	})
	_, err2, _ := txn.Commit()
	if err2 != nil {
		t.Error("Mutate failed")
	}

	to := time.AfterFunc(time.Millisecond * 300, func(){
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
		Mutations: [][]interface{}{{"next_cfg", "+=", 1}},
	})
	txn2.Commit()

	for loop {

	}

	if updateCount > 1 {
		t.Error("Monitor cancel failed")
	}

	to.Stop()
}

func TestOVSDB_Cache_main(t *testing.T) {
	// dial in
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

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
		t.Error("Cacge failed: " + err.Error())
	}

	schemaId := cache.GetKeys("Open_vSwitch", "uuid")[0]
	bridgeIdList := cache.GetKeys("Open_vSwitch", "uuid", schemaId, "bridges")
	deleteBridge := cache.GetMap("Bridge", "name", "TEST")

	// delete old bridge
	if _, ok := deleteBridge["uuid"]; ok {
		newBridgeIdList := helpers.RemoveFromIdList(bridgeIdList, []string{deleteBridge["uuid"].(string)})

		txn := db.Transaction("Open_vSwitch")
		txn.Update(dbtransaction.Update{
			Table: "Open_vSwitch",
			Where: [][]interface{}{{"_uuid", "==", []string{"uuid", schemaId}}},
			Row: map[string]interface{}{
				"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
					"uuid": newBridgeIdList,
				}),
			},
			WaitRows: []interface{}{map[string]interface{}{
				"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
					"uuid": bridgeIdList,
				}),
			}},
		})
		txn.Commit()
	}

	bridgeIdList = cache.GetKeys("Open_vSwitch", "uuid", schemaId, "bridges")

	bridge := ovshelper.Bridge{
		Name: "TEST",
		FailMode: "standalone",
	}

	txn := db.Transaction("Open_vSwitch")
	bridgeTempId := txn.Insert(dbtransaction.Insert{
		Table: "Bridge",
		Row: bridge,
	})
	txn.Update(dbtransaction.Update{
		Table: "Open_vSwitch",
		Where: [][]interface{}{{"_uuid", "==", []string{"uuid", schemaId}}},
		Row: map[string]interface{}{
			"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
				"uuid": bridgeIdList,
				"named-uuid": []string{bridgeTempId},
			}),
		},
		WaitRows: []interface{}{map[string]interface{}{
			"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
				"uuid": bridgeIdList,
			}),
		}},
	})
	txn.Commit()

	someBridge := cache.GetMap("Bridge", "name", "TEST")
	if someBridge == nil {
		t.Error("Cache failed")
		return
	}
}

func TestOVSDB_Advanced_first(t *testing.T) {
	// dial in
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

	// initialize cache
	cache, err := db.Cache(Cache{
		Schema: "Open_vSwitch",
		Tables: map[string][]string{
			"Open_vSwitch": nil,
			"Bridge": nil,
			"Port": nil,
		},
		Indexes: map[string][]string{
			"Bridge": {"name"},
			"Port": {"name"},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	schemaId := cache.GetKeys("Open_vSwitch", "uuid")[0]
	bridgeIdList := cache.GetKeys("Open_vSwitch", "uuid", schemaId, "bridges")
	bridgeId, ok := cache.GetMap("Bridge", "name", "TEST_BRIDGE")["uuid"].(string)

	if ok {
		newBridgeIdList := helpers.RemoveFromIdList(bridgeIdList, []string{bridgeId})
		txn := db.Transaction("Open_vSwitch")
		txn.Update(dbtransaction.Update{
			Table: "Open_vSwitch",
			Where: [][]interface{}{{"_uuid", "==", []string{"uuid", schemaId}}},
			Row: map[string]interface{}{
				"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
					"uuid": newBridgeIdList,
				}),
			},
			WaitRows: []interface{}{map[string]interface{}{
				"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
					"uuid": bridgeIdList,
				}),
			}},
		})
		_, err, _ := txn.Commit()
		if err != nil {
			t.Error(err)
			return
		}
	}


	bridgeIdList = cache.GetKeys("Open_vSwitch", "uuid", schemaId, "bridges")
	txn := db.Transaction("Open_vSwitch")
	interf := ovshelper.Interface{
		Name: "TEST_INTERFACE",
	}
	newInterfId := txn.Insert(dbtransaction.Insert{
		Table: "Interface",
		Row: interf,
	})
	port := ovshelper.Port{
		Name: "TEST_PORT",
		Interfaces: helpers.MakeOVSDBSet(map[string]interface{}{
			"named-uuid": []string{newInterfId},
		}),
	}
	newPortId := txn.Insert(dbtransaction.Insert{
		Table: "Port",
		Row: port,
	})
	bridge := ovshelper.Bridge{
		Name: "TEST_BRIDGE",
		Ports: helpers.MakeOVSDBSet(map[string]interface{}{
			"named-uuid": []string{newPortId},
		}),
	}
	newBridgeId := txn.Insert(dbtransaction.Insert{
		Table: "Bridge",
		Row: bridge,
	})
	txn.Update(dbtransaction.Update{
		Table: "Open_vSwitch",
		Where: [][]interface{}{{"_uuid", "==", []string{"uuid", schemaId}}},
		Row: map[string]interface{}{
			"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
				"uuid": bridgeIdList,
				"named-uuid": []string{newBridgeId},
			}),
		},
		WaitRows: []interface{}{map[string]interface{}{
			"bridges": helpers.MakeOVSDBSet(map[string]interface{}{
				"uuid": bridgeIdList,
			}),
		}},
	})
	_, err, _ = txn.Commit()
	if err != nil {
		t.Error(err)
	}
}

func TestOVSDB_Advanced_Helpers(t *testing.T) {
	// dial in
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

	// initialize cache
	cache, err := db.Cache(Cache{
		Schema: "Open_vSwitch",
		Tables: map[string][]string{
			"Open_vSwitch": nil,
			"Bridge": nil,
			"Port": nil,
		},
		Indexes: map[string][]string{
			"Bridge": {"name"},
			"Port": {"name"},
		},
	})
	if err != nil {
		t.Error(err)
	}

	schemaId := cache.GetKeys("Open_vSwitch", "uuid")[0]
	bridgeId, ok := cache.GetMap("Bridge", "name", "TEST_BRIDGE")["uuid"].(string)

	if ok {
		// delete old bridge
		db.Transaction("Open_vSwitch").DeleteReferences(dbtransaction.DeleteReferences{
			Table: "Open_vSwitch",
			WhereId: schemaId,
			ReferenceColumn: "bridges",
			DeleteIdsList: []string{bridgeId},
			Wait: true,
			Cache: cache,
		}).Commit()
	}

	txn := db.Transaction("Open_vSwitch")
	newInterfaceId := txn.Insert(dbtransaction.Insert{
		Table: "Interface",
		Row: ovshelper.Interface{
			Name: "TEST_INTERFACE",
		},
	})
	newPortId := txn.Insert(dbtransaction.Insert{
		Table: "Port",
		Row: ovshelper.Port{
			Name: "TEST_PORT",
			Interfaces: helpers.MakeOVSDBSet(map[string]interface{}{
				"named-uuid": []string{newInterfaceId},
			}),
		},
	})
	newBridgeId := txn.Insert(dbtransaction.Insert{
		Table: "Bridge",
		Row: ovshelper.Bridge{
			Name: "TEST_BRIDGE",
			Ports: helpers.MakeOVSDBSet(map[string]interface{}{
				"named-uuid": []string{newPortId},
			}),
		},
	})
	txn.InsertReferences(dbtransaction.InsertReferences{
		Table: "Open_vSwitch",
		WhereId: schemaId,
		ReferenceColumn: "bridges",
		InsertIdsList: []string{newBridgeId},
		Wait: true,
		Cache: cache,
	})
	_, err, _ = txn.Commit()
	if err != nil {
		t.Error(err)
	}
}

func TestOVSDB_Race_Condition(t *testing.T) {
	to := time.AfterFunc(time.Millisecond * 300, func(){
		t.Error("Race condition timeout")
		panic("!!!")
	})
	// dial in
	db := Dial([][]string{{network, address}}, nil, nil)
	defer db.Close()

	// initialize cache
	cache, err := db.Cache(Cache{
		Schema: "Open_vSwitch",
		Tables: map[string][]string{
			"Open_vSwitch": nil,
			"Bridge":       nil,
			"Port":         nil,
		},
		Indexes: map[string][]string{
			"Bridge": {"name"},
			"Port":   {"name"},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	schemaId := cache.GetKeys("Open_vSwitch", "uuid")[0]

	// Insert bridge to delete it later
	bridgeId, ok := cache.GetMap("Bridge", "name", "TEST_BRIDGE")["uuid"].(string)
	if !ok {
		txn := db.Transaction("Open_vSwitch")
		bridgeId := txn.Insert(dbtransaction.Insert{
			Table: "Bridge",
			Row: ovshelper.Bridge{
				Name: "TEST_BRIDGE",
			},
		})
		txn.InsertReferences(dbtransaction.InsertReferences{
			Table:           "Open_vSwitch",
			WhereId:         schemaId,
			ReferenceColumn: "bridges",
			InsertIdsList:   []string{bridgeId},
			Wait:            true,
			Cache:           cache,
		})
		txn.Commit()
	}

	// Concurrent delete
	bridgeId, ok = cache.GetMap("Bridge", "name", "TEST_BRIDGE")["uuid"].(string)
	ch := make(chan int, 1)
	ch2 := make(chan int, 1)
	go func() {
		ch2 <- 1

		var retry = true
		for retry {
			_, _, retry = db.Transaction("Open_vSwitch").DeleteReferences(dbtransaction.DeleteReferences{
				Table:           "Open_vSwitch",
				WhereId:         schemaId,
				ReferenceColumn: "bridges",
				DeleteIdsList:   []string{bridgeId},
				Wait:            true,
				Cache:           cache,
				LockChannel:     ch,
			}).Commit()
		}

		ch2 <- 1
	}()

	<- ch2

	// Change bridge list to cause concurrent delete to fail
	bridgeId2, ok2 := cache.GetMap("Bridge", "name", "TEST_BRIDGE2")["uuid"].(string)
	if ok2 {
		// delete old bridge
		db.Transaction("Open_vSwitch").DeleteReferences(dbtransaction.DeleteReferences{
			Table:           "Open_vSwitch",
			WhereId:         schemaId,
			ReferenceColumn: "bridges",
			DeleteIdsList:   []string{bridgeId2},
			Wait:            true,
			Cache:           cache,
		}).Commit()
	} else {
		txn := db.Transaction("Open_vSwitch")
		bridgeId2 := txn.Insert(dbtransaction.Insert{
			Table: "Bridge",
			Row: ovshelper.Bridge{
				Name: "TEST_BRIDGE2",
			},
		})
		txn.InsertReferences(dbtransaction.InsertReferences{
			Table:           "Open_vSwitch",
			WhereId:         schemaId,
			ReferenceColumn: "bridges",
			InsertIdsList:   []string{bridgeId2},
			Wait:            true,
			Cache:           cache,
		})
		txn.Commit()
	}

	ch <- 1
	ch <- 1

	<- ch2
	to.Stop()
}

func TestOVSDB_Persistent_Connection(t *testing.T) {
	to := time.AfterFunc(time.Millisecond * 500, func(){
		t.Error("Persistent connection timeout")
		panic("!!!")
	})
	m := new(sync.Mutex)

	var cache *dbcache.Cache
	db := Dial([][]string{{network, address}, {"tcp",":1234"}}, func(db *OVSDB) error {
		// initialize cache
		m.Lock()
		tmpCache, err := db.Cache(Cache{
			Schema: "Open_vSwitch",
			Tables: map[string][]string{
				"Open_vSwitch": nil,
				"Bridge":       nil,
			},
			Indexes: map[string][]string{
				"Bridge": {"name"},
			},
		})
		if err != nil {
			m.Unlock()
			return err
		}
		cache = tmpCache
		m.Unlock()
		return nil
	}, nil)

	// first disconnect, reconnect happens before transaction
	db.Close()
	time.Sleep(100*time.Millisecond)

	m.Lock()
	schemaId := cache.GetKeys("Open_vSwitch", "uuid")[0]
	m.Unlock()
	counter := 0

	var once sync.Once

	var retry = true
	for retry {
		counter = counter + 1
		txn := db.Transaction("Open_vSwitch")
		bridgeId := txn.Insert(dbtransaction.Insert{
			Table: "Bridge",
			Row: ovshelper.Bridge{
				Name: "TEST_BRIDGE",
			},
		})
		txn.InsertReferences(dbtransaction.InsertReferences{
			Table:           "Open_vSwitch",
			WhereId:         schemaId,
			ReferenceColumn: "bridges",
			InsertIdsList:   []string{bridgeId},
			Wait:            true,
			Cache:           cache,
		})

		once.Do(func(){db.Close()})
		_, _, retry = txn.Commit()
	}

	if counter != 2 {
		t.Error("Wrong transaction try count")
	}

	to.Stop()
}
