package ovsdb

import (
	"net"
	"fmt"
	//"github.com/pborman/uuid"
	"strconv"
	"math/rand"
	"time"
	"encoding/json"
	"./responses"
	"errors"
)

type Pending struct {
	channel  chan int
	response *json.RawMessage
}

// ovsdb session handle structure
type OVSDB struct {
	Conn net.Conn
	ID string
	dec *json.Decoder
	enc *json.Encoder
	pending map[uint64]*Pending
	counter uint64
}

// Dial initiates ovsdb session
// It returns session handle and error if encountered
func Dial(network, address string) (*OVSDB, error) {
	ovsdb := new(OVSDB)
	conn, err := net.Dial(network, address)
	ovsdb.Conn = conn

	ovsdb.dec = json.NewDecoder(conn)
	ovsdb.enc = json.NewEncoder(conn)

	rand.Seed(time.Now().UnixNano())
	ovsdb.ID = "id" + strconv.FormatUint(rand.Uint64(), 10)

	ovsdb.pending = make(map[uint64]*Pending)
	ovsdb.counter = 0

	go ovsdb.loop()

	return ovsdb, err
}

// closes ovsdb network connection
func (ovsdb *OVSDB) Close() error {
	return ovsdb.Conn.Close()
}

// incoming message header structure
// note that Result is stored in raw format
type message struct {
	Method string        	`json:"method"`
	Params []interface{} 	`json:"params"`
	Result *json.RawMessage	`json:"result"`
	ID     interface{} 		`json:"id"`
}

// loop is responsible for receiving all incoming messages
func (ovsdb *OVSDB) loop() error {
	for true {
		var msg message
		// receive incoming message and store in header structure
		if err := ovsdb.dec.Decode(&msg); err != nil {
			return err
		}


		switch msg.Method {
		case "echo": // handle incoming echo messages
			resp := map[string]interface{}{
				"result": msg.Params,
				"error":  nil,
				"id":     "echo",
			}
			ovsdb.enc.Encode(resp)
		case "update": // handle incoming update notification
			fmt.Println("update: ")
			fmt.Println(msg.Params)
		default: // handle incoming response
			id := uint64(msg.ID.(float64))
			ovsdb.pending[id].response = msg.Result
			// unblock related call invocation
			ovsdb.pending[id].channel <- 1
		}
	}

	return nil
}

// call sends request to server and blocks
// after it is unblocked in incoming message receiver loop it returns response
// from server as raw data to be unmarshaled later
func (ovsdb *OVSDB) call(method string, args interface{}) (json.RawMessage, error) {
	id := ovsdb.counter
	ovsdb.counter++

	// create RPC request
	req := map[string]interface{}{
		"method": method,
		"params": args,
		"id":     id,
	}

	ch := make(chan int, 1)

	// store channel in list to pass it to receiver loop
	ovsdb.pending[id] = &Pending{
		channel:  ch,
	}

	// send message
	err := ovsdb.enc.Encode(req)

	// block function
	<-ch

	response := ovsdb.pending[id].response
	delete(ovsdb.pending, id)

	return *response, err
}

// ListDbs returns list of databases
func (ovsdb *OVSDB) ListDbs() responses.DbList {
	response, _ := ovsdb.call("list_dbs", []interface{}{})
	dbs := responses.DbList{}
	json.Unmarshal(response, &dbs)
	return dbs
}

// GetSchema returns schema object containing all db schema data
func (ovsdb *OVSDB) GetSchema(schema string) responses.Schema {
	response, _ := ovsdb.call("get_schema", []string{schema})
	sc := responses.Schema{}
	json.Unmarshal(response, &sc)
	return sc
}

// Transact returns transaction handle
func (ovsdb *OVSDB) Transact(schema string) (*Transaction) {
	txn := new(Transaction)

	txn.OVSDB = ovsdb
	txn.schema = schema
	txn.Tables = map[string]bool{}
	txn.References = make(map[string][]interface{})
	txn.counter = 1

	return txn
}

// Transaction handle structure
type Transaction struct {
	OVSDB *OVSDB
	schema string
	Actions []interface{}
	Tables map[string]bool
	References map[string][]interface{}
	counter int
}

// Insert adds new entity
func (txn *Transaction) Insert(item interface{}) {
	action := map[string]interface{}{}

	action["uuid-name"] = "row" + strconv.Itoa(txn.counter)
	txn.counter++

	action["row"] = map[string]interface{} {
		"name": item.(Bridge).Name,
		"fail_mode": "secure",
	}
	action["op"] = "insert"

	switch v := item.(type) {
	case Bridge:
		txn.Tables["Bridge"] = true
		action["table"] = "Bridge"
		txn.References["Bridge"] = append(txn.References["Bridge"], []string{"named-uuid", action["uuid-name"].(string)})
	default:
		fmt.Printf("I don't know about type %T!\n", v)
	}

	txn.Actions = append(txn.Actions, action)
}


// Commit stores all staged changes in DB. It manages references in main table
// automatically.
func (txn *Transaction) Commit() (interface{}, error) {
	txn.loadReferences()

	args := []interface {}{txn.schema}
	args = append(args, txn.Actions...)

	// stored references in main table
	action := map[string]interface{}{}
	action["op"] = "update"
	action["row"] = map[string]interface{}{
		"bridges": []interface{}{
			"set",
			txn.References["Bridge"],
		},
	}
	action["table"] = txn.schema
	action["where"] = []interface{}{}

	args = append(args, action)

	response, _ := txn.OVSDB.call("transact", args)

	var t responses.Transact
	json.Unmarshal(response, &t)

	// we have an error
	if len(t) - 1 > len(txn.Actions) {
		return nil, errors.New(t[len(t)-1].Error + ": " + t[len(t)-1].Details)
	}

	ret := make([]interface{}, 0)

	for i := 0; i < len(txn.Actions); i++ {
		if (txn.Actions[i].(map[string]interface{})["op"] == "insert") {
			ret = append(ret, t[i].UUID)
		} else {
			ret = append(ret, nil)
		}
	}

	return ret, nil
}

// loadReferences is a helper function for Commit. It pulls all required
// references.
func (txn *Transaction) loadReferences() error {
	args := []interface {}{txn.schema}

	tableList := make([]string, len(txn.Tables))
	idx := 0
	for table, _ := range txn.Tables {
		tableList[idx] = table
		idx++
	}

	for _, table := range tableList {
		action := map[string]interface{}{}
		action["op"] = "select"
		action["table"] = table
		action["where"] = []interface{}{}
		action["columns"] = []string{"name", "_uuid"}

		args = append(args, action)
	}

	response, _ := txn.OVSDB.call("transact", args)

	var t responses.Transact
	json.Unmarshal(response, &t)

	for idx, table := range tableList {
		for _, row := range t[idx].Rows {
			txn.References[table] = append(txn.References[table], []string{"uuid", row.UUID[1]})
		}
	}

	return nil
}


