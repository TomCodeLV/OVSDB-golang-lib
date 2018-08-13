package dbtransaction

import (
	"strconv"
	"encoding/json"
	"errors"
	)

type iOVSDB interface {
	Call(string, interface{}, *uint64) (json.RawMessage, error)
	Notify(string, interface{}) (error)
}

type UUID []string


type ActionResponse struct {
	Rows []interface{}		`json:"rows"`
	UUID UUID
	Error string
	Details string
}

type Transact []ActionResponse


// Transaction handle structure
type Transaction struct {
	OVSDB iOVSDB
	Schema string
	Actions []interface{}
	Tables map[string]string
	References map[string][]interface{}
	Counter int
	id uint64
}

func (txn *Transaction) Cancel() {
	args := []interface {}{txn.id}

	txn.OVSDB.Notify("cancel", args	)
}

func (txn *Transaction) Select(tableName string, columns []string, conditions [][]string) {
	action := map[string]interface{}{}

	action["op"] = "select"
	action["table"] = tableName
	action["where"] = conditions
	action["columns"] = columns

	txn.Actions = append(txn.Actions, action)
}

func (txn *Transaction) Insert(tableName string, item interface{}) string {
	action := map[string]interface{}{}

	tempId := "row" + strconv.Itoa(txn.Counter)
	txn.Counter++

	action["uuid-name"] = tempId
	action["row"] = item
	action["op"] = "insert"
	action["table"] = tableName

	txn.Actions = append(txn.Actions, action)

	return tempId
}

func (txn *Transaction) Update(tableName string, item interface{}) {
	action := map[string]interface{}{}

	action["op"] = "update"
	action["table"] = tableName
	action["where"] = []interface{}{}
	action["row"] = item

	txn.Actions = append(txn.Actions, action)
}

func (txn *Transaction) Mutate(tableName string, conditions [][]string, mutations [][]interface{}) {
	action := map[string]interface{}{}

	action["op"] = "mutate"
	action["table"] = tableName
	action["where"] = conditions
	action["mutations"] = mutations

	txn.Actions = append(txn.Actions, action)
}

func (txn *Transaction) Delete(tableName string, conditions [][]string) {
	action := map[string]interface{}{}

	action["op"] = "delete"
	action["table"] = tableName
	action["where"] = conditions

	txn.Actions = append(txn.Actions, action)
}

func (txn *Transaction) Wait(timeout uint64, tableName string,  conditions [][]interface{}, columns []string, until string, rows []interface{}) {
	action := map[string]interface{}{}

	action["op"] = "wait"
	action["timeout"] = timeout
	action["table"] = tableName
	action["where"] = conditions
	action["columns"] = columns
	action["until"] = until
	action["rows"] = rows

	txn.Actions = append(txn.Actions, action)
}

// Commit stores all staged changes in DB. It manages references in main table
// automatically.
func (txn *Transaction) Commit() (Transact, error) {
	args := []interface {}{txn.Schema}
	args = append(args, txn.Actions...)

	var id uint64
	response, _ := txn.OVSDB.Call("transact", args, &id)
	txn.id = id

	var t Transact
	json.Unmarshal(response, &t)

	// we have an error
	if len(t) > len(txn.Actions) {
		return nil, errors.New(t[len(t)-1].Error + ": " + t[len(t)-1].Details)
	}

	return t, nil
}
