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

type Select struct {
	Table string
	Columns []string
	Where [][]string
}

func (txn *Transaction) Select(s Select) {
	action := map[string]interface{}{}

	action["op"] = "select"
	action["table"] = s.Table
	action["where"] = s.Where
	action["columns"] = s.Columns

	txn.Actions = append(txn.Actions, action)
}

type Insert struct {
	Table string
	Row interface{}
}

func (txn *Transaction) Insert(i Insert) string {
	action := map[string]interface{}{}

	tempId := "row" + strconv.Itoa(txn.Counter)
	txn.Counter++

	action["uuid-name"] = tempId
	action["row"] = i.Row
	action["op"] = "insert"
	action["table"] = i.Table

	txn.Actions = append(txn.Actions, action)

	return tempId
}

type Update struct {
	Table string
	Where [][]interface{}
	Row map[string]interface{}
	WaitRows []interface{}
}

func (txn *Transaction) Update(u Update) {
	if u.WaitRows != nil {
		columns := make([]string, len(u.Row))
		c := 0
		for column, _ := range u.Row {
			columns[c] = column
			c++
		}

		txn.Wait(Wait{
			Table: u.Table,
			Where: u.Where,
			Columns: columns,
			Until: "==",
			Rows: u.WaitRows,
		})
	}

	action := map[string]interface{}{}

	action["op"] = "update"
	action["table"] = u.Table
	action["where"] = u.Where
	action["row"] = u.Row

	txn.Actions = append(txn.Actions, action)
}

type Mutate struct {
	Table string
	Where [][]string
	Mutations [][]interface{}
}

func (txn *Transaction) Mutate(m Mutate) {
	action := map[string]interface{}{}

	action["op"] = "mutate"
	action["table"] = m.Table
	action["where"] = m.Where
	action["mutations"] = m.Mutations

	txn.Actions = append(txn.Actions, action)
}

type Delete struct {
	Table string
	Where [][]string
}

func (txn *Transaction) Delete(d Delete) {
	action := map[string]interface{}{}

	action["op"] = "delete"
	action["table"] = d.Table
	action["where"] = d.Where

	txn.Actions = append(txn.Actions, action)
}

type Wait struct {
	Timeout uint64
	Table string
	Where [][]interface{}
	Columns []string
	Until string
	Rows []interface{}
}

func (txn *Transaction) Wait(w Wait) {
	action := map[string]interface{}{}

	action["op"] = "wait"
	action["timeout"] = w.Timeout
	action["table"] = w.Table
	action["where"] = w.Where
	action["columns"] = w.Columns
	action["until"] = w.Until
	action["rows"] = w.Rows

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
