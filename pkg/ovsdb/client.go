package ovsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbcache"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbmonitor"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbtransaction"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

type Lock struct {
	Locked bool
}

type Pending struct {
	channel  chan int
	response *json.RawMessage
	error *json.RawMessage
	connectionClosed bool
}

type callback func(interface{})

// ovsdb session handle structure
type OVSDB struct {
	Conn net.Conn
	ID string
	dec *json.Decoder
	enc *json.Encoder
	pending map[uint64]*Pending
	callbacks map[string]dbmonitor.Callback
	lockedCallback func(string)
	stolenCallback func(string)
	counter uint64
	synchronize *Synchronize
}

// Dial initiates ovsdb session
// It returns session handle and error if encountered
func Dial(network, address string) (*OVSDB, error) {
	ovsdb := new(OVSDB)
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	ovsdb.Conn = conn

	ovsdb.dec = json.NewDecoder(conn)
	ovsdb.enc = json.NewEncoder(conn)

	rand.Seed(time.Now().UnixNano())
	ovsdb.ID = "id" + strconv.FormatUint(rand.Uint64(), 10)

	ovsdb.pending = make(map[uint64]*Pending)
	ovsdb.callbacks = make(map[string]dbmonitor.Callback)
	ovsdb.counter = 0

	go ovsdb.loop()

	return ovsdb, nil
}

// helper structure for synchronizing db connection and socket reads and writes
type Synchronize struct {
	connected customCond
	initialized customCond
	socketError customCond
}

type customCond struct {
	sync.Mutex
	cond *sync.Cond
	val bool
}

func (p *Synchronize) init() {
	p.connected = customCond{}
	p.connected.cond = sync.NewCond(&p.connected)

	p.initialized = customCond{}
	p.initialized.cond = sync.NewCond(&p.initialized)

	p.socketError = customCond{}
	p.socketError.cond = sync.NewCond(&p.socketError)
}

// if not connected waits until connection is established
func (p *Synchronize) WaitConnected() bool {
	if !p.connected.val {
		p.connected.Lock()
		p.connected.cond.Wait()
		p.connected.Unlock()
		return true
	}
	return false
}

func (p *Synchronize) SetConnected() {
	p.connected.val = true
	p.socketError.val = false
	p.connected.cond.Broadcast()
}

// if not initialized waits until initialization callback is completed
func (p *Synchronize) WaitInitialized() {
	if !p.initialized.val {
		p.initialized.Lock()
		p.initialized.cond.Wait()
		p.initialized.Unlock()
	}
}

func (p *Synchronize) SetInitialized() {
	p.initialized.val = true
	p.initialized.cond.Broadcast()
}

// if there is no socket error, locks and waits until socket errors
func (p *Synchronize) WaitError() {
	if !p.socketError.val {
		p.socketError.Lock()
		p.socketError.cond.Wait()
		p.socketError.Unlock()
	}
}

func (p *Synchronize) SetError() {
	p.socketError.val = true
	p.connected.val = false
	p.initialized.val = false
	p.socketError.cond.Broadcast()
}

// PersistentDial provides automatic reconnection in case of connection failure.
// Reconnection will be performed with each provided address.
// After unsuccessfully trying all addresses it will sleep for 1,2,4,8,8,8,...
// seconds before trying again.
// Initialize will be called after every successful connection to db.
// Function will lock until first successful connect.
// Returns a pointer to db which will point to new db structure on each connect.
func PersistentDial(addressList [][]string, initialize func(*OVSDB) error) (**OVSDB) {
	var db *OVSDB
	var err	error
	synchronize := new(Synchronize)
	synchronize.init()

	idx := 0
	timeOut := 1

	go func() {
		for true {
			network := addressList[idx][0]
			address := addressList[idx][1]
			db, err = Dial(network, address)
			if err != nil {
				fmt.Println(err)
				idx = idx + 1
				if idx == len(addressList) {
					time.Sleep(time.Duration(timeOut) * time.Second)

					idx = 0
					if timeOut < 8 {
						timeOut = timeOut * 2
					}
				}
			} else {
				idx = 0
				timeOut = 1

				synchronize.SetConnected()

				db.synchronize = synchronize
				err = initialize(db)
				if err!= nil {
				} else {
					synchronize.SetInitialized()
					synchronize.WaitError()
				}
			}
		}
	}()

	// lock until initialize called
	synchronize.WaitInitialized()

	return &db
}

// closes ovsdb network connection
func (ovsdb *OVSDB) Close() error {
	resp := ovsdb.Conn.Close()
	// unlock all pending calls
	for _, val := range ovsdb.pending {
		val.connectionClosed = true
		val.channel <- 1
	}
	return resp
}

// incoming message header structure
// note that Result is stored in raw format
type message struct {
	Method string        		`json:"method"`
	Params []*json.RawMessage 	`json:"params"`
	Result *json.RawMessage		`json:"result"`
	Error  *json.RawMessage		`json:"error"`
	ID     interface{} 			`json:"id"`
}

type Error struct {
	Syntax string	`json:"syntax"`
	Details string	`json:"details"`
	Error string	`json:"error"`
}


func (ovsdb *OVSDB) encodeWrapper(v interface{}) error {
	err := ovsdb.enc.Encode(v)
	if err != nil {
		if ovsdb.synchronize != nil {
			ovsdb.Close()
			ovsdb.synchronize.SetError()
			if ovsdb.synchronize != nil { ovsdb.synchronize.WaitConnected() }
		}
		return err
	}
	return nil
}

func (ovsdb *OVSDB) decodeWrapper(v *message) error {
	err := ovsdb.dec.Decode(v)
	if err != nil {
		ovsdb.Close()
		if ovsdb.synchronize != nil {
			ovsdb.Close()
			ovsdb.synchronize.SetError()
			if ovsdb.synchronize != nil { ovsdb.synchronize.WaitConnected() }
		}
		return err
	}
	return nil
}

// loop is responsible for receiving all incoming messages
func (ovsdb *OVSDB) loop() error {
	for true {
		var msg message
		// receive incoming message and store in header structure
		if err := ovsdb.decodeWrapper(&msg); err != nil {
			return err
		}

		switch msg.Method {
		case "echo": // handle incoming echo messages
			resp := map[string]interface{}{
				"result": msg.Params,
				"error":  nil,
				"id":     "echo",
			}
			ovsdb.encodeWrapper(resp)
		case "update": // handle incoming update notification
			var id string
			json.Unmarshal(*msg.Params[0], &id)
			ovsdb.callbacks[id](*msg.Params[1])
		case "locked":
			if ovsdb.lockedCallback != nil {
				var resp string
				json.Unmarshal(*msg.Params[0], &resp)
				ovsdb.lockedCallback(resp)
			}
		case "stolen":
			if ovsdb.stolenCallback != nil {
				var resp string
				json.Unmarshal(*msg.Params[0], &resp)
				ovsdb.stolenCallback(resp)
			}
		default: // handle incoming response
			id := uint64(msg.ID.(float64))
			if msg.Error == nil {
				ovsdb.pending[id].response = msg.Result
			} else {
				ovsdb.pending[id].error = msg.Error
			}
			// unblock related call invocation
			ovsdb.pending[id].channel <- 1
		}
	}

	return nil
}

// call sends request to server and blocks
// after it is unblocked in incoming message receiver loop it returns response
// from server as raw data to be unmarshaled later
func (ovsdb *OVSDB) Call(method string, args interface{}, idref *uint64) (json.RawMessage, error) {
	if ovsdb.synchronize != nil && ovsdb.synchronize.WaitConnected() {
		return nil, errors.New("no connection")
	}

	id := ovsdb.GetCounter()
	if idref != nil {
		*idref = id
	}

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
	err := ovsdb.encodeWrapper(req)
	if err != nil {
		return nil, err
	}

	// block function
	<-ch

	if ovsdb.pending[id].connectionClosed {
		return nil, errors.New("connection closed")
	}

	// transaction error always is null, OVSDB errors for transactions are handled later
	if ovsdb.pending[id].error != nil {
		var err2 Error

		json.Unmarshal(*ovsdb.pending[id].error, &err2)

		delete(ovsdb.pending, id)

		return nil, errors.New(err2.Error + ": " + err2.Details + " (" + err2.Syntax + ")" )
	}

	response := ovsdb.pending[id].response
	delete(ovsdb.pending, id)

	return *response, nil
}

func (ovsdb *OVSDB) Notify(method string, args interface{}) error {
	req := map[string]interface{}{
		"method": method,
		"params": args,
		"id":     nil,
	}
	err := ovsdb.encodeWrapper(req)

	return err
}

func (ovsdb *OVSDB) AddCallBack(id string, callback dbmonitor.Callback) {
	ovsdb.callbacks[id] = callback
}

func (ovsdb *OVSDB) GetCounter() uint64 {
	counter := ovsdb.counter
	ovsdb.counter++
	return counter
}

// ListDbs returns list of databases
func (ovsdb *OVSDB) ListDbs() []string {
	response, _ := ovsdb.Call("list_dbs", []interface{}{}, nil)
	dbs := []string{}
	json.Unmarshal(response, &dbs)
	return dbs
}

// GetSchema returns schema object containing all db schema data
func (ovsdb *OVSDB) GetSchema(schema string) (json.RawMessage, error) {
	return ovsdb.Call("get_schema", []string{schema}, nil)
}

// ===================================
// ADVANCED FUNCTIONALITY CONSTRUCTORS
// ===================================

// Transaction returns transaction handle
func (ovsdb *OVSDB) Transaction(schema string) *dbtransaction.Transaction {
	txn := new(dbtransaction.Transaction)

	txn.OVSDB = ovsdb
	txn.Schema = schema
	txn.Tables = map[string]string{}
	txn.References = make(map[string][]interface{})
	txn.Counter = 1

	return txn
}

func (ovsdb *OVSDB) Monitor(schema string) *dbmonitor.Monitor {
	monitor := new(dbmonitor.Monitor)

	monitor.OVSDB = ovsdb
	monitor.Schema = schema
	monitor.MonitorRequests = make(map[string]interface{})

	return monitor
}

type Cache struct {
	Schema string
	Tables map[string][]string
	Indexes map[string][]string
}

func (ovsdb *OVSDB) Cache(c Cache) (*dbcache.Cache, error) {
	cache := new(dbcache.Cache)

	cache.OVSDB = ovsdb
	cache.Schema = c.Schema
	cache.Indexes = c.Indexes

	err := cache.StartMonitor(c.Schema, c.Tables)
	if err != nil {
		return nil, err
	}

	return cache, nil
}

// =======
// LOCKING
// =======

func (ovsdb *OVSDB) RegisterLockedCallback(Callback func(string)) {
	ovsdb.lockedCallback = Callback
}

func (ovsdb *OVSDB) RegisterStolenCallback(Callback func(string)) {
	ovsdb.stolenCallback = Callback
}

func (ovsdb *OVSDB) Lock(id string) (interface{}, error) {
	response, err := ovsdb.Call("lock", []string{id}, nil)
	lock := Lock{}
	json.Unmarshal(response, &lock)
	return lock, err
}

func (ovsdb *OVSDB) Steal(id string) (interface{}, error) {
	response, err := ovsdb.Call("steal", []string{id}, nil)
	lock := Lock{}
	json.Unmarshal(response, &lock)
	return lock, err
}

func (ovsdb *OVSDB) Unlock(id string) (interface{}, error) {
	response, err := ovsdb.Call("unlock", []string{id}, nil)
	lock := Lock{}
	json.Unmarshal(response, &lock)
	return lock, err
}


