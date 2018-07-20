package main

import (
	"fmt"
	"../ovsdb"
)

func main() {
	fmt.Println("hi")

	db, err := ovsdb.Dial("tcp", ":12346") //db, err := ovsdb.Dial("unix", "/run/openvswitch/db.sock")
	if err != nil {
		fmt.Println("unable to dial: " + err.Error())
		return
	}

	fmt.Println(db.ListDbs())

	schema := db.GetSchema("Open_vSwitch")
	fmt.Println(schema.Tables["Bridge"])

	txn := db.Transact("Open_vSwitch")

	txn.Insert(ovsdb.Bridge{Name: "Peter"})


	res, err := txn.Commit()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}

	for true {

	}

	fmt.Println(db.Close())
}
