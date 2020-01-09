package ovsdb

import (
	"testing"

	"github.com/Microsoft/go-winio"
)

func TestPipeDialNet(t *testing.T) {
	network := "winpipe"
	address := `\\.\pipe\ovsdbclienttestpipe`
	l, err := winio.ListenPipe(address, nil)
	if err != nil {
		t.Error(err)
	}
	defer l.Close()
	go func() {
		_, err = l.Accept()
	}()
	_, err = DialNet(network, address)
	if err != nil {
		t.Error(err)
	}
}
