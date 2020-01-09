package ovsdb

import (
	"github.com/Microsoft/go-winio"
	"net"
)

// DialNet supports connect to named pipe by specifying network as "winpipe".
func DialNet(network, address string) (net.Conn, error) {
	if network == "winpipe" {
		return winio.DialPipe(address, nil)
	}
	return net.Dial(network, address)
}
