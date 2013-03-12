/*


*/

package goh

import (
	"errors"
	"fmt"
	_ "github.com/sdming/goh/Hbase"
	"thrift"
)

/*
state
*/
const (
	stateDefault = iota // default
	stateOpen           // opened
)

/*
Protocol
*/
const (
	TBinaryProtocol     = iota //"binary"
	TCompactProtocol           // "compact"
	TDebugProtocol             // "debug"
	TDenseProtocol             // "debug"
	TJSONProtocol              // "json"
	TSimpleJSONProtocol        // "simplejson"
)

/*
Transport
*/
const (
	TFileTransport   = iota // "binary"
	TFramedTransport        // "compact"
	TMemoryTransport        // "json"
	TSocket                 // "simplejson"
	TZlibTransport          // "debug"
)

/*
Server
*/
const (
	TNonblockingServer = iota
	TSimpleServer
	TThreadPoolServer
)

func newProtocolFactory(protocol int) (thrift.TProtocolFactory, error) {
	switch protocol {
	case TBinaryProtocol:
		return thrift.NewTBinaryProtocolFactoryDefault(), nil
	case TCompactProtocol:
		return thrift.NewTCompactProtocolFactory(), nil
	case TJSONProtocol:
		return thrift.NewTJSONProtocolFactory(), nil
	case TSimpleJSONProtocol:
		return thrift.NewTSimpleJSONProtocolFactory(), nil
	}

	return nil, errors.New(fmt.Sprint("invalid protocol:", protocol))
}
