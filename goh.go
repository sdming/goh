/*


*/

package goh

import (
	"errors"
	"fmt"
	"github.com/sdming/goh/Hbase"
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

/*
HbaseError
*/
type HbaseError struct {
	IOError *Hbase.IOError // IOError
	Errors  []error        // other errors
}

func newHbaseError(ioError *Hbase.IOError, errors ...error) *HbaseError {
	he := &HbaseError{
		IOError: ioError,
		Errors:  errors[:],
	}
	return he
}

/*
String
*/
func (he *HbaseError) String() string {
	if he == nil {
		return "<nil>"
	}

	if he.IOError == nil && he.Errors == nil {
		return ""
	} else if he.Errors == nil {
		return he.IOError.String()
	} else if he.IOError == nil {
		return fmt.Sprint(he.Errors)
	}

	return fmt.Sprint(he.IOError, " ", he.Errors)
}

/*
Error
*/
func (he *HbaseError) Error() string {
	return he.String()
}

func checkHbaseError(ioError *Hbase.IOError, err error) error {
	if ioError != nil || err != nil {
		return newHbaseError(ioError, err)
	}
	return nil
}
