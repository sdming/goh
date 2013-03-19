/*


*/

package goh

import (
	"bytes"
	"github.com/sdming/goh/Hbase"
)

/*
HbaseError
*/
type HbaseError struct {
	IOErr  *Hbase.IOError         // IOError
	ArgErr *Hbase.IllegalArgument // IllegalArgument
	Err    error                  // error

}

func newHbaseError(io *Hbase.IOError, arg *Hbase.IllegalArgument, err error) *HbaseError {
	return &HbaseError{
		IOErr:  io,
		ArgErr: arg,
		Err:    err,
	}
}

/*
String
*/
func (e *HbaseError) String() string {
	if e == nil {
		return "<nil>"
	}

	var b bytes.Buffer
	if e.IOErr != nil {
		b.WriteString("IOError:")
		b.WriteString(e.IOErr.Message)
		b.WriteString(";")
	}

	if e.ArgErr != nil {
		b.WriteString("ArgumentError:")
		b.WriteString(e.ArgErr.Message)
		b.WriteString(";")
	}

	if e.Err != nil {
		b.WriteString("Error:")
		b.WriteString(e.Err.Error())
		b.WriteString(";")
	}
	return b.String()
}

/*
Error
*/
func (e *HbaseError) Error() string {
	return e.String()
}

func checkHbaseError(io *Hbase.IOError, err error) error {
	if io != nil || err != nil {
		return newHbaseError(io, nil, err)
	}
	return nil
}

func checkHbaseArgError(io *Hbase.IOError, arg *Hbase.IllegalArgument, err error) error {
	if io != nil || arg != nil || err != nil {
		return newHbaseError(io, arg, err)
	}
	return nil
}
