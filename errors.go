/*


*/

package goh

import (
	"fmt"
	"github.com/sdming/goh/Hbase"
)

/*
HbaseError
*/
type HbaseError struct {
	IOErr  *IOError         // IOError
	ArgErr *IllegalArgument // IllegalArgument
	Err    error            // error

}

func newHbaseError(ioError *Hbase.IOError, argError *Hbase.IllegalArgument, err error) *HbaseError {
	var io *IOError

	if ioError != nil {
		io = &IOError{
			Message: ioError.Message,
		}
	}

	var arg *IllegalArgument
	if argError != nil {
		arg = &IllegalArgument{
			Message: argError.Message,
		}
	}

	hError := &HbaseError{
		IOErr:  io,
		ArgErr: arg,
		Err:    err,
	}
	return hError
}

/*
String
*/
func (hError *HbaseError) String() string {
	if hError == nil {
		return "<nil>"
	}

	return fmt.Sprintf("IOError:%v; ArgError:%v; Error:%v;", hError.IOErr, hError.ArgErr, hError.Err)
}

/*
Error
*/
func (hError *HbaseError) Error() string {
	return hError.String()
}

func checkHbaseError(ioError *Hbase.IOError, err error) error {
	if ioError != nil || err != nil {
		return newHbaseError(ioError, nil, err)
	}
	return nil
}

func checkHbaseArgError(ioError *Hbase.IOError, argError *Hbase.IllegalArgument, err error) error {
	if ioError != nil || argError != nil || err != nil {
		return newHbaseError(ioError, argError, err)
	}
	return nil
}
