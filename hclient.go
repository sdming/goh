package goh

import (
	"github.com/sdming/goh/Hbase"
	"github.com/sdming/goh/thrift" // will replace it later
	"net"
	"net/url"
	//"thrift"
)

/*
HClient is wrap of hbase client
*/
type HClient struct {
	//Host            string
	//Port            int
	addr            string
	Protocol        int
	Trans           thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
	hbase           *Hbase.HbaseClient
	state           int //
}

/*
NewHttpClient return a hbase http client instance

*/
func NewHttpClient(rawurl string, protocol int) (client *HClient, err error) {
	parsedUrl, err := url.Parse(rawurl)
	if err != nil {
		return
	}

	trans, err := thrift.NewTHttpClient(parsedUrl.String())
	if err != nil {
		return
	}

	return newClient(parsedUrl.String(), protocol, trans)
}

/*
NewTcpClient return a base tcp client instance

*/
func NewTcpClient(rawaddr string, protocol int, framed bool) (client *HClient, err error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", rawaddr)
	if err != nil {
		return
	}

	var trans thrift.TTransport
	trans, err = thrift.NewTNonblockingSocketAddr(tcpAddr)
	if err != nil {
		return
	}
	if framed {
		trans = thrift.NewTFramedTransport(trans)
	}

	return newClient(tcpAddr.String(), protocol, trans)
}

/*
newClient create a new hbase client 
*/
func newClient(addr string, protocol int, trans thrift.TTransport) (*HClient, error) {
	var client *HClient

	protocolFactory, err := newProtocolFactory(protocol)
	if err != nil {
		return client, err
	}

	client = &HClient{
		addr:            addr,
		Protocol:        protocol,
		ProtocolFactory: protocolFactory,
		Trans:           trans,
		hbase:           Hbase.NewHbaseClientFactory(trans, protocolFactory),
	}

	// if err = client.Open(); err != nil {
	// 	return nil, err
	// }

	return client, nil
}

/*
Open connection
*/
func (client *HClient) Open() error {
	if client.state == stateDefault {
		if err := client.Trans.Open(); err != nil {
			return err
		}
		client.state = stateOpen
	}
	return nil
}

/*
Close connection
*/
func (client *HClient) Close() error {
	if client.state == stateOpen {
		if err := client.Trans.Close(); err != nil {
			return err
		}
		client.state = stateDefault
	}
	return nil
}

/**
 * Brings a table on-line (enables it)
 * 
 * Parameters:
 *  - TableName: name of the table
 */
func (client *HClient) EnableTable(tableName string) error {
	return checkHbaseError(client.hbase.EnableTable(Hbase.Bytes(tableName)))
}

/**
 * Disables a table (takes it off-line) If it is being served, the master
 * will tell the servers to stop serving it.
 * 
 * Parameters:
 *  - TableName: name of the table
 */
func (client *HClient) DisableTable(tableName string) (err error) {
	return checkHbaseError(client.hbase.DisableTable(Hbase.Bytes(tableName)))
}

/**
 * @return true if table is on-line
 * 
 * Parameters:
 *  - TableName: name of the table to check
 */
func (client *HClient) IsTableEnabled(tableName string) (ret bool, err error) {
	ret, io, e1 := client.hbase.IsTableEnabled(Hbase.Bytes(tableName))
	err = checkHbaseError(io, e1)
	return
}

/**
 * Parameters:
 *  - TableNameOrRegionName
 */
func (client *HClient) Compact(tableNameOrRegionName string) (err error) {
	return checkHbaseError(client.hbase.Compact(Hbase.Bytes(tableNameOrRegionName)))
}

/**
 * Parameters:
 *  - TableNameOrRegionName
 */
func (client *HClient) MajorCompact(tableNameOrRegionName string) (err error) {
	return checkHbaseError(client.hbase.MajorCompact(Hbase.Bytes(tableNameOrRegionName)))
}

/**
 * List all the column families assoicated with a table.
 * 
 * @return list of column family descriptors
 * 
 * Parameters:
 *  - TableName: table name
 */
func (client *HClient) GetTableNames() (tables []string, err error) {
	ret, io, e1 := client.hbase.GetTableNames()
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	tables = textListToStr(ret)
	return
}

/**
 * List all the column families assoicated with a table.
 * 
 * @return list of column family descriptors
 * 
 * Parameters:
 *  - TableName: table name
 */
func (client *HClient) GetColumnDescriptors(tableName string) (columns map[string]*ColumnDescriptor, err error) {
	ret, io, e1 := client.hbase.GetColumnDescriptors(Hbase.Text(tableName))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}
	columns = toColMap(ret)
	return
}

/**
 * List the regions associated with a table.
 * 
 * @return list of region descriptors
 * 
 * Parameters:
 *  - TableName: table name
 */
func (client *HClient) GetTableRegions(tableName string) (regions []*TRegionInfo, err error) {
	ret, io, e1 := client.hbase.GetTableRegions(Hbase.Text(tableName))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	regions = toRegionList(ret)
	return
}

/**
 * Create a table with the specified column families.  The name
 * field for each ColumnDescriptor must be set and must end in a
 * colon (:). All other fields are optional and will get default
 * values if not explicitly specified.
 * 
 * @throws IllegalArgument if an input parameter is invalid
 * 
 * @throws AlreadyExists if the table name already exists
 * 
 * Parameters:
 *  - TableName: name of table to create
 *  - ColumnFamilies: list of column family descriptors
 */
func (client *HClient) CreateTable(tableName string, columnFamilies []*ColumnDescriptor) (exists bool, err error) {
	columns := toHbaseColList(columnFamilies)
	io, ia, ex, e1 := client.hbase.CreateTable(Hbase.Text(tableName), columns)
	if err = checkHbaseArgError(io, ia, e1); err != nil {
		return
	}
	exists = (ex != nil)
	return
}

/**
 * Deletes a table
 * 
 * @throws IOError if table doesn't exist on server or there was some other
 * problem
 * 
 * Parameters:
 *  - TableName: name of table to delete
 */
func (client *HClient) DeleteTable(tableName string) (err error) {
	return checkHbaseError(client.hbase.DeleteTable(Hbase.Text(tableName)))
}

/**
 * Get a single TCell for the specified table, row, and column at the
 * latest timestamp. Returns an empty list if no such value exists.
 * 
 * @return value for specified row/column
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Column: column name
 *  - Attributes: Get attributes
 */
func (client *HClient) Get(tableName string, row []byte, column string, attributes map[string]string) (data []*Hbase.TCell, err error) {
	ret, io, e1 := client.hbase.Get(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(column), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the specified number of versions for the specified table,
 * row, and column.
 * 
 * @return list of cells for specified row/column
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Column: column name
 *  - NumVersions: number of versions to retrieve
 *  - Attributes: Get attributes
 */
func (client *HClient) GetVer(tableName string, row []byte, column string, numVersions int32, attributes map[string]string) (data []*Hbase.TCell, err error) {
	ret, io, e1 := client.hbase.GetVer(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(column), numVersions, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the specified number of versions for the specified table,
 * row, and column.  Only versions less than or equal to the specified
 * timestamp will be returned.
 * 
 * @return list of cells for specified row/column
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Column: column name
 *  - Timestamp: timestamp
 *  - NumVersions: number of versions to retrieve
 *  - Attributes: Get attributes
 */
func (client *HClient) GetVerTs(tableName string, row []byte, column string, timestamp int64, numVersions int32, attributes map[string]string) (data []*Hbase.TCell, err error) {
	ret, io, e1 := client.hbase.GetVerTs(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(column), timestamp, numVersions, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get all the data for the specified table and row at the latest
 * timestamp. Returns an empty list if the row does not exist.
 * 
 * @return TRowResult containing the row and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRow(tableName string, row []byte, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRow(Hbase.Text(tableName), Hbase.Text(row), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the specified columns for the specified table and row at the latest
 * timestamp. Returns an empty list if the row does not exist.
 * 
 * @return TRowResult containing the row and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Columns: List of columns to return, null for all columns
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRowWithColumns(tableName string, row []byte, columns []string, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRowWithColumns(Hbase.Text(tableName), Hbase.Text(row), toHbaseTextList(columns), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get all the data for the specified table and row at the specified
 * timestamp. Returns an empty list if the row does not exist.
 * 
 * @return TRowResult containing the row and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of the table
 *  - Row: row key
 *  - Timestamp: timestamp
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRowTs(tableName string, row []byte, timestamp int64, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRowTs(Hbase.Text(tableName), Hbase.Text(row), timestamp, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the specified columns for the specified table and row at the specified
 * timestamp. Returns an empty list if the row does not exist.
 * 
 * @return TRowResult containing the row and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Columns: List of columns to return, null for all columns
 *  - Timestamp
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRowWithColumnsTs(tableName string, row []byte, columns []string, timestamp int64, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRowWithColumnsTs(Hbase.Text(tableName), Hbase.Text(row), toHbaseTextList(columns), timestamp, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get all the data for the specified table and rows at the latest
 * timestamp. Returns an empty list if no rows exist.
 * 
 * @return TRowResult containing the rows and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Rows: row keys
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRows(tableName string, rows [][]byte, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRows(Hbase.Text(tableName), toHbaseTextListFromByte(rows), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the specified columns for the specified table and rows at the latest
 * timestamp. Returns an empty list if no rows exist.
 * 
 * @return TRowResult containing the rows and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Rows: row keys
 *  - Columns: List of columns to return, null for all columns
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRowsWithColumns(tableName string, rows [][]byte, columns []string, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	if err = client.Open(); err != nil {
		return
	}

	ret, io, e1 := client.hbase.GetRowsWithColumns(Hbase.Text(tableName), toHbaseTextListFromByte(rows), toHbaseTextList(columns), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get all the data for the specified table and rows at the specified
 * timestamp. Returns an empty list if no rows exist.
 * 
 * @return TRowResult containing the rows and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of the table
 *  - Rows: row keys
 *  - Timestamp: timestamp
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRowsTs(tableName string, rows [][]byte, timestamp int64, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRowsTs(Hbase.Text(tableName), toHbaseTextListFromByte(rows), timestamp, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the specified columns for the specified table and rows at the specified
 * timestamp. Returns an empty list if no rows exist.
 * 
 * @return TRowResult containing the rows and map of columns to TCells
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Rows: row keys
 *  - Columns: List of columns to return, null for all columns
 *  - Timestamp
 *  - Attributes: Get attributes
 */
func (client *HClient) GetRowsWithColumnsTs(tableName string, rows [][]byte, columns []string, timestamp int64, attributes map[string]string) (data []*Hbase.TRowResult, err error) {
	ret, io, e1 := client.hbase.GetRowsWithColumnsTs(Hbase.Text(tableName), toHbaseTextListFromByte(rows), toHbaseTextList(columns), timestamp, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/*
 * Apply a series of mutations (updates/deletes) to a row in a
 * single transaction.  If an exception is thrown, then the
 * transaction is aborted.  Default current timestamp is used, and
 * all entries will have an identical timestamp.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Mutations: list of mutation commands
 *  - Attributes: Mutation attributes
 */
func (client *HClient) MutateRow(tableName string, row []byte, mutations []*Hbase.Mutation, attributes map[string]string) error {
	return checkHbaseArgError(client.hbase.MutateRow(Hbase.Text(tableName), Hbase.Text(row), mutations, toHbaseTextMap(attributes)))
}

/**
 * Apply a series of mutations (updates/deletes) to a row in a
 * single transaction.  If an exception is thrown, then the
 * transaction is aborted.  The specified timestamp is used, and
 * all entries will have an identical timestamp.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Mutations: list of mutation commands
 *  - Timestamp: timestamp
 *  - Attributes: Mutation attributes
 */
func (client *HClient) MutateRowTs(tableName string, row []byte, mutations []*Hbase.Mutation, timestamp int64, attributes map[string]string) error {
	return checkHbaseArgError(client.hbase.MutateRowTs(Hbase.Text(tableName), Hbase.Text(row), mutations, timestamp, toHbaseTextMap(attributes)))
}

/**
 * Apply a series of batches (each a series of mutations on a single row)
 * in a single transaction.  If an exception is thrown, then the
 * transaction is aborted.  Default current timestamp is used, and
 * all entries will have an identical timestamp.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - RowBatches: list of row batches
 *  - Attributes: Mutation attributes
 */
func (client *HClient) MutateRows(tableName string, rowBatches []*Hbase.BatchMutation, attributes map[string]string) error {
	return checkHbaseArgError(client.hbase.MutateRows(Hbase.Text(tableName), rowBatches, toHbaseTextMap(attributes)))
}

/**
 * Apply a series of batches (each a series of mutations on a single row)
 * in a single transaction.  If an exception is thrown, then the
 * transaction is aborted.  The specified timestamp is used, and
 * all entries will have an identical timestamp.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - RowBatches: list of row batches
 *  - Timestamp: timestamp
 *  - Attributes: Mutation attributes
 */
func (client *HClient) MutateRowsTs(tableName string, rowBatches []*Hbase.BatchMutation, timestamp int64, attributes map[string]string) error {
	return checkHbaseArgError(client.hbase.MutateRowsTs(Hbase.Text(tableName), rowBatches, timestamp, toHbaseTextMap(attributes)))
}

/**
 * Atomically increment the column value specified.  Returns the next value post increment.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row to increment
 *  - Column: name of column
 *  - Value: amount to increment by
 */
func (client *HClient) AtomicIncrement(tableName string, row []byte, column string, value int64) (v int64, err error) {
	ret, io, ia, e1 := client.hbase.AtomicIncrement(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(column), value)
	if err = checkHbaseArgError(io, ia, e1); err != nil {
		return
	}

	v = ret
	return
}

/**
 * Delete all cells that match the passed row and column.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: Row to update
 *  - Column: name of column whose value is to be deleted
 *  - Attributes: Delete attributes
 */
func (client *HClient) DeleteAll(tableName string, row []byte, column string, attributes map[string]string) error {
	return checkHbaseError(client.hbase.DeleteAll(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(column), toHbaseTextMap(attributes)))
}

/**
 * Delete all cells that match the passed row and column and whose
 * timestamp is equal-to or older than the passed timestamp.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: Row to update
 *  - Column: name of column whose value is to be deleted
 *  - Timestamp: timestamp
 *  - Attributes: Delete attributes
 */
func (client *HClient) DeleteAllTs(tableName string, row []byte, column string, timestamp int64, attributes map[string]string) error {
	return checkHbaseError(client.hbase.DeleteAllTs(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(column), timestamp, toHbaseTextMap(attributes)))
}

/**
 * Completely delete the row's cells.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: key of the row to be completely deleted.
 *  - Attributes: Delete attributes
 */
func (client *HClient) DeleteAllRow(tableName string, row []byte, attributes map[string]string) error {
	return checkHbaseError(client.hbase.DeleteAllRow(Hbase.Text(tableName), Hbase.Text(row), toHbaseTextMap(attributes)))
}

/**
 * Increment a cell by the ammount.
 * Increments can be applied async if hbase.regionserver.thrift.coalesceIncrement is set to true.
 * False is the default.  Turn to true if you need the extra performance and can accept some
 * data loss if a thrift server dies with increments still in the queue.
 * 
 * Parameters:
 *  - Increment: The single increment to apply
 */
func (client *HClient) Increment(increment *Hbase.TIncrement) error {
	return checkHbaseError(client.hbase.Increment(increment))
}

/**
 * Parameters:
 *  - Increments: The list of increments
 */
func (client *HClient) IncrementRows(increments []*Hbase.TIncrement) error {
	return checkHbaseError(client.hbase.IncrementRows(increments))
}

/**
 * Completely delete the row's cells marked with a timestamp
 * equal-to or older than the passed timestamp.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: key of the row to be completely deleted.
 *  - Timestamp: timestamp
 *  - Attributes: Delete attributes
 */
func (client *HClient) DeleteAllRowTs(tableName string, row []byte, timestamp int64, attributes map[string]string) error {
	return checkHbaseError(client.hbase.DeleteAllRowTs(Hbase.Text(tableName), Hbase.Text(row), timestamp, toHbaseTextMap(attributes)))
}

/**
 * Get a scanner on the current table, using the Scan instance
 * for the scan parameters.
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Scan: Scan instance
 *  - Attributes: Scan attributes
 */
func (client *HClient) ScannerOpenWithScan(tableName string, scan *TScan, attributes map[string]string) (id int32, err error) {
	ret, io, e1 := client.hbase.ScannerOpenWithScan(Hbase.Text(tableName), toHbaseTScan(scan), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	id = int32(ret)
	return
}

/**
 * Get a scanner on the current table starting at the specified row and
 * ending at the last row in the table.  Return the specified columns.
 * 
 * @return scanner id to be used with other scanner procedures
 * 
 * Parameters:
 *  - TableName: name of table
 *  - StartRow: Starting row in table to scan.
 * Send "" (empty string) to start at the first row.
 *  - Columns: columns to scan. If column name is a column family, all
 * columns of the specified column family are returned. It's also possible
 * to pass a regex in the column qualifier.
 *  - Attributes: Scan attributes
 */
func (client *HClient) ScannerOpen(tableName string, startRow []byte, columns []string, attributes map[string]string) (id int32, err error) {
	ret, io, e1 := client.hbase.ScannerOpen(Hbase.Text(tableName), Hbase.Text(startRow), toHbaseTextList(columns), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	id = int32(ret)
	return
}

/**
 * Get a scanner on the current table starting and stopping at the
 * specified rows.  ending at the last row in the table.  Return the
 * specified columns.
 * 
 * @return scanner id to be used with other scanner procedures
 * 
 * Parameters:
 *  - TableName: name of table
 *  - StartRow: Starting row in table to scan.
 * Send "" (empty string) to start at the first row.
 *  - StopRow: row to stop scanning on. This row is *not* included in the
 * scanner's results
 *  - Columns: columns to scan. If column name is a column family, all
 * columns of the specified column family are returned. It's also possible
 * to pass a regex in the column qualifier.
 *  - Attributes: Scan attributes
 */
func (client *HClient) ScannerOpenWithStop(tableName string, startRow []byte, stopRow []byte, columns []string, attributes map[string]string) (id int32, err error) {
	ret, io, e1 := client.hbase.ScannerOpenWithStop(Hbase.Text(tableName), Hbase.Text(startRow), Hbase.Text(stopRow), toHbaseTextList(columns), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	id = int32(ret)
	return
}

/**
 * Open a scanner for a given prefix.  That is all rows will have the specified
 * prefix. No other rows will be returned.
 * 
 * @return scanner id to use with other scanner calls
 * 
 * Parameters:
 *  - TableName: name of table
 *  - StartAndPrefix: the prefix (and thus start row) of the keys you want
 *  - Columns: the columns you want returned
 *  - Attributes: Scan attributes
 */
func (client *HClient) ScannerOpenWithPrefix(tableName string, startAndPrefix []byte, columns []string, attributes map[string]string) (id int32, err error) {
	ret, io, e1 := client.hbase.ScannerOpenWithPrefix(Hbase.Text(tableName), Hbase.Text(startAndPrefix), toHbaseTextList(columns), toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	id = int32(ret)
	return
}

/**
 * Get a scanner on the current table starting at the specified row and
 * ending at the last row in the table.  Return the specified columns.
 * Only values with the specified timestamp are returned.
 * 
 * @return scanner id to be used with other scanner procedures
 * 
 * Parameters:
 *  - TableName: name of table
 *  - StartRow: Starting row in table to scan.
 * Send "" (empty string) to start at the first row.
 *  - Columns: columns to scan. If column name is a column family, all
 * columns of the specified column family are returned. It's also possible
 * to pass a regex in the column qualifier.
 *  - Timestamp: timestamp
 *  - Attributes: Scan attributes
 */
func (client *HClient) ScannerOpenTs(tableName string, startRow []byte, columns []string, timestamp int64, attributes map[string]string) (id int32, err error) {
	ret, io, e1 := client.hbase.ScannerOpenTs(Hbase.Text(tableName), Hbase.Text(startRow), toHbaseTextList(columns), timestamp, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	id = int32(ret)
	return
}

/**
 * Get a scanner on the current table starting and stopping at the
 * specified rows.  ending at the last row in the table.  Return the
 * specified columns.  Only values with the specified timestamp are
 * returned.
 * 
 * @return scanner id to be used with other scanner procedures
 * 
 * Parameters:
 *  - TableName: name of table
 *  - StartRow: Starting row in table to scan.
 * Send "" (empty string) to start at the first row.
 *  - StopRow: row to stop scanning on. This row is *not* included in the
 * scanner's results
 *  - Columns: columns to scan. If column name is a column family, all
 * columns of the specified column family are returned. It's also possible
 * to pass a regex in the column qualifier.
 *  - Timestamp: timestamp
 *  - Attributes: Scan attributes
 */
func (client *HClient) ScannerOpenWithStopTs(tableName string, startRow []byte, stopRow []byte, columns []string, timestamp int64, attributes map[string]string) (id int32, err error) {
	ret, io, e1 := client.hbase.ScannerOpenWithStopTs(Hbase.Text(tableName), Hbase.Text(startRow), Hbase.Text(stopRow), toHbaseTextList(columns), timestamp, toHbaseTextMap(attributes))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	id = int32(ret)
	return
}

/**
 * Returns the scanner's current row value and advances to the next
 * row in the table.  When there are no more rows in the table, or a key
 * greater-than-or-equal-to the scanner's specified stopRow is reached,
 * an empty list is returned.
 * 
 * @return a TRowResult containing the current row and a map of the columns to TCells.
 * 
 * @throws IllegalArgument if ScannerID is invalid
 * 
 * @throws NotFound when the scanner reaches the end
 * 
 * Parameters:
 *  - Id: id of a scanner returned by scannerOpen
 */
func (client *HClient) ScannerGet(id int32) (data []*Hbase.TRowResult, err error) {
	ret, io, ia, e1 := client.hbase.ScannerGet(Hbase.ScannerID(id))
	if err = checkHbaseArgError(io, ia, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Returns, starting at the scanner's current row value nbRows worth of
 * rows and advances to the next row in the table.  When there are no more
 * rows in the table, or a key greater-than-or-equal-to the scanner's
 * specified stopRow is reached,  an empty list is returned.
 * 
 * @return a TRowResult containing the current row and a map of the columns to TCells.
 * 
 * @throws IllegalArgument if ScannerID is invalid
 * 
 * @throws NotFound when the scanner reaches the end
 * 
 * Parameters:
 *  - Id: id of a scanner returned by scannerOpen
 *  - NbRows: number of results to return
 */
func (client *HClient) ScannerGetList(id int32, nbRows int32) (data []*Hbase.TRowResult, err error) {
	ret, io, ia, e1 := client.hbase.ScannerGetList(Hbase.ScannerID(id), nbRows)
	if err = checkHbaseArgError(io, ia, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Closes the server-state associated with an open scanner.
 * 
 * @throws IllegalArgument if ScannerID is invalid
 * 
 * Parameters:
 *  - Id: id of a scanner returned by scannerOpen
 */
func (client *HClient) ScannerClose(id int32) error {
	return checkHbaseArgError(client.hbase.ScannerClose(Hbase.ScannerID(id)))
}

/**
 * Get the row just before the specified one.
 * 
 * @return value for specified row/column
 * 
 * Parameters:
 *  - TableName: name of table
 *  - Row: row key
 *  - Family: column name
 */
func (client *HClient) GetRowOrBefore(tableName string, row string, family string) (data []*Hbase.TCell, err error) {
	ret, io, e1 := client.hbase.GetRowOrBefore(Hbase.Text(tableName), Hbase.Text(row), Hbase.Text(family))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	data = ret
	return
}

/**
 * Get the regininfo for the specified row. It scans
 * the metatable to find region's start and end keys.
 * 
 * @return value for specified row/column
 * 
 * Parameters:
 *  - Row: row key
 */
func (client *HClient) GetRegionInfo(row string) (region *TRegionInfo, err error) {
	ret, io, e1 := client.hbase.GetRegionInfo(Hbase.Text(row))
	if err = checkHbaseError(io, e1); err != nil {
		return
	}

	region = toRegion(ret)
	return
}
