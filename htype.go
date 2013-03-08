/*


*/

package goh

type ScannerID int32

/**
 * An HColumnDescriptor contains information about a column family
 * such as the number of versions, compression settings, etc. It is
 * used as input when creating a table or adding a column.
 * 
 * Attributes:
 *  - Name
 *  - MaxVersions
 *  - Compression
 *  - InMemory
 *  - BloomFilterType
 *  - BloomFilterVectorSize
 *  - BloomFilterNbHashes
 *  - BlockCacheEnabled
 *  - TimeToLive
 */
type ColumnDescriptor struct {
	Name                  string "name"                  // 1
	MaxVersions           int32  "maxVersions"           // 2
	Compression           string "compression"           // 3
	InMemory              bool   "inMemory"              // 4
	BloomFilterType       string "bloomFilterType"       // 5
	BloomFilterVectorSize int32  "bloomFilterVectorSize" // 6
	BloomFilterNbHashes   int32  "bloomFilterNbHashes"   // 7
	BlockCacheEnabled     bool   "blockCacheEnabled"     // 8
	TimeToLive            int32  "timeToLive"            // 9
}

func NewColumnDescriptor() *ColumnDescriptor {
	output := &ColumnDescriptor{}
	output.MaxVersions = 3
	output.Compression = "NONE"
	output.InMemory = false
	output.BloomFilterType = "NONE"
	output.BloomFilterVectorSize = 0
	output.BloomFilterNbHashes = 0
	output.BlockCacheEnabled = false
	output.TimeToLive = -1

	return output
}

/**
 * A TRegionInfo contains information about an HTable region.
 * 
 * Attributes:
 *  - StartKey
 *  - EndKey
 *  - Id
 *  - Name
 *  - Version
 *  - ServerName
 *  - Port
 */
type TRegionInfo struct {
	StartKey   string "startKey"   // 1
	EndKey     string "endKey"     // 2
	Id         int64  "id"         // 3
	Name       string "name"       // 4
	Version    byte   "version"    // 5
	ServerName string "serverName" // 6
	Port       int32  "port"       // 7
}

/**
 * A Mutation object is used to either update or delete a column-value.
 * 
 * Attributes:
 *  - IsDelete
 *  - Column
 *  - Value
 *  - WriteToWAL
 */
type Mutation struct {
	IsDelete   bool   "isDelete"   // 1
	Column     string "column"     // 2
	Value      string "value"      // 3
	WriteToWAL bool   "writeToWAL" // 4
}

func NewMutation() *Mutation {
	output := &Mutation{}
	output.IsDelete = false
	output.WriteToWAL = true

	return output
}

/**
 * A BatchMutation object is used to apply a number of Mutations to a single row.
 * 
 * Attributes:
 *  - Row
 *  - Mutations
 */
type BatchMutation struct {
	Row       string     "row"       // 1
	Mutations []Mutation "mutations" // 2
}

/**
 * For increments that are not incrementColumnValue
 * equivalents.
 * 
 * Attributes:
 *  - Table
 *  - Row
 *  - Column
 *  - Ammount
 */
type TIncrement struct {
	Table   string "table"   // 1
	Row     string "row"     // 2
	Column  string "column"  // 3
	Ammount int64  "ammount" // 4
}

/**
 * Holds row name and then a map of columns to cells.
 * 
 * Attributes:
 *  - Row
 *  - Columns
 */
type TRowResult struct {
	Row     string           "row"     // 1
	Columns map[string]TCell "columns" // 2
}

/**
 * A Scan object is used to specify scanner parameters when opening a scanner.
 * 
 * Attributes:
 *  - StartRow
 *  - StopRow
 *  - Timestamp
 *  - Columns
 *  - Caching
 *  - FilterString
 */
type TScan struct {
	StartRow     string   "startRow"     // 1
	StopRow      string   "stopRow"      // 2
	Timestamp    int64    "timestamp"    // 3
	Columns      []string "columns"      // 4
	Caching      int32    "caching"      // 5
	FilterString string   "filterString" // 6
}

//type Text []byte

//type Bytes []byte

//type ScannerID int32

/**
 * TCell - Used to transport a cell value (byte[]) and the timestamp it was
 * stored with together as a result for get and getRow methods. This promotes
 * the timestamp of a cell to a first-class value, making it easy to take
 * note of temporal data. Cell is used all the way from HStore up to HTable.
 * 
 * Attributes:
 *  - Value
 *  - Timestamp
 */
type TCell struct {
	Value     []byte "value"     // 1
	Timestamp int64  "timestamp" // 2
}

/**
 * An IOError exception signals that an error occurred communicating
 * to the Hbase master or an Hbase region server.  Also used to return
 * more general Hbase error conditions.
 * 
 * Attributes:
 *  - Message
 */
type IOError struct {
	Message string "message" // 1
}

/**
 * An IllegalArgument exception indicates an illegal or invalid
 * argument was passed into a procedure.
 * 
 * Attributes:
 *  - Message
 */
type IllegalArgument struct {
	Message string "message" // 1
}

/**
 * An AlreadyExists exceptions signals that a table with the specified
 * name already exists
 * 
 * Attributes:
 *  - Message
 */
type AlreadyExists struct {
	Message string "message" // 1
}
