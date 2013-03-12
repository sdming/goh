/*


*/

package goh

import (
	"fmt"
	"github.com/sdming/goh/Hbase"
	"thrift"
)

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

func NewColumnDescriptorDefault(name string) *ColumnDescriptor {
	output := &ColumnDescriptor{}
	output.MaxVersions = 3
	output.Compression = "NONE"
	output.InMemory = false
	output.BloomFilterType = "NONE"
	output.BloomFilterVectorSize = 0
	output.BloomFilterNbHashes = 0
	output.BlockCacheEnabled = false
	output.TimeToLive = -1
	output.Name = name
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
	Row     string            "row"     // 1
	Columns map[string]*TCell "columns" // 2
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

func (t *AlreadyExists) String() string {
	if t == nil {
		return "<nil>"
	}
	return t.Message
}

/*
convert
*/

func fromListStr(list []string) thrift.TList {
	if list == nil {
		return nil
	}

	l := len(list)
	data := thrift.NewTList(thrift.STRING, l)
	for i := 0; i < l; i++ {
		//data.Set(i, Hbase.Text(list[i]))
		data.Push(Hbase.Text(list[i]))
	}

	return data
}

func toListStr(list thrift.TList) []string {
	if list == nil {
		return make([]string, 0)
	}

	l := list.Len()
	data := make([]string, l)
	for i := 0; i < l; i++ {
		data[i] = list.At(i).(string)
	}

	return data
}

func toColumnsMap(hbaseColumns thrift.TMap) map[string]*ColumnDescriptor {
	if hbaseColumns == nil {
		return nil
	}

	l := hbaseColumns.Len()
	columns := make(map[string]*ColumnDescriptor, l)
	//fmt.Println("KeyType", hbaseColumns.KeyType())
	//fmt.Println("ValueType", hbaseColumns.ValueType())
	//fmt.Println("len", l)

	keys := hbaseColumns.Keys()
	for i := 0; i < l; i++ {
		key := keys[i]
		value, ok := hbaseColumns.Get(key)

		if !ok {
			continue
		}

		column := toColumn(value.(*Hbase.ColumnDescriptor))
		columns[column.Name] = column
	}

	return columns
}

func toColumn(hbaseColumn *Hbase.ColumnDescriptor) *ColumnDescriptor {
	column := &ColumnDescriptor{
		Name:                  string(hbaseColumn.Name),
		MaxVersions:           hbaseColumn.MaxVersions,
		Compression:           hbaseColumn.Compression,
		InMemory:              hbaseColumn.InMemory,
		BloomFilterType:       hbaseColumn.BloomFilterType,
		BloomFilterVectorSize: hbaseColumn.BloomFilterVectorSize,
		BloomFilterNbHashes:   hbaseColumn.BloomFilterNbHashes,
		BlockCacheEnabled:     hbaseColumn.BlockCacheEnabled,
		TimeToLive:            hbaseColumn.TimeToLive,
	}
	return column
}

func fromColumns(columnFamilies []*ColumnDescriptor) thrift.TList {
	l := len(columnFamilies)
	columns := thrift.NewTListDefault()
	for i := 0; i < l; i++ {
		col := columnFamilies[i]
		hbaseColumn := &Hbase.ColumnDescriptor{
			Name:                  Hbase.Text(col.Name),
			MaxVersions:           col.MaxVersions,
			Compression:           col.Compression,
			InMemory:              col.InMemory,
			BloomFilterType:       col.BloomFilterType,
			BloomFilterVectorSize: col.BloomFilterVectorSize,
			BloomFilterNbHashes:   col.BloomFilterNbHashes,
			BlockCacheEnabled:     col.BlockCacheEnabled,
			TimeToLive:            col.TimeToLive,
		}
		columns.Push(hbaseColumn)
	}
	return columns
}

func toRegion(hbaseRegion *Hbase.TRegionInfo) *TRegionInfo {
	region := &TRegionInfo{
		StartKey:   string(hbaseRegion.StartKey),
		EndKey:     string(hbaseRegion.EndKey),
		Id:         hbaseRegion.Id,
		Name:       string(hbaseRegion.Name),
		Version:    hbaseRegion.Version,
		ServerName: string(hbaseRegion.ServerName),
		Port:       hbaseRegion.Port,
	}
	return region
}

func toRegionsList(list thrift.TList) []*TRegionInfo {
	l := list.Len()
	regions := make([]*TRegionInfo, l)
	//fmt.Println("ElemType", ret.ElemType())
	//fmt.Println("len", l)

	for i := 0; i < l; i++ {
		value := list.At(i)
		regions[i] = toRegion(value.(*Hbase.TRegionInfo))
	}

	return regions
}

func toAlreadyExists(exist *Hbase.AlreadyExists) *AlreadyExists {
	if exist == nil {
		return nil
	}

	return &AlreadyExists{
		Message: exist.Message,
	}

}

func toCell(hbaseCell *Hbase.TCell) *TCell {
	cell := &TCell{
		Value:     hbaseCell.Value,
		Timestamp: hbaseCell.Timestamp,
	}
	return cell
}

func toListCell(list thrift.TList) []*TCell {
	if list == nil {
		return nil
	}

	l := list.Len()
	data := make([]*TCell, l)

	for i := 0; i < l; i++ {
		value := list.At(i)
		data[i] = toCell(value.(*Hbase.TCell))
	}

	return data
}

func fromMapStr(data map[string]string) thrift.TMap {

	var m thrift.TMap
	if data != nil {
		m = thrift.NewTMap(thrift.STRING, thrift.STRING, len(data))
		for k, v := range data {
			m.Set(Hbase.Text(k), Hbase.Text(v))
		}
	}

	return m
}

func toRowResult(hbaseRow *Hbase.TRowResult) *TRowResult {
	columns := make(map[string]*TCell)

	keys := hbaseRow.Columns.Keys()
	l := hbaseRow.Columns.Len()
	for i := 0; i < l; i++ {
		key := keys[i]
		value, ok := hbaseRow.Columns.Get(key)

		if !ok {
			continue
		}
		columns[key.(string)] = toCell(value.(*Hbase.TCell))
	}

	row := &TRowResult{
		Row:     string(hbaseRow.Row),
		Columns: columns,
	}
	return row
}

func toListRowResult(list thrift.TList) []*TRowResult {
	if list == nil {
		return nil
	}

	l := list.Len()
	data := make([]*TRowResult, l)

	for i := 0; i < l; i++ {
		value := list.At(i)
		data[i] = toRowResult(value.(*Hbase.TRowResult))
	}

	return data
}

func name() {
	fmt.Println("...")
}
