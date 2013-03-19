package main

import (
	"encoding/json"
	"fmt"
	"github.com/sdming/goh"
	"github.com/sdming/goh/Hbase"
)

func main() {

	address := "192.168.17.129:9090"
	fmt.Println(address)

	client, err := goh.NewTcpClient(address, goh.TBinaryProtocol, false)
	if err != nil {
		fmt.Println(err)
		return
	}

	if err = client.Open(); err != nil {
		fmt.Println(err)
		return
	}

	defer client.Close()

	table := "test"

	attributes := make(map[string]string)
	attributes["attr1"] = "attr-val1"
	attributes["attr2"] = "attr-val2"

	columns := make([]string, 2)
	columns[0] = "cf:a"
	columns[1] = "cf:b"

	var timestamp int64
	timestamp = 1362555589004

	rows := make([][]byte, 2)
	rows[0] = []byte("row1")
	rows[1] = []byte("row2")

	mutations := make([]*Hbase.Mutation, 1)
	mutations[0] = goh.NewMutation("cf:c", []byte("value3-mutation"))

	rowBatches := make([]*Hbase.BatchMutation, 1)
	rowBatches[0] = goh.NewBatchMutation([]byte("row3"), mutations)

	scan := &goh.TScan{
		StartRow:     []byte("row1"),
		StopRow:      []byte("row9"),
		Timestamp:    0,
		Columns:      []string{"cf:a", "cf:b"},
		Caching:      10,
		FilterString: "",
		//FilterString: "substring:value",
	}

	var scanId int32

	if "test" == "" {

		fmt.Print("IsTableEnabled:")
		fmt.Println(client.IsTableEnabled(table))

		fmt.Print("DisableTable:")
		fmt.Println(client.DisableTable(table))

		fmt.Print("IsTableEnabled:")
		fmt.Println(client.IsTableEnabled(table))

		fmt.Print("EnableTable:")
		fmt.Println(client.EnableTable(table))

		fmt.Print("IsTableEnabled:")
		fmt.Println(client.IsTableEnabled(table))

		fmt.Print("Compact:")
		fmt.Println(client.Compact(table))

		fmt.Print("MajorCompact:")
		fmt.Println(client.MajorCompact(table))

		fmt.Print("GetTableNames:")
		if data, err := client.GetTableNames(); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
		}

		fmt.Print("GetColumnDescriptors:")
		if data, err := client.GetColumnDescriptors(table); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
		}

		fmt.Print("GetTableRegions:")
		if data, err := client.GetTableRegions(table); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
		}

		fmt.Print("CreateTable:")
		cols := make([]*goh.ColumnDescriptor, 2)
		cols[0] = goh.NewColumnDescriptorDefault("cfa")
		cols[1] = goh.NewColumnDescriptorDefault("cfb")
		if exist, err := client.CreateTable("test_create", cols); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(exist)
		}

		fmt.Print("DisableTable:")
		fmt.Println(client.DisableTable("test_create"))

		fmt.Print("DeleteTable:")
		fmt.Println(client.DeleteTable("test_create"))

		fmt.Print("Get:")
		if data, err := client.Get(table, []byte("row1"), "cf:a", attributes); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)
		}

		fmt.Print("GetVer:")
		if data, err := client.GetVer(table, []byte("row1"), "cf:a", 10, attributes); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)
			//0 : value1 ; 1362555589004
			//1 : value1 ; 1362463859431
		}

		fmt.Print("GetVerTs:")
		if data, err := client.GetVerTs(table, []byte("row1"), "cf:a", timestamp, 10, attributes); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)
		}

		fmt.Print("GetRow:")
		if data, err := client.GetRow(table, []byte("row1"), nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowWithColumns:")
		if data, err := client.GetRowWithColumns(table, []byte("row1"), columns, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowTs:")
		if data, err := client.GetRowTs(table, []byte("row1"), timestamp, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowWithColumnsTs:")
		if data, err := client.GetRowWithColumnsTs(table, []byte("row1"), columns, timestamp, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRows:")
		if data, err := client.GetRows(table, rows, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowsWithColumns:")
		if data, err := client.GetRowsWithColumns(table, rows, columns, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowsTs:")
		if data, err := client.GetRowsTs(table, rows, timestamp, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowsWithColumnsTs:")
		if data, err := client.GetRowsWithColumnsTs(table, rows, columns, timestamp, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowOrBefore:")
		if data, err := client.GetRowOrBefore(table, "row1", "cf"); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)
		}

		fmt.Print("GetRegionInfo:")
		if data, err := client.GetRegionInfo(""); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
		}

		fmt.Print("MutateRow:")
		fmt.Println(client.MutateRow(table, []byte("row3"), mutations, nil))

		fmt.Print("MutateRowTs:")
		fmt.Println(client.MutateRowTs(table, []byte("row3"), mutations, timestamp, nil))

		fmt.Print("MutateRows:")
		fmt.Println(client.MutateRows(table, rowBatches, nil))

		fmt.Print("MutateRowsTs:")
		fmt.Println(client.MutateRowsTs(table, rowBatches, timestamp, nil))

		fmt.Print("AtomicIncrement:")
		if data, err := client.AtomicIncrement(table, []byte("row4"), "cf:a", 64); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
		}

		fmt.Print("DeleteAll:")
		fmt.Println(client.DeleteAll(table, []byte("row4"), "cf:a", nil))

		fmt.Print("DeleteAllTs:")
		fmt.Println(client.DeleteAllTs(table, []byte("row4"), "cf:a", timestamp, nil))

		fmt.Print("DeleteAllRow:")
		fmt.Println(client.DeleteAllRow(table, []byte("row4"), nil))

		fmt.Print("Increment:")
		fmt.Println(client.Increment(goh.NewTIncrement(table, []byte("row4"), "cf:a", 64)))

		fmt.Print("IncrementRows:")
		fmt.Println(client.IncrementRows([]*Hbase.TIncrement{goh.NewTIncrement(table, []byte("row4"), "cf:a", 64)}))

		fmt.Print("DeleteAllRowTs:")
		fmt.Println(client.DeleteAllRowTs(table, []byte("row4"), timestamp, nil))

		fmt.Print("ScannerOpenWithScan:")
		if data, err := client.ScannerOpenWithScan(table, scan, nil); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
			scanId = data
		}

		fmt.Print("ScannerOpen:")
		if data, err := client.ScannerOpen(table, []byte("row1"), columns, nil); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
			scanId = data
		}

		fmt.Print("ScannerOpenWithStop:")
		if data, err := client.ScannerOpenWithStop(table, []byte("row1"), []byte("row9"), columns, nil); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
			scanId = data
		}

		fmt.Print("ScannerOpenWithPrefix:")
		if data, err := client.ScannerOpenWithPrefix(table, []byte("row"), columns, nil); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
			scanId = data
		}

		fmt.Print("ScannerOpenTs:")
		if data, err := client.ScannerOpenTs(table, []byte("row1"), columns, 0, nil); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
			scanId = data
		}

		fmt.Print("ScannerOpenWithStopTs:")
		if data, err := client.ScannerOpenWithStopTs(table, []byte("row1"), []byte("row9"), columns, 0, nil); err != nil {
			fmt.Println(err)
		} else {
			dump(data)
			scanId = data
		}
	}

	fmt.Print("ScannerOpen:")
	if data, err := client.ScannerOpen(table, []byte("row1"), columns, nil); err != nil {
		fmt.Println(err)
	} else {
		dump(data)
		scanId = data
	}

	if scanId > 0 {
		// fmt.Println("scan start")
		// for {
		// 	if data, err := client.ScannerGet(scanId); err != nil {
		// 		fmt.Println(err)
		// 		break
		// 	} else if len(data) == 0 {
		// 		fmt.Println("scan end")
		// 		break
		// 	} else {
		// 		fmt.Println(data)
		// 	}
		// }

		fmt.Println("ScannerGetList")
		if data, err := client.ScannerGetList(scanId, 1000); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

	}

	if scanId > 0 {
		fmt.Print("ScannerClose:")
		fmt.Println(client.ScannerClose(scanId))
	}

	fmt.Println("done")

}

// func printTList(list thrift.TList) {

// 	fmt.Println("printTList")
// 	fmt.Println("Len()", list.Len())

// 	l := list.Len()
// 	fmt.Println("[")
// 	for i := 0; i < l; i++ {
// 		fmt.Println(i, ":", list.At(i))
// 	}
// 	fmt.Println("]")

// }

func dump(data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json.Marshal error:", err)
		return
	}
	fmt.Println(string(b))
}

func printCells(data []*Hbase.TCell) {
	if data == nil {
		fmt.Println("<nil>")
	}

	l := len(data)
	fmt.Println("[]*Hbase.TCell len:", l)
	for i, x := range data {
		fmt.Println(i, ":", string(x.Value), ";", x.Timestamp)
	}

}

func printRows(data []*Hbase.TRowResult) {
	if data == nil {
		fmt.Println("<nil>")
	}

	l := len(data)
	fmt.Println("[]*Hbase.TRowResult len:", l)
	for i, x := range data {
		fmt.Println(i, string(x.Row), "\n[")
		for k, v := range x.Columns {
			fmt.Println("\t", k, ":", string(v.Value), v.Timestamp)
		}
		fmt.Println("]")
	}

}
