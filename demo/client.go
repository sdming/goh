package main

import (
	"encoding/json"
	"fmt"
	"github.com/sdming/goh"
	_ "github.com/sdming/goh/Hbase"
	"thrift"
)

func main() {

	host, port := "192.168.17.129", "9090"
	fmt.Println(host, ":", port)

	client, err := goh.NewTcpClient(host, port, goh.TBinaryProtocol, false)
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
	attributes["attr"] = "attr-val"

	columns := make([]string, 2)
	columns[0] = "cf:a"
	columns[1] = "cf:b"

	var timestamp int64
	timestamp = 1362555589004

	rows := make([]string, 2)
	rows[0] = "row1"
	rows[1] = "row2"

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

		fmt.Print("DeleteTable:")
		fmt.Println(client.DeleteTable("test_create"))

		fmt.Print("Get:")
		if data, err := client.Get(table, "row1", "cf:a", attributes); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)
		}

		fmt.Print("GetVer:")
		if data, err := client.GetVer(table, "row1", "cf:a", 10, attributes); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)

			//0 : value1 ; 1362555589004
			//1 : value1 ; 1362463859431
		}

		fmt.Print("GetVerTs:")
		if data, err := client.GetVerTs(table, "row1", "cf:a", timestamp, 10, attributes); err != nil {
			fmt.Println(err)
		} else {
			printCells(data)
		}

		fmt.Print("GetRow:")
		if data, err := client.GetRow(table, "row1", nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowWithColumns:")
		if data, err := client.GetRowWithColumns(table, "row1", columns, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowTs:")
		if data, err := client.GetRowTs(table, "row1", timestamp, nil); err != nil {
			fmt.Println(err)
		} else {
			printRows(data)
		}

		fmt.Print("GetRowWithColumnsTs:")
		if data, err := client.GetRowWithColumnsTs(table, "row1", columns, timestamp, nil); err != nil {
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
	}

}

func printTList(list thrift.TList) {

	fmt.Println("printTList")
	fmt.Println("Len()", list.Len())

	l := list.Len()
	fmt.Println("[")
	for i := 0; i < l; i++ {
		fmt.Println(i, ":", list.At(i))
	}
	fmt.Println("]")

}

func dump(data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json.Marshal error:", err)
		return
	}
	fmt.Println(string(b))
}

func printCells(data []*goh.TCell) {
	if data == nil {
		fmt.Println("<nil>")
	}

	l := len(data)
	fmt.Println("len:", l)
	for i, x := range data {
		fmt.Println(i, ":", string(x.Value), ";", x.Timestamp)
	}

}

func printRows(data []*goh.TRowResult) {
	if data == nil {
		fmt.Println("<nil>")
	}

	l := len(data)
	fmt.Println("rows len:", l)
	for i, x := range data {
		fmt.Println(i, string(x.Row), "[")
		for k, v := range x.Columns {
			fmt.Println(k, v.Value, v.Timestamp)
		}
		fmt.Println("]")
	}

}
