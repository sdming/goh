package main

import (
	"encoding/json"
	"fmt"
	"github.com/sdming/goh"
)

func main() {

	host, port := "192.168.17.129", "9090"
	fmt.Println(host, ":", port)

	client, err := goh.NewTcpClient(host, port, goh.TBinaryProtocol, false)
	if err != nil {
		fmt.Println(err)
		return
	}

	table := "test"
	fmt.Println("table:", table)

	// fmt.Print("IsTableEnabled:")
	// fmt.Println(client.IsTableEnabled(table))

	// fmt.Print("DisableTable:")
	// fmt.Println(client.DisableTable(table))

	// fmt.Print("IsTableEnabled:")
	// fmt.Println(client.IsTableEnabled(table))

	// fmt.Print("EnableTable:")
	// fmt.Println(client.EnableTable(table))

	// fmt.Print("IsTableEnabled:")
	// fmt.Println(client.IsTableEnabled(table))

	// fmt.Print("Compact:")
	// fmt.Println(client.Compact(table))

	// fmt.Print("MajorCompact:")
	// fmt.Println(client.Compact(table))

	// fmt.Print("GetTableNames:")
	// fmt.Println(client.GetTableNames())

	// fmt.Print("GetColumnDescriptors:")
	// fmt.Println(client.GetColumnDescriptors(table))

	fmt.Print("GetTableRegions:")
	regions, err := client.GetTableRegions(table)
	fmt.Println(err)
	for _, x := range regions {
		dump(x)
	}

	// argvalue0 := "test"
	// value0 := Hbase.Text(argvalue0)
	// argvalue1 := "row1"
	// value1 := Hbase.Text(argvalue1)
	// value2 := thrift.NewTMapDefault()

	// ret, e1, e2 := client.GetRow(value0, value1, value2)
	// fmt.Println(e1)
	// fmt.Println(e2)

	// fmt.Println("ElemType", ret.ElemType())
	// fmt.Println("Len", ret.Len())

	// for i := 0; i < ret.Len(); i++ {
	// 	item := ret.At(i).(*Hbase.TRowResult)
	// 	fmt.Printf("row %s \n ", item.Row)

	// 	printTmap(item.Columns)

	// }

	// argvalue0 := "test"
	// value0 := Hbase.Text(argvalue0)
	// argvalue1 := ""
	// value1 := Hbase.Text(argvalue1)
	// argvalue2 := "cf"
	// value2 := Hbase.Text(argvalue2)
	// ret, e1, e2 := client.GetRowOrBefore(value0, value1, value2)

	// fmt.Println(e1)
	// fmt.Println(e2)

	// fmt.Println("ElemType", ret.ElemType())
	// fmt.Println("Len", ret.Len())

	// for i := 0; i < ret.Len(); i++ {
	// 	item := ret.At(i)
	// 	fmt.Printf("row %#v \n ", item)
	// }

	// fmt.Print("\n")

	// argvalue0 := "test"
	// value0 := Hbase.Bytes(argvalue0)
	// fmt.Print(client.IsTableEnabled(value0))
	// fmt.Print("\n")

	// argvalue0 := "test"
	// value0 := Hbase.Text(argvalue0)
	// ret, e1, e2 := client.GetColumnDescriptors(value0)
	// fmt.Println(e1)
	// fmt.Println(e2)
	// printTmap(ret)

	// ret, e1, e2 := client.GetTableNames()
	// fmt.Println("e1", e1)
	// fmt.Println("e2", e2)
	// fmt.Println("ret")

	// fmt.Println("ElemType", ret.ElemType())
	// fmt.Println("Len", ret.Len())

	// for i := 0; i < ret.Len(); i++ {
	// 	item := ret.At(i)
	// 	fmt.Println(item)
	// }

	// argvalue0 := "test"
	// value0 := Hbase.Text(argvalue0)
	// regions, _, _ := client.GetTableRegions(value0)
	// for i := 0; i < regions.Len(); i++ {
	// 	item := regions.At(i).(*Hbase.TRegionInfo)
	// 	fmt.Println(item)

	// 	fmt.Printf(" item.Name = %s \n", item.Name)
	// }

	//===============================
	// argvalue0 := "demo"
	// value0 := Hbase.Text(argvalue0)

	// column := Hbase.NewColumnDescriptor()
	// column.Name = Hbase.Text("col1")

	// value1 := thrift.NewTList(thrift.TypeFromValue(column), 1)
	// value1.Set(1, column)

	// fmt.Println(client.CreateTable(value0, value1))

	//===============================

}

func dump(data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Println("json.Marshal error:", err)
		return
	}
	fmt.Println(string(b))
}
