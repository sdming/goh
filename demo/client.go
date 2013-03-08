package main

import (
	"./Hbase"
	"fmt"
	"net"
	"thrift"
)

func main() {

	var trans thrift.TTransport
	addr, err := net.ResolveTCPAddr("tcp", "192.168.17.129:9090")
	if err != nil {
		fmt.Println(err)
		return
	}

	trans, err = thrift.NewTNonblockingSocketAddr(addr)
	// if framed {
	// 	trans = thrift.NewTFramedTransport(trans)
	// }

	protocol := ""

	var protocolFactory thrift.TProtocolFactory
	switch protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
		break
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
		break
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
		break
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
		break
	default:
		fmt.Println("Invalid protocol specified: ", protocol)
		return
	}

	client := Hbase.NewHbaseClientFactory(trans, protocolFactory)
	if err = trans.Open(); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(" trans.Open() ")

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
	argvalue0 := "demo"
	value0 := Hbase.Text(argvalue0)

	column := Hbase.NewColumnDescriptor()
	column.Name = Hbase.Text("col1")

	value1 := thrift.NewTList(thrift.TypeFromValue(column), 1)
	value1.Set(1, column)

	fmt.Println(client.CreateTable(value0, value1))

	//===============================

}

func printTmap(t thrift.TMap) {

	l := t.Len()
	keys := t.Keys()
	values := t.Values()

	fmt.Println("len", l)
	fmt.Println("keys", keys)
	fmt.Println("values", values)

	for i := 0; i < l; i++ {
		fmt.Println("key ", keys[i])
		fmt.Println("value ", values[i])
	}

}
