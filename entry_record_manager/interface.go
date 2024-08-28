package entry_record_manager

import (
	fm "file_manager"
)

type SchemaInterface interface {
	AddField(field_name string, field_type FIELD_TYPE, length int)
	AddIntField(field_name string)
	AddStringField(field_name string, length int)
	Add(field_name string, sch SchemaInterface)
	AddAll(sch SchemaInterface)
	Fields() []string
	HasFields(field_name string) bool
	Type(field_name string) FIELD_TYPE
	Length(field_name string) int
}

type LayoutInterface interface {
	Schema() SchemaInterface
	Offset(field_name string) int
	SlotSize() int
}

// 标志位为0代表可以删除
type EntryRecordManagerInterface interface {
	Block() *fm.BlockID                        //返回记录所在页面对应的区块
	GetInt(slot int, fld_name string) int      //根据给定字段名取出其对应的int值
	SetInt(slot int, fld_name string, val int) //根据给定字段名取出其对应的int值
	GetString(slot int, fld_name string) string
	SetString(slot int, fld_name string, val string)
	Format()                  //将所有插槽中的记录设定为默认值
	Delete(slot int)          //将给定插槽的占用标志位设置为0
	NextAfter(slot int) int   //查找给定插槽之后第一个占用标志位为1的记录
	InsertAfter(slot int) int //查找给定插槽之后第一个占用标志位为0的记录
}

type RIDInterface interface {
	BlockNumber() int //记录所在的区块号
	Slot() int        //记录的插槽号
	Equals(other RIDInterface) bool
	ToString() string
}

type TableScanInterface interface {
	Close()
	BeforeFirst() //指针指向第一个记录之前
	Next() bool   //指针指向下一个记录
	Insert()
	Delete()
	HasField(field_name string) bool

	GetInt(field_name string) int
	SetInt(field_name string, val int)
	GetString(field_name string) string
	SetString(field_name string, val string)

	CurrentRID() RIDInterface
}
