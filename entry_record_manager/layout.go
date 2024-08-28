package entry_record_manager

import (
	fm "file_manager"
	ts "transaction"
)

const (
	BYTES_OF_INT = 8
)

type Layout struct {
	schema    SchemaInterface
	offsets   map[string]int
	slot_size int
}

func NewLayoutWithSchema(schema SchemaInterface) *Layout {
	layout := &Layout{
		schema:    schema,
		offsets:   make(map[string]int),
		slot_size: 0,
	}

	fields := schema.Fields()
	pos := ts.UINT64_LENGTH //使用一个int作为标志位，占8个字节
	for i := 0; i < len(fields); i++ {
		layout.offsets[fields[i]] = pos
		pos += layout.lengthInBytes(fields[i])
	}

	layout.slot_size = pos

	return layout
}

func NewLayout(schema SchemaInterface, offsets map[string]int, slot_size int) *Layout {
	return &Layout{
		schema:    schema,
		offsets:   offsets,
		slot_size: slot_size,
	}
}

func (l *Layout) Schema() SchemaInterface {
	return l.schema
}

func (l *Layout) Offset(field_name string) int {
	offset, ok := l.offsets[field_name]
	if !ok {
		return -1
	}

	return offset
}

func (l *Layout) SlotSize() int {
	return l.slot_size
}

func (l *Layout) lengthInBytes(field_name string) int {
	fld_type := l.schema.Type(field_name)
	p := fm.NewPageBySize(1)

	if fld_type == INTEGER {
		return BYTES_OF_INT
	} else {
		field_len := l.schema.Length(field_name)
		dummy_str := make([]byte, field_len)
		return int(p.MaxLengthForString(string(dummy_str)))
	}
}
