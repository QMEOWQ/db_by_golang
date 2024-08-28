package entry_record_manager

type FIELD_TYPE int

const (
	INTEGER FIELD_TYPE = iota
	VARCHAR
	//BLOB
)

type FieldInfo struct {
	field_type FIELD_TYPE
	length     int
}

func newFieldInfo(field_type FIELD_TYPE, length int) *FieldInfo {
	return &FieldInfo{
		field_type: field_type,
		length:     length,
	}
}

type Schema struct {
	fields []string
	info   map[string]*FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]*FieldInfo),
	}
}

//实现以下接口
// AddField(field_name string, field_type FIELD_TYPE, length int)
// 	AddIntField(field_name string, length int)
// 	AddStringField(field_name string, length int)
// 	Add(field_name string, sch SchemaInterface)
// 	AddAll(sch SchemaInterface)
// 	Fields() []string
// 	HasFields(field_name string) bool
// 	Type(field_name string) FIELD_TYPE
// 	Length(field_name string) int

func (s *Schema) AddField(field_name string, field_type FIELD_TYPE, length int) {
	s.fields = append(s.fields, field_name)
	s.info[field_name] = newFieldInfo(field_type, length)
}

func (s *Schema) AddIntField(field_name string) {
	s.AddField(field_name, INTEGER, 0)
}

func (s *Schema) AddStringField(field_name string, length int) {
	s.AddField(field_name, VARCHAR, length)
}

func (s *Schema) Add(field_name string, sch SchemaInterface) {
	field_type := sch.Type(field_name)
	length := sch.Length(field_name)
	s.AddField(field_name, field_type, length)
}

func (s *Schema) AddAll(sch SchemaInterface) {
	fields := sch.Fields()
	for _, field_name := range fields {
		s.Add(field_name, sch)
	}
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) Type(field_name string) FIELD_TYPE {
	return s.info[field_name].field_type
}

func (s *Schema) Length(field_name string) int {
	return s.info[field_name].length
}

func (s *Schema) HasField(field_name string) bool {
	for _, val := range s.fields {
		if val == field_name {
			return true
		}
	}

	return false
}

func (s *Schema) HasFields(field_name string) bool {
	for _, val := range s.fields {
		if val != field_name {
			return true
		}
	}

	return false
}
