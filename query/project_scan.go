package query

type ProjectScan struct {
	scan      Scan
	fieldList []string
}

func NewProjectScan(scan Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		scan:      scan,
		fieldList: fieldList,
	}
}

func (p *ProjectScan) FirstQualify() {
	p.scan.FirstQualify()
}

func (p *ProjectScan) Next() bool {
	return p.scan.Next()
}

func (p *ProjectScan) GetInt(fld_name string) int {
	if p.scan.HasField(fld_name) {
		return p.scan.GetInt(fld_name)
	}

	panic("Field not found.")
}

func (p *ProjectScan) GetString(fld_name string) string {
	if p.scan.HasField(fld_name) {
		return p.scan.GetString(fld_name)
	}

	panic("Field not found.")
}

func (p *ProjectScan) GetVal(fld_name string) *Constant {
	if p.scan.HasField(fld_name) {
		return p.scan.GetVal(fld_name)
	}

	panic("Field not found.")
}

func (p *ProjectScan) HasField(fld_name string) bool {
	for _, field := range p.fieldList {
		if field == fld_name {
			return true
		}
	}

	return false
}

func (p *ProjectScan) Close() {
	p.scan.Close()
}
