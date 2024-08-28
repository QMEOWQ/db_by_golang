package query

type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(scan1 Scan, scan2 Scan) *ProductScan {
	p := &ProductScan{
		scan1: scan1,
		scan2: scan2,
	}

	p.scan1.Next()

	return p
}

func (p *ProductScan) FirstQualify() {
	p.scan1.FirstQualify()
	p.scan1.Next()
	p.scan2.FirstQualify()
}

func (p *ProductScan) Next() bool {
	if p.scan2.Next() {
		return true
	} else {
		p.scan2.FirstQualify()
		return p.scan1.Next() && p.scan2.Next()
	}
}

func (p *ProductScan) GetInt(fld_name string) int {
	if p.scan1.HasField(fld_name) {
		return p.scan1.GetInt(fld_name)
	} else {
		return p.scan2.GetInt(fld_name)
	}
}

func (p *ProductScan) GetString(fld_name string) string {
	if p.scan1.HasField(fld_name) {
		return p.scan1.GetString(fld_name)
	} else {
		return p.scan2.GetString(fld_name)
	}
}

func (p *ProductScan) GetVal(fld_name string) *Constant {
	if p.scan1.HasField(fld_name) {
		return p.scan1.GetVal(fld_name)
	} else {
		return p.scan2.GetVal(fld_name)
	}
}

func (p *ProductScan) HasField(fld_name string) bool {
	return p.scan1.HasField(fld_name) || p.scan2.HasField(fld_name)
}

func (p *ProductScan) Close() {
	p.scan1.Close()
	p.scan2.Close()
}
