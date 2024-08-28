package parser

import "fmt"

type ViewData struct {
	view_name  string
	query_data *QueryData
}

func NewViewData(view_name string, query_data *QueryData) *ViewData {
	return &ViewData{
		view_name:  view_name,
		query_data: query_data,
	}
}

func (vd *ViewData) ViewName() string {
	return vd.view_name
}

func (vd *ViewData) ViewDef() string {
	return vd.query_data.ToString()
}

func (vd *ViewData) ToString() string {
	str := fmt.Sprintf("view_name: %s, view_def: %s", vd.view_name, vd.ViewDef())
	return str
}
