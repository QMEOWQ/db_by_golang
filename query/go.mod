module query

replace lexer => ../lexer

replace query => ../query

replace parser => ../parser

replace file_manager => ../file_manager

replace log_manager => ../log_manager

replace buffer_manager => ../buffer_manager

replace transaction => ../transaction

replace entry_record_manager => ../entry_record_manager

replace metadata_manager => ../metadata_manager

go 1.20

require (
	buffer_manager v0.0.0-00010101000000-000000000000
	entry_record_manager v0.0.0-00010101000000-000000000000
	file_manager v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.9.0
	log_manager v0.0.0-00010101000000-000000000000
	transaction v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/exp v0.0.0-20240808152545-0cdaa3abc0fa
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
