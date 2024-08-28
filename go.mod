module db_by_golang

go 1.20

replace file_manager => ./file_manager

replace log_manager => ./log_manager

replace buffer_manager => ./buffer_manager

replace transaction => ./transaction

replace entry_record_manager => ./entry_record_manager

replace metadata_manager => ./metadata_manager

replace lexer => ./lexer

replace parser => ./parser

replace query => ./query

require (
	lexer v0.0.0-00010101000000-000000000000
	parser v0.0.0-00010101000000-000000000000
	query v0.0.0-00010101000000-000000000000
)

require (
	buffer_manager v0.0.0-00010101000000-000000000000 // indirect
	entry_record_manager v0.0.0-00010101000000-000000000000 // indirect
	file_manager v0.0.0-00010101000000-000000000000 // indirect
	log_manager v0.0.0-00010101000000-000000000000 // indirect
	transaction v0.0.0-00010101000000-000000000000 // indirect
)
