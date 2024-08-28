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

replace planner => ./planner

require (
	buffer_manager v0.0.0-00010101000000-000000000000
	entry_record_manager v0.0.0-00010101000000-000000000000
	file_manager v0.0.0-00010101000000-000000000000
	log_manager v0.0.0-00010101000000-000000000000
	metadata_manager v0.0.0-00010101000000-000000000000
	parser v0.0.0-00010101000000-000000000000 // indirect
	planner v0.0.0-00010101000000-000000000000
	query v0.0.0-00010101000000-000000000000
	transaction v0.0.0-00010101000000-000000000000
)

require lexer v0.0.0-00010101000000-000000000000 // indirect
