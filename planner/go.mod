module planner

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
	entry_record_manager v0.0.0-00010101000000-000000000000
	parser v0.0.0-00010101000000-000000000000
	transaction v0.0.0-00010101000000-000000000000
)

require (
	buffer_manager v0.0.0-00010101000000-000000000000 // indirect
	file_manager v0.0.0-00010101000000-000000000000 // indirect
	lexer v0.0.0-00010101000000-000000000000 // indirect
	log_manager v0.0.0-00010101000000-000000000000 // indirect
	metadata_manager v0.0.0-00010101000000-000000000000
	query v0.0.0-00010101000000-000000000000
)
