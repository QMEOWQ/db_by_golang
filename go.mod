module db_by_golang

go 1.20

replace file_manager => ./file_manager

replace log_manager => ./log_manager

replace buffer_manager => ./buffer_manager

replace transaction => ./transaction

require (
	buffer_manager v0.0.0-00010101000000-000000000000
	file_manager v0.0.0-00010101000000-000000000000
	log_manager v0.0.0-00010101000000-000000000000
	transaction v0.0.0-00010101000000-000000000000
)
