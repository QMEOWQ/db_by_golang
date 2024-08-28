## A database by golang

### all tests can be passed.

### The implementation order is:

```
1. recovery_mgr ->
2. concurr_mgr ->
3. entry_record_mgr ->
4. view_mgr ->
5. sql_lexer ->
6. parser ->
7. select_parser ->
8. create_parser ->
9. insert_delete_update_parser ->
10. planner ->
11. hash_index
```

### notion:

Before finish recovery_mgr,
We has Finished necessary parts such as file_mgr, log_mgr, log_mgr, and transaction .

after_finish_recovery_mgr is put in main branch, other Iterated versions are put in other branches.

cmd template: 
  git commit -m "commit after_finish_recovery_mgr version in main branch"

