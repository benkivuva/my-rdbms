# Simple RDBMS in Go

A lightweight, educational Relational Database Management System built from scratch in Go. This project demonstrates core database concepts including page-based storage, B-Tree indexing, and a Volcano-style query executor.

## Features

- **Page-Based Storage**: 4KB fixed-size pages with a buffer pool for caching
- **Slotted Page Layout**: Variable-length tuple storage with efficient space management
- **B-Tree Index**: O(log n) primary key lookups with leaf node splitting
- **SQL Parser**: Recursive descent parser supporting basic SQL statements
- **Volcano Executor**: Pull-based query execution model
- **Interactive REPL**: Command-line interface for SQL queries
- **REST API**: HTTP endpoint for remote query execution

## Project Structure

```
my-rdbms/
├── cmd/rdbms/           # Application entry points
│   ├── main.go          # REPL and server startup
│   └── repl.go          # Interactive shell logic
├── internal/
│   ├── storage/         # Disk and memory management
│   │   ├── page.go          # Page definition (4KB)
│   │   ├── disk_manager.go  # File I/O operations
│   │   ├── buffer_pool.go   # LRU page cache
│   │   ├── slotted_page.go  # Tuple layout within pages
│   │   ├── table_heap.go    # Linked list of pages
│   │   └── rid.go           # Record identifier
│   ├── index/           # B-Tree implementation
│   │   ├── btree.go         # Tree operations
│   │   └── btree_node.go    # Node structure
│   ├── sql/             # SQL parsing
│   │   ├── lexer.go         # Tokenizer
│   │   ├── parser.go        # AST builder
│   │   └── ast.go           # Statement definitions
│   └── executor/        # Query execution
│       ├── executor.go      # Executor interface
│       └── nodes.go         # SeqScan, Insert, Filter
└── go.mod
```

## Quick Start

### Run the REPL

```bash
go run cmd/rdbms/*.go
```

Example session:
```sql
db> INSERT INTO users VALUES (1)
INSERT OK
db> INSERT INTO users VALUES (42)
INSERT OK
db> SELECT * FROM users
----------------
[1]
[42]
(2 rows)
db> exit
```

### Run as HTTP Server

```bash
go run cmd/rdbms/*.go server
```

Query via curl:
```bash
curl -X POST -d "q=INSERT INTO demo VALUES (123)" http://localhost:8080/query
curl -X POST -d "q=SELECT * FROM demo" http://localhost:8080/query
```

## Supported SQL

| Statement | Syntax |
|-----------|--------|
| INSERT | `INSERT INTO table VALUES (value1, value2, ...)` |
| SELECT | `SELECT * FROM table [WHERE column = value]` |
| CREATE TABLE | `CREATE TABLE name (col1 INT, col2 VARCHAR)` |

## Running Tests

```bash
go test -v ./...
```

## Architecture Overview

### Storage Layer
The storage layer manages persistence through a hierarchy of abstractions:
- **DiskManager**: Handles raw file I/O for 4KB pages
- **BufferPool**: Caches frequently accessed pages in memory
- **SlottedPage**: Organizes variable-length tuples within a page
- **TableHeap**: Links multiple pages together for table storage

### Index Layer
B-Tree index provides efficient key lookups:
- Leaf nodes store (key, RID) pairs
- Internal nodes store (key, child_page_id) pairs
- Automatic leaf splitting when full

### Execution Layer
Volcano-style pull model:
- Each operator implements `Init()`, `Next()`, `Close()`
- Data flows upward through operator tree
- Supports filtering via WHERE clauses

## Limitations

- No transaction support (no ACID guarantees)
- No concurrent query execution
- B-Tree index not persisted across restarts
- Single table per database file
- No UPDATE or DELETE operations

## 🚀 Roadmap & Future Work

### Phase 1: Persistence & Reliability
- [ ] **Catalog Persistence**: Store table schemas and B-Tree root IDs in a dedicated Metadata Page (Page 0).
- [ ] **B-Tree Serialization**: Implement `Serialize/Deserialize` for B-Tree nodes to ensure indexes survive REPL restarts.
- [ ] **Write-Ahead Logging (WAL)**: Basic redo-logging to provide atomicity in case of crashes.

### Phase 2: SQL Enhancements
- [ ] **Type System**: Add validation for `VARCHAR` length and `INT` ranges during insertion.
- [ ] **Delete & Update**: Implement `DeleteExecutor` and `UpdateExecutor` using the RID-based access pattern.
- [ ] **Joins**: Implement Nested Loop Join (NLJ) support.

### Phase 3: Performance
- [ ] **Index Scan**: Update the planner to use `IndexScanExecutor` instead of `SeqScan` when a filter matches the primary key.
- [ ] **Query Optimizer**: Rule-based optimization for predicate pushdown.

## License

MIT License
