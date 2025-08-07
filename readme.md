# Redifu

Redifu is a Go library for efficient Redis-backed pagination and sorting using Redis Sorted Sets. It provides a clean abstraction for handling paginated data with customizable page sizes and sorting directions, with MongoDB support.

## Features

- **Redis-backed Pagination** - Efficiently fetch data with cursor-based pagination
- **Sorted Data Sets** - Sort data in ascending or descending order
- **Flexible Item Storage** - Store items with automatic TTL management
- **MongoDB Integration** - Support for MongoDB ObjectIDs
- **Generic Implementation** - Type-safe operations using Go generics
- **Performance Optimized** - Designed for high-throughput applications

## Installation

```bash
go get github.com/yourusername/pageflow
```

### Pagination Flow

1. Items are stored in Redis using their random ID as key
2. Items are added to sorted sets with timestamps as scores
3. Pagination operates by fetching ranges from sorted sets
4. Page boundaries are tracked using first/last page markers
5. The current implementation only supports pagination based on creation date. Future releases will support not only the creation date, but any custom attribute.

### TTL Management

- Individual items: 7 days (configurable via `INDIVIDUAL_KEY_TTL`)
- Sorted sets: 2 days (configurable via `SORTED_SET_TTL`)
- TTLs are automatically extended on access

## License

[MIT License]