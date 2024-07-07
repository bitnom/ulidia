# Ulidia

Ulidia is a robust, PostgreSQL-native implementation of Universally Unique Lexicographically Sortable Identifiers (ULIDs) designed for use with Supabase and other PostgreSQL-based projects.

## Features

- Efficient 16-byte storage using a custom `ulid` type based on UUID
- ULID generation with cryptographically secure randomness via `pgcrypto`
- Conversion between ULID and its 26-character string representation
- Timestamp extraction from ULIDs
- Lexicographic sorting and efficient indexing
- Comprehensive test suite

## Installation

1. Ensure you have PostgreSQL 9.6 or later and the `pgcrypto` extension available.
2. Run the `ulidia.sql` script in your PostgreSQL database:

   ```sql
   \i path/to/ulidia.sql
   ```

## Usage

After installation, you can use ULIDs in your database:

```sql
-- Create a table with a ULID primary key
CREATE TABLE users (
  id ulid PRIMARY KEY DEFAULT generate_ulid(),
  name TEXT NOT NULL
);

-- Insert a record
INSERT INTO users (name) VALUES ('Alice');

-- Query using ULID
SELECT * FROM users WHERE id = '01F8MECHZCP3RP0AQCCPD0JQBF'::ulid;

-- Get string representation
SELECT ulid_to_string(id) FROM users;

-- Get timestamp
SELECT ulid_to_timestamp(id) FROM users;
```

## Functions

- `generate_ulid()`: Generate a new ULID
- `generate_ulid_at(timestamp_ms BIGINT)`: Generate a ULID with a specific timestamp
- `ulid_to_string(u ulid)`: Convert ULID to string
- `string_to_ulid(s text)`: Convert string to ULID
- `ulid_to_timestamp(u ulid)`: Extract timestamp from ULID
- `is_valid_ulid(s text)`: Validate ULID string
- `compare_ulids(a ulid, b ulid)`: Compare two ULIDs

## Testing

Run the included test suite:

```sql
SELECT * FROM test_ulid_functions();
```

## Limitations

- This implementation does not guarantee strict monotonicity in high-concurrency scenarios.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the [ULID spec](https://github.com/ulid/spec)
- Uses PostgreSQL's `pgcrypto` for secure random number generation
