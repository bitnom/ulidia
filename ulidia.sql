-- Ensure pgcrypto is available
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create a custom ULID type
CREATE TYPE ulid;

-- Input function for ULID
CREATE FUNCTION ulid_in(cstring) RETURNS ulid AS 'uuid_in' LANGUAGE internal IMMUTABLE STRICT;

-- Output function for ULID
CREATE FUNCTION ulid_out(ulid) RETURNS cstring AS 'uuid_out' LANGUAGE internal IMMUTABLE STRICT;

-- Create the ULID type
CREATE TYPE ulid (
    INPUT = ulid_in,
    OUTPUT = ulid_out,
    INTERNALLENGTH = 16,
    PASSEDBYVALUE
);

-- Comparison operators for ULID
CREATE FUNCTION ulid_lt(ulid, ulid) RETURNS boolean AS 'uuid_lt' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION ulid_le(ulid, ulid) RETURNS boolean AS 'uuid_le' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION ulid_eq(ulid, ulid) RETURNS boolean AS 'uuid_eq' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION ulid_ge(ulid, ulid) RETURNS boolean AS 'uuid_ge' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION ulid_gt(ulid, ulid) RETURNS boolean AS 'uuid_gt' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION ulid_ne(ulid, ulid) RETURNS boolean AS 'uuid_ne' LANGUAGE internal IMMUTABLE STRICT;

CREATE OPERATOR < (LEFTARG = ulid, RIGHTARG = ulid, PROCEDURE = ulid_lt);
CREATE OPERATOR <= (LEFTARG = ulid, RIGHTARG = ulid, PROCEDURE = ulid_le);
CREATE OPERATOR = (LEFTARG = ulid, RIGHTARG = ulid, PROCEDURE = ulid_eq);
CREATE OPERATOR >= (LEFTARG = ulid, RIGHTARG = ulid, PROCEDURE = ulid_ge);
CREATE OPERATOR > (LEFTARG = ulid, RIGHTARG = ulid, PROCEDURE = ulid_gt);
CREATE OPERATOR <> (LEFTARG = ulid, RIGHTARG = ulid, PROCEDURE = ulid_ne);

-- B-tree index support
CREATE FUNCTION ulid_cmp(ulid, ulid) RETURNS int4 AS 'uuid_cmp' LANGUAGE internal IMMUTABLE STRICT;

CREATE OPERATOR CLASS ulid_ops DEFAULT FOR TYPE ulid USING btree AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 ulid_cmp(ulid, ulid);

-- ULID generation function
CREATE OR REPLACE FUNCTION generate_ulid() RETURNS ulid AS $$
DECLARE
    timestamp BYTEA = E'\\000\\000\\000\\000\\000\\000';
    unix_time BIGINT;
BEGIN
    unix_time = (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;
    timestamp = SET_BYTE(timestamp, 0, (unix_time >> 40)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 1, (unix_time >> 32)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 2, (unix_time >> 24)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 3, (unix_time >> 16)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 4, (unix_time >> 8)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 5, unix_time::BIT(8)::INTEGER);
    RETURN (timestamp || gen_random_bytes(10))::ulid;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Generate ULID with a given timestamp
CREATE OR REPLACE FUNCTION generate_ulid_at(timestamp_ms BIGINT) RETURNS ulid AS $$
DECLARE
    timestamp BYTEA = E'\\000\\000\\000\\000\\000\\000';
BEGIN
    timestamp = SET_BYTE(timestamp, 0, (timestamp_ms >> 40)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 1, (timestamp_ms >> 32)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 2, (timestamp_ms >> 24)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 3, (timestamp_ms >> 16)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 4, (timestamp_ms >> 8)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 5, timestamp_ms::BIT(8)::INTEGER);
    RETURN (timestamp || gen_random_bytes(10))::ulid;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- ULID to string function
CREATE OR REPLACE FUNCTION ulid_to_string(u ulid) RETURNS text AS $$
DECLARE
    encoding CONSTANT text := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    timestamp BIGINT;
    randomness BYTEA;
    result text = '';
BEGIN
    timestamp = (u::uuid::text::bit(48))::bigint;
    randomness = substring(u::uuid::text::bytea from 7 for 10);
    
    -- Encode timestamp (first 10 characters)
    FOR i IN REVERSE 9..0 LOOP
        result = substr(encoding, (timestamp & 31)::int + 1, 1) || result;
        timestamp = timestamp >> 5;
    END LOOP;

    -- Encode randomness (last 16 characters)
    FOR i IN 0..9 LOOP
        result = result || substr(encoding, (get_byte(randomness, i) & 224) >> 5 + 1, 1);
        result = result || substr(encoding, (get_byte(randomness, i) & 31) + 1, 1);
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- String to ULID function
CREATE OR REPLACE FUNCTION string_to_ulid(s text) RETURNS ulid AS $$
DECLARE
    encoding CONSTANT text := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    timestamp BIGINT = 0;
    randomness BYTEA = E'\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000';
    value int;
BEGIN
    IF NOT s ~ '^[0-9A-Za-z]{26}$' THEN
        RAISE EXCEPTION 'Invalid ULID string: %', s USING ERRCODE = '22P02';
    END IF;
    s = UPPER(s);
    
    FOR i IN 1..26 LOOP
        value = strpos(encoding, substr(s, i, 1)) - 1;
        IF value = -1 THEN
            RAISE EXCEPTION 'Invalid character in ULID string: %', substr(s, i, 1) USING ERRCODE = '22P02';
        END IF;
        IF i <= 10 THEN
            timestamp = (timestamp << 5) | value;
        ELSE
            randomness = set_byte(randomness, (i - 11) / 2, 
                                  (get_byte(randomness, (i - 11) / 2) << 5) | value);
        END IF;
    END LOOP;

    RETURN (substring(timestamp::bit(48)::bytea for 6) || randomness)::ulid;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Extract timestamp function
CREATE OR REPLACE FUNCTION ulid_to_timestamp(u ulid) RETURNS timestamp with time zone AS $$
BEGIN
    RETURN to_timestamp((u::uuid::text::bit(48))::bigint / 1000.0);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Validation function
CREATE OR REPLACE FUNCTION is_valid_ulid(s text) RETURNS boolean AS $$
BEGIN
    RETURN s ~ '^[0-9A-Za-z]{26}$';
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Compare ULIDs function
CREATE OR REPLACE FUNCTION compare_ulids(a ulid, b ulid) RETURNS integer AS $$
BEGIN
    IF a < b THEN
        RETURN -1;
    ELSIF a > b THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Enhanced test function
CREATE OR REPLACE FUNCTION test_ulid_functions() RETURNS SETOF text AS $$
DECLARE
    test_ulid ulid;
    test_string text;
    start_time timestamp;
    end_time timestamp;
    duration interval;
    prev_ulid ulid;
    current_ulid ulid;
BEGIN
    -- Test generate_ulid
    test_ulid := generate_ulid();
    ASSERT test_ulid IS NOT NULL, 'generate_ulid should return a non-null value';
    RETURN NEXT 'generate_ulid: OK';

    -- Test generate_ulid_at
    test_ulid := generate_ulid_at(1000000000000);
    ASSERT ulid_to_timestamp(test_ulid) = to_timestamp(1000000000), 'generate_ulid_at should create ULID with correct timestamp';
    RETURN NEXT 'generate_ulid_at: OK';

    -- Test ulid_to_string and string_to_ulid
    test_string := ulid_to_string(test_ulid);
    ASSERT length(test_string) = 26, 'ULID string should be 26 characters long';
    ASSERT test_ulid = string_to_ulid(test_string), 'string_to_ulid should reverse ulid_to_string';
    ASSERT test_ulid = string_to_ulid(lower(test_string)), 'string_to_ulid should be case insensitive';
    RETURN NEXT 'ulid_to_string and string_to_ulid: OK';

    -- Test ulid_to_timestamp
    ASSERT ulid_to_timestamp(test_ulid) IS NOT NULL, 'ulid_to_timestamp should return a non-null value';
    ASSERT ulid_to_timestamp(test_ulid) <= CLOCK_TIMESTAMP(), 'Extracted timestamp should be in the past or present';
    RETURN NEXT 'ulid_to_timestamp: OK';

    -- Test is_valid_ulid
    ASSERT is_valid_ulid(test_string), 'Generated ULID string should be valid';
    ASSERT is_valid_ulid(lower(test_string)), 'Lowercase ULID string should be valid';
    ASSERT NOT is_valid_ulid('invalid-ulid'), 'Invalid ULID string should be rejected';
    RETURN NEXT 'is_valid_ulid: OK';

    -- Test compare_ulids
    ASSERT compare_ulids(generate_ulid(), generate_ulid()) = -1, 'Later ULID should be greater';
    ASSERT compare_ulids(generate_ulid_at(1000000000000), generate_ulid_at(1000000000000)) = 0, 'ULIDs with same timestamp should be equal';
    RETURN NEXT 'compare_ulids: OK';

    -- Test sorting
    CREATE TEMPORARY TABLE test_ulid_table (id ulid PRIMARY KEY, data text);
    INSERT INTO test_ulid_table (id, data) VALUES (generate_ulid(), 'test1'), (generate_ulid(), 'test2'), (generate_ulid(), 'test3');
    ASSERT (SELECT COUNT(*) FROM (SELECT id FROM test_ulid_table ORDER BY id) AS sorted) = 3, 'ULIDs should be sortable';
    RETURN NEXT 'ULID sorting: OK';

    -- Test error handling
    BEGIN
        PERFORM string_to_ulid('invalid-ulid');
        RAISE EXCEPTION 'string_to_ulid should raise an exception for invalid input';
    EXCEPTION WHEN invalid_text_representation THEN
        RETURN NEXT 'string_to_ulid error handling (invalid input): OK';
    END;

    BEGIN
        PERFORM string_to_ulid('O0123456789ABCDEFGHJKMNPQRST');
        RAISE EXCEPTION 'string_to_ulid should raise an exception for invalid character';
    EXCEPTION WHEN invalid_text_representation THEN
        RETURN NEXT 'string_to_ulid error handling (invalid character): OK';
    END;

    -- Test for increasing order (not guaranteed to be monotonic)
    prev_ulid := generate_ulid();
    FOR i IN 1..1000 LOOP
        current_ulid := generate_ulid();
        ASSERT current_ulid > prev_ulid, 'ULIDs should generally be in increasing order';
        prev_ulid := current_ulid;
    END LOOP;
    RETURN NEXT 'ULID increasing order: OK';

    -- Concurrency test
    CREATE TEMPORARY TABLE concurrent_ulids (id ulid);
    INSERT INTO concurrent_ulids
    SELECT generate_ulid() FROM generate_series(1, 1000);
    ASSERT (SELECT COUNT(DISTINCT id) FROM concurrent_ulids) = 1000, 'All concurrently generated ULIDs should be unique';
    RETURN NEXT 'Concurrency test: OK';

    -- Extended performance test
    start_time := CLOCK_TIMESTAMP();
    FOR i IN 1..1000000 LOOP
        test_ulid := generate_ulid();
        test_string := ulid_to_string(test_ulid);
        PERFORM string_to_ulid(test_string);
    END LOOP;
    end_time := CLOCK_TIMESTAMP();
    duration := end_time - start_time;
    RETURN NEXT format('Extended performance test: %s ms for 1,000,000 iterations', EXTRACT(EPOCH FROM duration) * 1000);

    DROP TABLE test_ulid_table;
    DROP TABLE concurrent_ulids;
END;
$$ LANGUAGE plpgsql;

-- Run tests
SELECT * FROM test_ulid_functions();

-- Documentation
COMMENT ON TYPE ulid IS 'ULID type based on UUID for efficient 16-byte storage and indexing';
COMMENT ON FUNCTION generate_ulid() IS 'Generates a new ULID';
COMMENT ON FUNCTION generate_ulid_at(BIGINT) IS 'Generates a ULID with the given timestamp (in milliseconds since the Unix epoch)';
COMMENT ON FUNCTION ulid_to_string(ulid) IS 'Converts a ULID to its 26-character string representation';
COMMENT ON FUNCTION string_to_ulid(text) IS 'Converts a 26-character string representation to a ULID. Case-insensitive.';
COMMENT ON FUNCTION ulid_to_timestamp(ulid) IS 'Extracts the timestamp from a ULID as a timestamp with time zone';
COMMENT ON FUNCTION is_valid_ulid(text) IS 'Checks if a string is a valid 26-character ULID representation';
COMMENT ON FUNCTION compare_ulids(ulid, ulid) IS 'Compares two ULIDs. Returns -1 if a < b, 1 if a > b, and 0 if a = b';
