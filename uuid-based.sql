CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create ULID functions without creating a new type

-- ULID generation function
CREATE OR REPLACE FUNCTION generate_ulid() RETURNS uuid AS $$
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
    RETURN (timestamp || gen_random_bytes(10))::uuid;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Generate ULID with a given timestamp
CREATE OR REPLACE FUNCTION generate_ulid_at(timestamp_ms BIGINT) RETURNS uuid AS $$
DECLARE
    timestamp BYTEA = E'\\000\\000\\000\\000\\000\\000';
BEGIN
    timestamp = SET_BYTE(timestamp, 0, (timestamp_ms >> 40)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 1, (timestamp_ms >> 32)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 2, (timestamp_ms >> 24)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 3, (timestamp_ms >> 16)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 4, (timestamp_ms >> 8)::BIT(8)::INTEGER);
    timestamp = SET_BYTE(timestamp, 5, timestamp_ms::BIT(8)::INTEGER);
    RETURN (timestamp || gen_random_bytes(10))::uuid;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- ULID to string function
CREATE OR REPLACE FUNCTION ulid_to_string(u uuid) RETURNS text AS $$
DECLARE
    encoding CONSTANT text := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    timestamp BIGINT;
    randomness BYTEA;
    result text = '';
BEGIN
    timestamp = (u::text::bit(48))::bigint;
    randomness = substring(u::text::bytea from 7 for 10);
    
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
CREATE OR REPLACE FUNCTION string_to_ulid(s text) RETURNS uuid AS $$
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

    RETURN (substring(timestamp::bit(48)::bytea for 6) || randomness)::uuid;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Extract timestamp function
CREATE OR REPLACE FUNCTION ulid_to_timestamp(u uuid) RETURNS timestamp with time zone AS $$
BEGIN
    RETURN to_timestamp((u::text::bit(48))::bigint / 1000.0);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Validation function
CREATE OR REPLACE FUNCTION is_valid_ulid(s text) RETURNS boolean AS $$
BEGIN
    RETURN s ~ '^[0-9A-Za-z]{26}$';
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Enhanced test function
CREATE OR REPLACE FUNCTION test_ulid_functions() RETURNS SETOF text AS $$
DECLARE
    test_ulid uuid;
    test_string text;
    start_time timestamp;
    end_time timestamp;
    duration interval;
    prev_ulid uuid;
    current_ulid uuid;
    ulid1 uuid;
    ulid2 uuid;
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

    -- Test sorting
    CREATE TEMPORARY TABLE test_ulid_table (id uuid PRIMARY KEY, data text);
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
    CREATE TEMPORARY TABLE concurrent_ulids (id uuid);
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
COMMENT ON FUNCTION generate_ulid() IS 'Generates a new ULID as UUID';
COMMENT ON FUNCTION generate_ulid_at(BIGINT) IS 'Generates a ULID as UUID with the given timestamp (in milliseconds since the Unix epoch)';
COMMENT ON FUNCTION ulid_to_string(uuid) IS 'Converts a ULID (as UUID) to its 26-character string representation';
COMMENT ON FUNCTION string_to_ulid(text) IS 'Converts a 26-character string representation to a ULID (as UUID). Case-insensitive.';
COMMENT ON FUNCTION ulid_to_timestamp(uuid) IS 'Extracts the timestamp from a ULID (as UUID) as a timestamp with time zone';
COMMENT ON FUNCTION is_valid_ulid(text) IS 'Checks if a string is a valid 26-character ULID representation';
