-- Example session showing a stream view:
--

-- hack=> select stream_subscribe('foo'::regclass);
--  stream_subscribe 
-- ------------------
--                 0
-- (1 row)
-- 
-- hack=> select * from foo_stream;
--  sensor | temperature 
-- --------+-------------
-- (0 rows)
-- 
-- hack=> insert into foo_stream values (1, 10), (2, 20);
-- INSERT 0 0
-- Asynchronous notification "foo" received from server process with PID 18168.
-- hack=> select * from foo_stream;
--  sensor | temperature 
-- --------+-------------
--       1 |       10.00
--       2 |       20.00
-- (2 rows)
-- 
-- hack=> select * from foo_stream;
--  sensor | temperature 
-- --------+-------------
-- (0 rows)
--
-- hack=> select stream_unsubscribe('foo'::regclass);
--  stream_unsubscribe 
-- --------------------
--  
-- (1 row)

-- Prototype implementation based on unlogged tables.

-- The following would be provided by the extension

CREATE UNLOGGED TABLE pg_stream_subscription (
  tuple_regclass regclass NOT NULL,
  backend_pid INTEGER NOT NULL,
  stream_counter INTEGER NOT NULL,
  PRIMARY KEY (tuple_regclass, backend_pid)
);

CREATE FUNCTION stream_create(tuple regclass) RETURNS INTEGER
  AS 'foo/bar', 'pg_stream_create'
  LANGUAGE C STRICT;

CREATE FUNCTION stream_drop(tuple regclass) RETURNS INTEGER
  AS 'foo/bar', 'pg_stream_drop'
  LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION stream_subscribe(tuple_class regclass) RETURNS INTEGER AS $$
DECLARE
  highwater INTEGER;
BEGIN
  EXECUTE 'SELECT COALESCE(MAX(stream_counter), 0) FROM ' || quote_ident(tuple_class::text || '_queue') INTO highwater;
  BEGIN
    INSERT INTO pg_stream_subscription (tuple_regclass, backend_pid, stream_counter)
    VALUES (tuple_class, pg_backend_pid(), highwater);
    EXCEPTION WHEN unique_violation THEN
      RAISE EXCEPTION 'Already subscribed';
  END;
  EXECUTE 'LISTEN ' || quote_ident(tuple_class::text);
  RETURN highwater;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION stream_unsubscribe(tuple_class regclass) RETURNS VOID AS $$
BEGIN
  DELETE FROM pg_stream_subscription
  WHERE tuple_regclass = tuple_class
  AND backend_pid = pg_backend_pid();
  EXECUTE 'UNLISTEN ' || quote_ident(tuple_class::text);
END;
$$ LANGUAGE plpgsql;

-- Here is an example of a composite type being declared:

CREATE TYPE foo AS (sensor INTEGER, temperature DECIMAL(4, 2));

-- pg_stream_create would generate the following objects:

CREATE UNLOGGED TABLE foo_queue (
  stream_counter SERIAL PRIMARY KEY,
  sensor INTEGER,
  temperature DECIMAL(4, 2)
);

CREATE OR REPLACE FUNCTION foo_read() RETURNS SETOF foo AS $$
DECLARE
  top_of_queue INTEGER;
  highwater INTEGER;
  trim_level INTEGER;
  r foo_tuple;
BEGIN
  BEGIN
    SELECT stream_counter INTO STRICT highwater
    FROM pg_stream_subscription
    WHERE backend_pid = pg_backend_pid()
    AND tuple_regclass = 'foo'::regclass;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'Not subscribed to stream foo';
  END;
  SELECT MAX(stream_counter) INTO STRICT top_of_queue
  FROM foo_queue;
  IF top_of_queue IS NULL THEN
    RETURN;
  END IF;
  SELECT COALESCE(MIN(stream_counter), top_of_queue) INTO trim_level
  FROM pg_stream_subscription
  WHERE backend_pid != pg_backend_pid()
  AND tuple_regclass = 'foo'::regclass;
  UPDATE pg_stream_subscription
  SET stream_counter = top_of_queue
  WHERE backend_pid = pg_backend_pid()
  AND tuple_regclass = 'foo'::regclass;
  FOR r IN SELECT sensor, temperature
           FROM foo_queue
           WHERE stream_counter > highwater
           AND stream_counter <= top_of_queue
           ORDER BY stream_counter
  LOOP
    RETURN NEXT r;
  END LOOP;
  DELETE FROM foo_queue 
  WHERE stream_counter <= trim_level;
  RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW foo_stream AS 
SELECT sensor, temperature
  FROM foo_read();

CREATE OR REPLACE FUNCTION foo_stream_trigger_fun() RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- in order to prevent sequence numbers being generated out of order
    -- in scenarios with multiple writers, we lock until commit; this
    -- is not great...
    PERFORM pg_advisory_xact_lock('foo'::regclass::oid::bigint);
    INSERT INTO foo_queue VALUES (DEFAULT, NEW.sensor, NEW.temperature);
    NOTIFY foo;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;  

CREATE TRIGGER foo_stream_trigger
INSTEAD OF INSERT ON foo_stream
FOR EACH ROW EXECUTE PROCEDURE foo_stream_trigger_fun();