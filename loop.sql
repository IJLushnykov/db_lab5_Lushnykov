DROP TABLE IF EXISTS Developer CASCADE;

CREATE TABLE Developer (
                            Developer_ID SERIAL PRIMARY KEY,
                            Developer VARCHAR(255)
                        );
						

DO $$ 
DECLARE
  i INT := 1;
  test_id INT;
BEGIN
  LOOP
    EXIT WHEN i > 10;
    INSERT INTO Developer (Developer)
    VALUES
      (i);
    i := i + 1;
  END LOOP;

END $$;

SELECT * FROM Developer;