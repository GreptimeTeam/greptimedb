-- description: Test casting of NaN and inf values
-- to integer/decimal casts

SELECT 'nan'::FLOAT;

-- cannot cast nan, inf or -inf to these types
SELECT 'nan'::FLOAT::INT;
SELECT 'nan'::FLOAT::DECIMAL(4,1);
SELECT 'nan'::FLOAT::DECIMAL(9,1);
SELECT 'nan'::FLOAT::DECIMAL(18,1);
SELECT 'nan'::FLOAT::DECIMAL(38,1);

SELECT 'inf'::FLOAT;

-- cannot cast nan, inf or -inf to these types
SELECT 'inf'::FLOAT::INT;
SELECT 'inf'::FLOAT::DECIMAL(4,1);
SELECT 'inf'::FLOAT::DECIMAL(9,1);
SELECT 'inf'::FLOAT::DECIMAL(18,1);
SELECT 'inf'::FLOAT::DECIMAL(38,1);

SELECT '-inf'::FLOAT;

-- cannot cast nan, inf or -inf to these types
SELECT '-inf'::FLOAT::INT;
SELECT '-inf'::FLOAT::DECIMAL(4,1);
SELECT '-inf'::FLOAT::DECIMAL(9,1);
SELECT '-inf'::FLOAT::DECIMAL(18,1);
SELECT '-inf'::FLOAT::DECIMAL(38,1);

SELECT 'nan'::DOUBLE;

-- cannot cast nan, inf or -inf to these types
SELECT 'nan'::DOUBLE::INT;
SELECT 'nan'::DOUBLE::DECIMAL(4,1);
SELECT 'nan'::DOUBLE::DECIMAL(9,1);
SELECT 'nan'::DOUBLE::DECIMAL(18,1);
SELECT 'nan'::DOUBLE::DECIMAL(38,1);

SELECT 'inf'::DOUBLE;

-- cannot cast nan, inf or -inf to these types
SELECT 'inf'::DOUBLE::INT;
SELECT 'inf'::DOUBLE::DECIMAL(4,1);
SELECT 'inf'::DOUBLE::DECIMAL(9,1);
SELECT 'inf'::DOUBLE::DECIMAL(18,1);
SELECT 'inf'::DOUBLE::DECIMAL(38,1);

SELECT '-inf'::DOUBLE;

-- cannot cast nan, inf or -inf to these types
SELECT '-inf'::DOUBLE::INT;
SELECT '-inf'::DOUBLE::DECIMAL(4,1);
SELECT '-inf'::DOUBLE::DECIMAL(9,1);
SELECT '-inf'::DOUBLE::DECIMAL(18,1);
SELECT '-inf'::DOUBLE::DECIMAL(38,1);

-- we can cast nan, inf and -inf between floats and doubles, as well as to/from strings
SELECT 'nan'::FLOAT::VARCHAR;
SELECT 'nan'::DOUBLE::VARCHAR;
SELECT 'nan'::VARCHAR::FLOAT;
SELECT 'nan'::VARCHAR::DOUBLE;
SELECT 'inf'::FLOAT::VARCHAR;
SELECT 'inf'::DOUBLE::VARCHAR;
SELECT 'inf'::VARCHAR::FLOAT;
SELECT 'inf'::VARCHAR::DOUBLE;
SELECT '-inf'::FLOAT::VARCHAR;
SELECT '-inf'::DOUBLE::VARCHAR;
SELECT '-inf'::VARCHAR::FLOAT;
SELECT '-inf'::VARCHAR::DOUBLE;
