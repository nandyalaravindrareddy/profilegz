-- ==============================================
-- STEP 1: CREATE THE UDF FUNCTION
-- ==============================================
CREATE OR REPLACE FUNCTION `custom-plating-475002-j7.dlp_audit.redact_column`(
    input_value STRING,
    regex_pattern STRING,
    action_type STRING,
    replace_text STRING,
    mask_char STRING,
    keep_chars INT64
)
RETURNS STRING
AS ((
    SELECT 
        CASE 
            WHEN regex_pattern IS NOT NULL 
                 AND REGEXP_CONTAINS(input_value, regex_pattern) THEN
                CASE action_type
                    WHEN 'FULL_REDACT' THEN ''
                    WHEN 'REPLACE' THEN COALESCE(replace_text, '[REDACTED]')
                    WHEN 'MASK_LAST_N' THEN
                        CONCAT(
                            REPEAT(COALESCE(mask_char, 'X'), 
                                   GREATEST(0, LENGTH(input_value) - COALESCE(keep_chars, 4))),
                            SUBSTR(input_value, LENGTH(input_value) - COALESCE(keep_chars, 4) + 1)
                        )
                    WHEN 'MASK_ALL' THEN
                        REPEAT(COALESCE(mask_char, 'X'), LENGTH(input_value))
                    ELSE input_value
                END
            ELSE input_value
        END
));

-- ==============================================
-- STEP 2: CREATE THE STORED PROCEDURE
-- ==============================================
CREATE OR REPLACE PROCEDURE `custom-plating-475002-j7.dlp_audit.redact_table`(
    source_table STRING,
    target_table STRING,
    column_config_json STRING
)
BEGIN
    DECLARE sql_query STRING;
    
    -- Build the SQL query dynamically
    SET sql_query = (
        WITH configs AS (
            SELECT 
                JSON_EXTRACT_SCALAR(config, '$.column_name') as column_name,
                JSON_EXTRACT_SCALAR(config, '$.regex_pattern') as regex_pattern,
                JSON_EXTRACT_SCALAR(config, '$.action') as action,
                JSON_EXTRACT_SCALAR(config, '$.replace_text') as replace_text,
                JSON_EXTRACT_SCALAR(config, '$.mask_char') as mask_char,
                SAFE_CAST(JSON_EXTRACT_SCALAR(config, '$.keep_chars') AS INT64) as keep_chars
            FROM UNNEST(JSON_EXTRACT_ARRAY(column_config_json)) as config
        )
        SELECT CONCAT(
            'CREATE OR REPLACE TABLE `', target_table, '` AS\n',
            'SELECT\n',
            STRING_AGG(
                CASE 
                    WHEN cfg.column_name IS NOT NULL THEN
                        CONCAT(
                            '    `custom-plating-475002-j7.dlp_audit.redact_column`(`',
                            c.column_name,
                            '`, \'',
                            cfg.regex_pattern,
                            '\', \'',
                            cfg.action,
                            '\', \'',
                            COALESCE(cfg.replace_text, ''),
                            '\', \'',
                            COALESCE(cfg.mask_char, 'X'),
                            '\', ',
                            CAST(COALESCE(cfg.keep_chars, 4) AS STRING),
                            ') AS `', c.column_name, '`'
                        )
                    ELSE
                        CONCAT('    `', c.column_name, '`')
                END,
                ',\n'
                ORDER BY c.ordinal_position
            ),
            '\nFROM `', source_table, '`'
        )
        FROM `custom-plating-475002-j7.dlp_audit.INFORMATION_SCHEMA.COLUMNS` c
        LEFT JOIN configs cfg ON c.column_name = cfg.column_name
        WHERE CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name) = source_table
    );
    
    -- Execute the SQL
    EXECUTE IMMEDIATE sql_query;
    
    SELECT CONCAT('✅ Successfully created redacted table: ', target_table) as result;
END;

-- ==============================================
-- STEP 3: CREATE TEST DATA
-- ==============================================
CREATE OR REPLACE TABLE `custom-plating-475002-j7.dlp_audit.test_customers` AS
SELECT 
  1 as id,
  'John Doe' as name,
  'john.doe@example.com' as email,
  '123-45-6789' as ssn,
  '555-123-4567' as phone,
  '4111111111111111' as credit_card,
  '123 Main St' as address
UNION ALL
SELECT 
  2 as id,
  'Jane Smith' as name,
  'jane.smith@company.com' as email,
  '987-65-4321' as ssn,
  '(555) 987-6543' as phone,
  '5500000000000004' as credit_card,
  '456 Oak Ave' as address;

-- ==============================================
-- STEP 4: CALL THE STORED PROCEDURE
-- ==============================================
-- Use SIMPLE regex patterns WITHOUT backslashes
CALL `custom-plating-475002-j7.dlp_audit.redact_table`(
  'custom-plating-475002-j7.dlp_audit.test_customers',
  'custom-plating-475002-j7.dlp_audit.test_customers_redacted',
  '''[
    {
      "column_name": "email",
      "regex_pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}$",
      "action": "MASK_LAST_N",
      "mask_char": "*",
      "keep_chars": 3
    },
    {
      "column_name": "ssn",
      "regex_pattern": "^[0-9]{3}-[0-9]{2}-[0-9]{4}$",
      "action": "REPLACE",
      "replace_text": "[SSN]"
    }
  ]'''
);

-- ==============================================
-- STEP 5: CHECK RESULTS
-- ==============================================
SELECT 'ORIGINAL DATA:' as table_type, * FROM `custom-plating-475002-j7.dlp_audit.test_customers`
UNION ALL
SELECT 'REDACTED DATA:', * FROM `custom-plating-475002-j7.dlp_audit.test_customers_redacted`
ORDER BY id, table_type DESC;
