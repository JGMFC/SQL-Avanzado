CREATE OR REPLACE FUNCTION keepcoding.clean_integer(value INT64)
RETURNS INT64
AS (
    IFNULL(value,-999999)
);