-- ==========================================================
-- Create ORDER_DDS Database
-- ==========================================================

IF NOT EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = 'ORDER_DDS'
)
BEGIN
    PRINT 'Creating ORDER_DDS database...';
    CREATE DATABASE ORDER_DDS;
END
ELSE
BEGIN
    PRINT 'ORDER_DDS database already exists.';
END;