/*
==============================================
Create Database and Schemas
==============================================

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'data_warehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- If the database exists, force-close connections, then drop it.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'data_warehouse')
BEGIN
    ALTER DATABASE [data_warehouse] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; -- ensure exclusive access to drop
    DROP DATABASE [data_warehouse];
END
GO

CREATE DATABASE [data_warehouse];
GO

USE data_warehouse;
GO

-- Layered schemas for ingestion (bronze), refinement (silver), and presentation (gold).
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
