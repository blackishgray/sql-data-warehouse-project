use master;

IF EXISTS (SELECT 1 FROM sys.databases where name='DataWareHouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO
-- Create the DataWareHouse Database 
CREATE DATABASE DataWareHouse;
GO;

use DataWareHouse;
GO;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;


