	/*
	----------------------
	CREATE A SCHEMA 
	----------------------
	
	Scripts purpose :
		This script creates a new database named 'DataWareHouse' after checking if it already exists.
		If the database exists, it is dropped and recreaded. Additionally, The script sets up thress schemas 
		within the database : 'bronze', 'silver' and 'gold'.
	
	WARNING:
		Running this script wiil drop and entire 'DataWareHouse' database if it exists.
		All data in the database will be permanently deleted. Proceed with caution 
		and ensure you have proper backups before running this script.
	*/
	
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
	
	
