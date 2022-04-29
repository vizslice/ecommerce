glue ecommerce codes

The Model Designed as snowflake 
6 tables included in the mart 
1 - Product [Dimension]
2 - Customer [Dimension]
3 - ProductCountry [Snowflake for Product]
4 - CustomerCountry [Snowflake for Customer]
5 - DimDate [Dimension]
6 - OrderItem [Fact]

These tables are 1 fact table 3 dimension and 2 snowflake dimension which includes couuntry information for customer and product dimension

OrderItems table has relation with "Customer,Product,DimDate" dimensions n to 1 

Dimensin table become enriched for slicing data based on month wekday etc. for reporting purposes. 

Project Folders 
ScriptsDD \ Includes required redshift table structure for our records 
DataModel\ Includes snowlake schema of or ecommerce datamart
ecommerGlueScript.py -> its glue job for ETL pipeline 
![datamodel](https://github.com/vizslice/ecommerce/blob/main/Model.PNG)

