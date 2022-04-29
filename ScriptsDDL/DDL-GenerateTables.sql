
create table if not exists ProductCountry(
CountryId numeric(18,0) not null,
CountryCode varchar(10) not null,
CountryName varchar(150) not null,
primary key(CountryId)  );

create table if not exists Product(
ProductId numeric(18,0) not null,
ProductName varchar(150) not null,
ProductType varchar(50) not null,
CountryId numeric(18,0) not null,
primary key(ProductId)  );



create table if not exists CustomerCountry(
CountryId numeric(18,0) not null,
CountryCode varchar(10) not null,
CountryName varchar(150) not null,
primary key(CountryId)  );

create table if not exists Customer(
CustomerId numeric(18,0) not null,
CustomerUserName varchar(50) not null,
CountryId numeric(18,0) not null,
primary key(CustomerId)  );

create table if not exists DimDate(
DateId numeric(18,0) not null,
DateShort date not null,
YearShort integer not null,
MonthShort integer not null,
MonthNameShort varchar(50) not null,
WeekDayShort integer not null, 
DayNameShort  varchar(50) not null 
primary key(DateId)  );

create table if not exists OrderItem(
CustomerId numeric(18,0) not null,
ProductId numeric(18,0) not null,
DateId numeric(18,0) not null,
Quantity numeric(18,3) not null,
OrderItemTimeStamp timestamp  not null,
 );