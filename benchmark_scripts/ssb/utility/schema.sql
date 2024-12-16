-- Creating LINEORDER Table
CREATE TABLE LINEORDER (
    LO_ORDERKEY NUMERIC,  -- Order key
    LO_LINENUMBER NUMERIC,  -- Line number (1-7)
    LO_CUSTKEY NUMERIC,  -- Foreign Key to C_CUSTKEY
    LO_PARTKEY NUMERIC,  -- Foreign Key to P_PARTKEY
    LO_SUPPKEY NUMERIC,  -- Foreign Key to S_SUPPKEY
    LO_ORDERDATE NUMERIC,  -- Foreign Key to D_DATEKEY
    LO_ORDERPRIORITY CHAR(15),  -- Fixed text
    LO_SHIPPRIORITY CHAR(1),  -- Fixed text
    LO_QUANTITY NUMERIC,  -- Quantity (1-50)
    LO_EXTENDEDPRICE NUMERIC,  -- Price ≤ 55,450
    LO_ORDTOTALPRICE NUMERIC,  -- Total price ≤ 388,000
    LO_DISCOUNT NUMERIC,  -- Discount (0-10 percent)
    LO_REVENUE NUMERIC,  -- Revenue calculated as (LO_EXTENDEDPRICE * (100 - LO_DISCOUNT)) / 100
    LO_SUPPLYCOST NUMERIC,  -- Supply cost
    LO_TAX NUMERIC,  -- Tax (0-8 percent)
    LO_COMMITDATE NUMERIC,  -- Foreign Key to D_DATEKEY
    LO_SHIPMODE CHAR(10),  -- Fixed text (REG AIR, AIR, etc.)
    PRIMARY KEY (LO_ORDERKEY, LO_LINENUMBER)
);

-- Creating PART Table
CREATE TABLE PART (
    P_PARTKEY NUMERIC,  -- Part identifier
    P_NAME VARCHAR(22),  -- Name of the part
    P_MFGR CHAR(6),  -- Manufacturer (MFGR#1 to MFGR#5)
    P_CATEGORY CHAR(7),  -- Category (e.g. 'MFGR#1' to 'MFGR#5')
    P_BRAND1 CHAR(9),  -- Brand name
    P_COLOR VARCHAR(11),  -- Color of the part
    P_TYPE VARCHAR(25),  -- Type of the part
    P_SIZE NUMERIC,  -- Size (1-50)
    P_CONTAINER CHAR(10),  -- Container type (e.g. 'BOX')
    PRIMARY KEY (P_PARTKEY)
);

-- Creating SUPPLIER Table
CREATE TABLE SUPPLIER (
    S_SUPPKEY NUMERIC,  -- Supplier key (identifier)
    S_NAME CHAR(25),  -- Supplier name (e.g., 'Supplier1')
    S_ADDRESS VARCHAR(25),  -- Address of the supplier
    S_CITY CHAR(10),  -- City of the supplier (city code)
    S_NATION CHAR(15),  -- Country or nation (e.g., 'UNITED KINGDOM')
    S_REGION CHAR(12),  -- Region (e.g., 'MIDDLE EAST')
    S_PHONE CHAR(15),  -- Phone number of the supplier
    PRIMARY KEY (S_SUPPKEY)
);

-- Creating CUSTOMER Table
CREATE TABLE CUSTOMER (
    C_CUSTKEY NUMERIC,  -- Customer key (identifier)
    C_NAME VARCHAR(25),  -- Customer name (e.g., 'Customer1')
    C_ADDRESS VARCHAR(25),  -- Address of the customer
    C_CITY CHAR(10),  -- City (city code)
    C_NATION CHAR(15),  -- Country (e.g., 'UNITED KINGDOM')
    C_REGION CHAR(12),  -- Region (e.g., 'MIDDLE EAST')
    C_PHONE CHAR(15),  -- Phone number of the customer
    C_MKTSEGMENT CHAR(10),  -- Market segment (e.g., 'AUTOMOBILE')
    PRIMARY KEY (C_CUSTKEY)
);

-- Creating DATE Table
CREATE TABLE DATE (
    D_DATEKEY NUMERIC,  -- Unique identifier for the date (e.g., 19980327)
    D_DATE CHAR(18),  -- Full date (e.g., 'December 22, 1998')
    D_DAYOFWEEK CHAR(20),  -- Day of the week (e.g., 'Sunday')
    D_MONTH CHAR(9),  -- Month (e.g., 'January')
    D_YEAR NUMERIC,  -- Year (e.g., 1998)
    D_YEARMONTHNUM NUMERIC,  -- Year-Month number (e.g., 199803)
    D_YEARMONTH CHAR(7),  -- Year-Month formatted (e.g., 'Mar1998')
    D_DAYNUMINWEEK NUMERIC,  -- Day number within the week (1-7)
    D_DAYNUMINMONTH NUMERIC,  -- Day number within the month (1-31)
    D_DAYNUMINYEAR NUMERIC,  -- Day number within the year (1-366)
    D_MONTHNUMINYEAR NUMERIC,  -- Month number within the year (1-12)
    D_WEEKNUMINYEAR NUMERIC,  -- Week number within the year (1-53)
    D_SELLINGSEASON VARCHAR(12),  -- Selling season (e.g., 'Christmas')
    D_LASTDAYINWEEKFL BOOLEAN,  -- Last day of the week flag (1 bit)
    D_LASTDAYINMONTHFL BOOLEAN,  -- Last day of the month flag (1 bit)
    D_HOLIDAYFL BOOLEAN,  -- Holiday flag (1 bit)
    D_WEEKDAYFL BOOLEAN,  -- Weekday flag (1 bit)
    PRIMARY KEY (D_DATEKEY)
);