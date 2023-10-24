create database HPC;
use HPC;

-- creates the databse table
 CREATE TABLE flightdetailsraw (
    fl_date VARCHAR(20),
    mkt_carrier_fl_num INT,
    op_unique_carrier VARCHAR(10),
    op_carrier_fl_num INT,
    origin_airport_id INT,
    origin_airport_seq_id INT,
    origin_city_market_id INT,
    origin VARCHAR(10),
    origin_city_name VARCHAR(100),
    origin_state_abr VARCHAR(10),
    dest_airport_id INT,
    dest_airport_seq_id INT,
    dest_city_market_id INT,
    dest VARCHAR(10),
    dest_city_name VARCHAR(100),
    dest_state_abr VARCHAR(10),
    crs_dep_time INT,
    dep_time INT,
    dep_delay_new INT,
    arr_time INT,
    arr_delay INT,
    cancelled TINYINT
);

CREATE TABLE uniquecarriermapping(
	op_unique_carrier varchar(10),
	op_carrier_name varchar(100)

 CREATE TABLE flightdetailsparsed (
    unix_time_flight BIGINT,
    mkt_carrier_fl_num INT,
    op_unique_carrier VARCHAR(10),
    origin VARCHAR(10),
    origin_city_name VARCHAR(100),
    origin_state_abr VARCHAR(10),
    dest VARCHAR(10),
    dest_city_name VARCHAR(100),
    dest_state_abr VARCHAR(10),
    arr_time BIGINT,
    arr_delay INT,
    cancelled TINYINT,
    avg_delay_arrival_airline FLOAT,
    avg_canceled_airline FLOAT,
    unix_time_arrival BIGINT,
    unix_time_departure BIGINT,
    airline_name VARCHAR(100)
);

 
LOAD DATA INFILE '/var/lib/mysql-files/T_ONTIME_MARKETING.csv'
IGNORE
INTO TABLE flightdetailsraw
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY ''
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/var/lib/mysql-files/L_UNIQUE_CARRIERS.csv'
IGNORE
INTO TABLE uniquecarriermap
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY ''
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


--We want the arrival and departure information to be null when the flight is cacanceled. 
update flightdetailsraw set dep_time = null, dep_delay_new=null, arr_time=null, arr_delay=null where cancelled=1;

--if the flight is ahead of time just treat it as on time and set it to 0 instead of negative
--in the dataset 100,000 flights were delayed by only 15 minutes or less ( select count(*) from flightdetailsraw where arr_delay > 0 and arr_delay < 15;)
--we don't really care about those, it's 15 minutes, we are more interested in 15 minutes to several hours
update flightdetailsraw set arr_delay = 0 where arr_delay<15 

-- this is a nasty bit of sql but we need to convert and add two columns to figure out what time this flight was scheduled to take off, fl date which is the date of the flight and a time that is always midnight and the crs_depart. Based on the documentation CRS depart is in the form of HHMM from midnight of the date of the flight. 
insert into flightdetailsparsed (unix_time_flight, mkt_carrier_fl_num, origin, origin_city_name, origin_state_abr, dest, dest_city_name, dest_state_abr, arr_time, arr_delay, cancelled) values 
Select -- this is a nasty bit of sql but we need to convert and add two columns to figure out what time this flight was scheduled to take off, fl date which is the date of the flight and a time that is always midnight and the crs_depart. Based on the documentation CRS depart is in the form of HHMM from midnight of the date of the flight.
UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING_INDEX(fl_date, ' ', 1), '%d/%m/%Y'))+((crs_dep_time/100)*3600+(crs_dep_time MOD 100)*60),
mkt_carrier_fl_num
origin,
origin_city_name,
origin_state_abr,
dest,
dest_city_name,
dest_cit_abr,
--same as above except handle the case of wrapped arrival times (ie took off at 2300 the night before and landed at 300)
UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING_INDEX(fl_date, ' ', 1), '%d/%m/%Y'))+((arr_time/100)*3600+(arr_time MOD 100)*60) + CASE 
	WHEN arr_time<crs_dep_time THEN 86400
        else 0
	end,
arr_delay,
cancelled,



