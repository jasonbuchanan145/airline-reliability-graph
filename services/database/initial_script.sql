create database HPC;
use HPC;

-- creates the databse table
 CREATE TABLE flightDetailsRaw (
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

CREATE TABLE uniqueCarrierMapping(
	op_unique_carrier varchar(10),
	op_carrier_name varchar(300)
);
 CREATE TABLE airlineAirportData (
    id INT NOT NULL AUTO_INCREMENT,
    origin VARCHAR(10),
    origin_city_name VARCHAR(100),
    origin_state_abr VARCHAR(10),
    carrier_name VARCHAR(300),
    dest VARCHAR(10),
    dest_city_name VARCHAR(100),
    dest_state_abr VARCHAR(10),
    percentage_delayed FLOAT,
    percentage_delayed_longer_than_15 FLOAT,
    percentage_cancelled FLOAT,
    avg_delay FLOAT,
    avg_delay_longer_than_15 FLOAT,
    num_flights INT,
    PRIMARY KEY(id)
);

 
LOAD DATA INFILE '/var/lib/mysql-files/T_ONTIME_MARKETING.csv'
IGNORE
INTO TABLE flightDetailsRaw
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY ''
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/var/lib/mysql-files/L_UNIQUE_CARRIERS.csv'
IGNORE
INTO TABLE uniqueCarrierMapping
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n';

-- We want the arrival and departure information to be null when the flight is cacanceled. 
update flightDetailsRaw set dep_time = null, dep_delay_new=null, arr_time=null, arr_delay=null where cancelled=1;

update flightDetailsRaw aw set arr_delay = 0 where arr_delay<0; 

-- aggregate the data. This will serve as our graph bases for hbase
INSERT INTO airlineAirportData
    (origin, origin_city_name, origin_state_abr, carrier_name,
     dest, dest_city_name, dest_state_abr,
     percentage_delayed, percentage_delayed_longer_than_15, percentage_cancelled, avg_delay, avg_delay_longer_than_15, num_flights)
SELECT
    origin, origin_city_name, origin_state_abr, mapping.op_carrier_name AS carrier_name,
    dest, dest_city_name, dest_state_abr,
    SUM(CASE WHEN arr_delay > 0 THEN 1 ELSE 0 END) / COUNT(*) AS percentage_delayed,
    SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END) / COUNT(*) AS percentage_delayed_longer_than_15,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) / COUNT(*) AS percentage_cancelled,
    AVG(arr_delay) AS avg_delay,
    SUM(CASE WHEN arr_delay > 15 THEN arr_delay ELSE 0 END) / 
    -- guard against divide by 0 errors by forcing the denominator to be at least 1 since the numerator will be 0 too it comes to 0
      CASE 
        WHEN SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END) = 0 THEN 1 
        ELSE SUM(CASE WHEN arr_delay > 15 THEN 1 ELSE 0 END)
    END AS avg_delays_longer_than_15,
    COUNT(*) AS num_flights
FROM
    flightDetailsRaw fl
LEFT OUTER JOIN
    uniqueCarrierMapping mapping ON fl.op_unique_carrier = mapping.op_unique_carrier
GROUP BY
    dest, origin, dest_city_name, dest_state_abr, origin_city_name, origin_state_abr, mapping.op_carrier_name;




