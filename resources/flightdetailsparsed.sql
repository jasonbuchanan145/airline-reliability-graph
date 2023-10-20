 CREATE TABLE flightdetailsparsed (
   --fl_date replaced with unix time
   unix_time_flight BIGINT,
    mkt_carrier_fl_num INT,
    op_unique_carrier VARCHAR(10),
    origin VARCHAR(10),
    origin_city_name VARCHAR(100),
    origin_state_abr VARCHAR(10),
    dest VARCHAR(10),
    dest_city_name VARCHAR(100),
    dest_state_abr VARCHAR(10),
    crs_dep_time INT,
    dep_time INT,
    dep_delay_new INT,
    arr_time INT,
    arr_delay INT,
    cancelled TINYINT,
    avg_delay_arrival_airline FLOAT,
    avg_canceled_airline FLOAT,
    unix_time_arrival BIGINT,
    unix_time_departure BIGINT,
    airline_name VARCHAR(100)
);
