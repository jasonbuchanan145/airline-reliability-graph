#/bin/bash

# Wait for MySQL to be ready
 echo "Waiting for MySQL to be ready..."
 while ! mysqladmin ping -h"mysql_db" --silent; do
     sleep 1
 done
 echo "MySQL is ready."
  sqoop import --connect jdbc:mysql://mysql_db:3307/HPC \
     --username root --password root \
     --table airlineAirportData \
     --driver com.mysql.jdbc.Driver
