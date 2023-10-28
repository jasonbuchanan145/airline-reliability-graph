
#  Ensure you have docker installed
- Download here: https://www.docker.com/products/docker-desktop/


##  Run the following commands
- cd /HPC/services/
- docker-compose -up

This will run all services required for the project, without individually installing software or tampering with the configuration on your local machine.

### You can test the scoop connection using:
sqoop eval --connect jdbc:mysql://172.30.208.1:3307/HPC --username root --password root --query "SELECT 1"