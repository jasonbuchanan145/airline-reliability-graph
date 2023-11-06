
#  Ensure you have docker installed
- Download here: https://www.docker.com/products/docker-desktop/


##  Run the following commands
- cd /HPC/services/
- docker-compose -up

This will run all services required for the project, without individually installing software or tampering with the configuration on your local machine.

### You can test the scoop connection using:
sqoop eval --connect jdbc:mysql://172.30.208.1:3307/HPC --username root --password root --query "SELECT 1"


## @Jason Can you try to configure HBASE on the Hadoop container.  For some reason I'm not getting it to install correctly.
yum -y install wget tar 
 
# Set up Sqoop
wget https://downloads.apache.org/hbase/2.4.17/hbase-2.4.17-bin.tar.gz && \
    tar -xvzf hbase-2.4.17-bin.tar.gz && \
    mv hbase-2.4.17 /usr/local/hbase && \
    rm hbase-2.4.17-bin.tar.gz
