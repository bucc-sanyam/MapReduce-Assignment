## MySQL Connector on AWS

### Log in to EMR using ssh
    ssh -i <Path to PEM File> hadoop@<Public DNS>

### Download MySQL Connector jar file
    wget https://de-mysql-connector.s3.amazonaws.com/mysql-connector-java-8.0.25.tar.gz

### Extract the MySQL Connector tar file
    tar -xvf mysql-connector-java-8.0.25.tar.gz

### Copy MySQL Connector to the Sqoop library
    cd mysql-connector-java-8.0.25/
    sudo cp mysql-connector-java-8.0.25.jar /usr/lib/sqoop/lib/

## Python Libraries on EC2

    pip install --user happybase mrjob

### Run all these commands in sequential order if you face any error regarding thrift library.

    sudo yum update
    sudo yum install gcc 
    sudo yum install python3-devel
    pip install happybase mrjob    