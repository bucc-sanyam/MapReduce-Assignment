## Load Data to RDS from .csv file

Login to EC2 Cluster
    ssh -i <Path to PEM File> ec2-user@<Public DNS>

Download Dataset:
    wget https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-01.csv
    wget https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-02.csv
    wget https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-03.csv
    wget https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-04.csv
    wget https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-05.csv
    wget https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-06.csv


Login to MySQL Client
    mysql -h <Endpoint> -u <username> -p

Create and Use DB
    create database TripRecords;
    use TripRecords;

Create Table Schema:
    create table TripRecords (
        VendorID int NOT NULL,
        tpep_pickup_datetime datetime NOT NULL,
        tpep_dropoff_datetime datetime NOT NULL,
        Passenger_count int NOT NULL,
        Trip_distance float NOT NULL,
        RateCodeID int NOT NULL,
        Store_and_fwd_flag varchar(1) NOT NULL,
        PULocationID int NOT NULL,
        DOLocationID int NOT NULL,
        Payment_type int NOT NULL,
        Fare_amount float NOT NULL,
        Extra float NOT NULL,
        MTA_tax float NOT NULL,
        Tip_amount float NOT NULL,
        Tolls_amount float NOT NULL,
        Improvement_surcharge float NOT NULL,
        Total_amount float NOT NULL,
        Congestion_Surcharge float NOT NULL,
        Airport_fee float NOT NULL,
        PRIMARY KEY (tpep_pickup_datetime, tpep_dropoff_datetime)
    );

Load Data:
    load data local infile '/home/ec2-user/yellow_tripdata_2017-02.csv' 
    into table TripRecords
    fields terminated by','
    lines terminated by'\n'
    ignore 1 lines;

    load data local infile '/home/ec2-user/yellow_tripdata_2017-02.csv' 
    into table TripRecords
    fields terminated by','
    lines terminated by'\n'
    ignore 1 lines;
