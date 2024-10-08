import happybase


def batch_ingest_data(filenames, tablename):
    """
    Batch ingest data from the list of files into given hbase tablename

    :param filenames: list of csv file names to load data from
    :param tablename: hbase table name to ingest data into
    """

    # Open the connection to hbase
    print("Opening connection to HBase.")
    connection = happybase.Connection(
        'localhost', port=9090, autoconnect=False)
    connection.open()

    # Hbase table pointer
    table = connection.table(tablename)

    # Start batch insert
    for filename in filenames:
        print("Start Batch insert of ", filename)
        with open(filename, 'r') as f:
            i = 0
            with table.batch(batch_size=100000) as b:
                for line in f:
                    if i != 0:
                        temp = line.strip().split(",")
                        tpep_pickup_datetime = temp[1]
                        tpep_dropoff_datetime = temp[2]
                        row = tpep_pickup_datetime + tpep_dropoff_datetime
                        b.put(row, {
                            'cf1:VendorID': str(temp[0]),
                            'cf1:tpep_pickup_datetime': str(tpep_pickup_datetime),
                            'cf1:tpep_dropoff_datetime': str(tpep_dropoff_datetime),
                            'cf1:passenger_count': str(temp[3]), 
                            'cf1:trip_distance': str(temp[4]),
                            'cf1:RatecodeID': str(temp[5]),
                            'cf1:store_and_fwd_flag': str(temp[6]), 
                            'cf1:PULocationID': str(temp[7]),
                            'cf1:DOLocationID': str(temp[8]),
                            'cf1:payment_type': str(temp[9]), 
                            'cf1:fare_amount': str(temp[10]),
                            'cf1:extra': str(temp[11]), 
                            'cf1:mta_tax': str(temp[12]),
                            'cf1:tip_amount': str(temp[13]), 
                            'cf1:tolls_amount': str(temp[14]),
                            'cf1:improvement_surcharge': str(temp[15]),
                            'cf1:total_amount': str(temp[16]), 
                            'cf1:congestion_surcharge': str(temp[17]),
                            'cf1:airport_fee': str(temp[18])}
                        )
                    i += 1

        print("Done Batch insert of ", filename)

    print("Batch insert complete.")


if __name__ == '__main__':
    batch_ingest_data(filenames=['yellow_tripdata_2017-03.csv', 'yellow_tripdata_2017-04.csv'],
                      tablename='trip_records_hbase')
