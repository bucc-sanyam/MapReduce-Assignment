# What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from datetime import datetime

# To run pass the argument of the directory containing all the 6 csv files.
# python3 mrtask_d.py <pathToDirectory>
class PickupLocationAvgTripTime(MRJob):

    # Different date time formats since the input file does not have one date format.
    date_time_formats = ['%m-%d-%Y %H:%M', '%m-%d-%Y %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']

    def parse_datetime(self, date_time_val):
        """
        parses the string date time value to datetime attempting various datetime formats

        :param date_time_val: string date time value
        :return: parsed datetime
        """
        for date_time_format in self.date_time_formats:
            try:
                return datetime.strptime(date_time_val, date_time_format)
            except ValueError:
                # try next format
                pass
        raise ValueError('Missing datetime format for ', date_time_val)

    
    def mapper(self, _, line):
        """
        Map pickup_location key to --> trip time in seconds

        :param _: ignored
        :param line: line of the file
        :return: yields tuple (pickup_location, trip_time in seconds)
        """
        if line.startswith('VendorID'):
            return
        data = line.split(',')
        pickup_location = data[7]
        pickup_datetime = self.parse_datetime(data[1])
        drop_off_datetime = self.parse_datetime(data[2])
        # difference in start and end of the trip taken in seconds
        trip_time_seconds = (drop_off_datetime - pickup_datetime).total_seconds()
        yield pickup_location, trip_time_seconds

    def combiner(self, key, values):
        """
        yields a tuple --> (pickup_location, (trip_times_seconds, total_count)

        :param key: pickup_location
        :param values: generator with trip_time_seconds
        """
        trip_times_seconds = 0
        total_count = 0
        for trip_time_seconds in values:
            trip_times_seconds += trip_time_seconds
            total_count += 1
        yield key, (trip_times_seconds, total_count)

    def reducer(self, key, values):
        """
        yields a tuple (payment_type, average trip time seconds)

        :param key: pickup_location
        :param values: generator with (trip_time in seconds, count)
        """
        total_trip_time_seconds = 0
        total_count = 0
        for trip_times_seconds, count in values:
            total_trip_time_seconds += trip_times_seconds
            total_count += count
        yield key, (total_trip_time_seconds / total_count)


if __name__ == '__main__':
    PickupLocationAvgTripTime.run()