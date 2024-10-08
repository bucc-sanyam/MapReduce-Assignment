# How does revenue vary over time? Calculate the average trip revenue per month -
#     analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from datetime import datetime

# To run pass the argument of the directory containing all the 6 csv files.
# python3 mrtask_f.py <pathToDirectory>
class AverageTripRevenueOverTime(MRJob):

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
        Map (month, hour, day_of_week) to --> revenue

        :param _: ignored
        :param line: line of the file
        :return: yields tuple ((month, hour, day_of_week), revenue)
        """
        if line.startswith('VendorID'):
            return
        data = line.split(',')
        pickup_datetime = self.parse_datetime(data[1])
        month = pickup_datetime.month
        hour = pickup_datetime.hour
        day_of_week = pickup_datetime.weekday()
        revenue = float(data[16])
        yield (month, hour, day_of_week), revenue


    def reducer(self, key, values):
        """
        yields a tuple ((month, hour, day_of_week), average tips revenue )

        :param key: (month, hour, day_of_week)
        :param values: generator of (revenue)
        """
        revenues = list(values)
        trips = len(revenues)
        total_revenue = sum(revenues)

        yield key, total_revenue / trips


if __name__ == '__main__':
    AverageTripRevenueOverTime.run()