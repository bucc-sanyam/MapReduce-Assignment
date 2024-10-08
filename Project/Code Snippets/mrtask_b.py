# Which pickup location generates the most revenue?

from mrjob.job import MRJob
from mrjob.step import MRStep

# To run pass the argument of the directory containing all the 6 csv files.
# python3 mrtask_b.py <pathToDirectory>
class PickupLocationMostRevenue(MRJob):

    def steps(self):
        """
        Re-define to make a multi-step job

        :return: a list of steps constructed with
                 :py:class:`~mrjob.step.MRStep` or other classes in
                 :py:mod:`mrjob.step`.
        """
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.most_revenue_reducer)]
    
    def mapper(self, _, line):
        """
        Map pickup_location key to revenue

        :param _: ignored
        :param line: line of the file
        :return: yields tuple pickup_location and mapped revenue
        """
        if line.startswith('VendorID'):
            return
        data = line.split(',')
        pickup_location = data[7]
        revenue = float(data[16])
        yield pickup_location, revenue


    def reducer(self, key, values):
        """
        yields a tuple (None, (sum of revenues, vendor_id)) for a given pickup_location

        :param key: vendor id
        :param values: revenues
        """
        yield None, (sum(values), key)

    def most_revenue_reducer(self, _, values):
        """
        2nd MrJob reducer gets values as [(revenues, pickup_location_a), (revenues, pickup_location_b), ...]
        and finds the pickup_location with max revenue

        :param _: ignored
        :param values: generator for (sum of revenues, pickup_location)
        """
        maximum_revenue, pickup_location = max(values)
        yield pickup_location, maximum_revenue


if __name__ == '__main__':
    PickupLocationMostRevenue.run()