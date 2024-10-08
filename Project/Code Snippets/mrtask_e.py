# Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep


# To run pass the argument of the directory containing all the 6 csv files.
# python3 mrtask_e.py <pathToDirectory>
class AverageTipsToRevenueRatioForPickupLocations(MRJob):

    def steps(self):
        """
        Re-define to make a multi-step job

        :return: a list of steps constructed with
                 :py:class:`~mrjob.step.MRStep` or other classes in
                 :py:mod:`mrjob.step`.
        """
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sorting_reducer)]

    def mapper(self, _, line):
        """
        Map pickup_location key to --> (tip_amount, revenue)

        :param _: ignored
        :param line: line of the file
        :return: yields tuple (pickup_location, (tip_amount, revenue)
        """
        if line.startswith('VendorID'):
            return
        data = line.split(',')
        pickup_location = data[7]
        tip_amount = float(data[13])
        revenue = float(data[16])
        yield pickup_location, (tip_amount, revenue)

    def combiner(self, key, values):
        """
        yields a tuple (pickup_location, (total_tips, total_revenue))

        :param key: pickup_location
        :param values: generator of (tip_amount, revenue)
        """
        total_tips = 0
        total_revenue = 0
        for tip_amount, revenue in values:
            total_tips += tip_amount
            total_revenue += revenue
        yield key, (total_tips, total_revenue)

    def reducer(self, key, values):
        """
        yields a tuple (pickup_location, average tips to revenue ratio)

        :param key: pickup_location
        :param values: generator of (tip_amount, revenue)
        """
        total_tip_amts = 0
        total_revenue = 0
        for tip_amount, revenue in values:
            total_tip_amts += tip_amount
            total_revenue += revenue
        yield None, (key, total_tip_amts / total_revenue)

    def sorting_reducer(self, _, values):
        """
        2nd MrJob reducer gets values as [(pickup_location_a, tips_revenue_ratio), (pickup_location_b, tips_revenue_ratio), ...]
        and yields sorted values on pickup_location

        :param _: ignored
        :param values: generator for (pickup_locations, tips_revenue_ratio)
        """
        for pickup_location, tips_revenue_ratio in sorted(values):
            yield pickup_location, tips_revenue_ratio


if __name__ == '__main__':
    AverageTipsToRevenueRatioForPickupLocations.run()
