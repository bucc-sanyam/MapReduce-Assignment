# What are the different payment types used by customers and their count?
# The final results should be in a sorted format

from mrjob.job import MRJob
from mrjob.step import MRStep

# To run pass the argument of the directory containing all the 6 csv files.
# python3 mrtask_c.py <pathToDirectory>
class PaymentTypesSortedCounts(MRJob):

    def steps(self):
        """
        Re-define to make a multi-step job

        :return: a list of steps constructed with
                 :py:class:`~mrjob.step.MRStep` or other classes in
                 :py:mod:`mrjob.step`.
        """
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.payment_types_count_sorted_reducer)]
    
    def mapper(self, _, line):
        """
        Map payment_type key to count of 1

        :param _: ignored
        :param line: line of the file
        :return: yields tuple (payment_type, 1)
        """
        if line.startswith('VendorID'):
            return
        data = line.split(',')
        payment_type = data[9]
        yield payment_type, 1


    def reducer(self, key, values):
        """
        yields a tuple (None, (count of payment_types, payment_type))

        :param key: payment_type
        :param values: generator with 1 corresponding to each row of the payment_type
        """
        yield None, (sum(values), key)

    def payment_types_count_sorted_reducer(self, _, values):
        """
        2nd MrJob reducer gets input as [(count, payment_type_a), (count, payment_type_b), ...]
        and returns the sorted order on count

        :param _: ignored
        :param values: generator for (count of payment_types, payment_type)
        """
        for count, payment_type in sorted(values, reverse=True):
            yield payment_type, count


if __name__ == '__main__':
    PaymentTypesSortedCounts.run()