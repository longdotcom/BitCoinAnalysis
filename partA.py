from mrjob.job import MRJob
import re
import datetime

WORD_REGEX = re.compile(r"\b\w+\b")

class partA(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if(len(fields)==5):
                time_epoch = int(fields[2])
                date = datetime.datetime.fromtimestamp(time_epoch)
                monthYear = date.strftime("%Y-%m")
                yield (monthYear, 1)
            else:
                yield ("malformed", 1)
        except:
            yield ("malformed", 1)

    def combiner(self, monthYear, count):
        yield(monthYear, sum(count))

    def reducer(self, monthYear, count):
        yield (monthYear, sum(count))

if __name__ == '__main__':
    partA.run()
