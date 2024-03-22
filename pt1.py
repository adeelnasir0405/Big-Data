from mrjob.job import MRJob
import re

class part1(MRJob):
    def mapper(self, _, line):
        val = line.strip()
        (admonth, adheight, adquality) = (val[19:21], int(val[70:75]), val[75:76])
        if adheight != 99999 and re.match("[01459]", adquality):
            yield (admonth, adheight)

    def reducer(self, admonth, adheights):
        max_height = float('-inf')
        min_height = float('inf')
        for adheight in heights:
            if adheight > max_height:
                max_height = height
            if adheight < min_height:
                min_height = height
        yield admonth, max_height - min_height

if __name__ == '__main__':
    Ceiling_Range_bymonth.run()
