import csv
import collections
import datetime
from dateutil.relativedelta import*

'''

Input: 
- bitcoin_blocks.csv from process.py

Output:
- mining concentration csv for biggest x pools

'''

def mining_concentration(x):

    #changed to output monthly values

    with open('bitcoin_blocks.csv', 'rb') as csvfile:
        bitcoin_blocks = csv.reader(csvfile)
        #skip header, because time conversion doesnt work if header is included
        next(csvfile)

        #store it as an array so I can iterate over it more than once..
        blocks_as_array = []
        for line in bitcoin_blocks:
            blocks_as_array.append(line)

        #change the date to %Y-%m
        for line in blocks_as_array:
            line[11] = datetime.datetime.strptime(line[11][:7], "%Y-%m")

        with open('mining_concentration_biggest_' +str(x) + '.csv', 'wb') as csvfile2:
            output_file = csv.writer(csvfile2)
            header = ["Date", "Category", "Percentage"]
            output_file.writerow(header)

            #go through original block file to get shares each month
            current_month = datetime.datetime.strptime("2009-01", "%Y-%m")
            max_month = datetime.datetime.strptime("2017-12", "%Y-%m")

            while current_month <= max_month:
                #reset after each month
                counter = 0
                mining_pools = []

                for line in blocks_as_array:
                    if current_month == line[11]:
                        counter += 1
                        mining_pools.append(line[9])

                pools_counter = collections.Counter(mining_pools)

                #calculate no pools involved
                no_pool = pools_counter[""]
                no_pool += pools_counter["Unknown Entity"]
                no_pool_share = no_pool/float(counter) #calculates percentage of no pools
                del pools_counter[""] #needed to get the top x pools without "no pool"
                del pools_counter["Unknown Entity"] #needed to get the top x pools without "no pool"

                #calculate biggest x pools
                biggest_pools = pools_counter.most_common(x)
                aggr_biggest_pools = 0
                for (name, count) in biggest_pools:
                    aggr_biggest_pools += count
                aggr_biggest_pools_share = aggr_biggest_pools/float(counter) #calcuates percentage of biggest x pools

                #calculate remaining pools
                other_pools_share = 1 - no_pool_share - aggr_biggest_pools_share

                #write to file
                output_file.writerow([current_month.strftime("%Y-%m-%d"), "no pool", no_pool_share])
                output_file.writerow([current_month.strftime("%Y-%m-%d"), "other pools", other_pools_share])
                output_file.writerow([current_month.strftime("%Y-%m-%d"), "biggest " + str(x) + " pools", aggr_biggest_pools_share])

                current_month += relativedelta(months=1)

mining_concentration(5)