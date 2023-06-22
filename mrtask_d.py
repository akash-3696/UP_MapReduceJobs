
#############################################
# Code for: What is the average trip time for different pickup locations?
#Importing required libraries 
#pandas for datetime function and MRJob for map reduce tasks
#############################################

import pandas as pd 
from mrjob.job import MRJob

#Class for average trip time MR Job
class MRAvgTripTime(MRJob):
    #mapper to map key value
    def mapper(self, _, line):
        rec = line.split(",") # Get the record for mapping
        PULoc = rec[7] #Pickup location code
        
        #Chceck for elimionating header row
        if rec[0] != 'VendorID':
            TripTime = pd.to_datetime(rec[2]) - pd.to_datetime(rec[1]) # Capturing trip time by substracting pickup time from drop time
            yield ( PULoc,TripTime.total_seconds()) #Key value stored by converting to seconds
    def reducer(self, key, values):
        yield key, sum(values)/len(key) #reducer value getting average trip time in seconds

#main function
if __name__ == '__main__':
    MRAvgTripTime.run()

