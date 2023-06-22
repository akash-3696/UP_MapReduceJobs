
#############################################
#File Name: mrtask_f
# How does revenue vary over time? 
# Calculate the average trip revenue per month 
# - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).
#############################################

#Importing all libraries required for map reduce job
import pandas as pd 
from mrjob.job import MRJob

#defining class for analyzing average trip revenue 
class MRAvgTripRev(MRJob):
    #Class for main mapper 
    def mapper(self, _, line):
        rec = line.split(",") # Get record by spltting line
        if rec[0] != 'VendorID':
            pkdate = pd.to_datetime(rec[1]) # Date value
            TripRev = float(rec[16]) #Trip revenue (total amount)
            
            #Month of pickup date 
            pkupmonth = pkdate.month_name()# Name of the month
            
                        
            #assuming after 11 it is night time till morning at 5
            if pkdate.hour in [23,0,1,2,3,4,5]:
                pkuphour = "night"
            else:
                pkuphour = "day"
            if pkdate.day_name() in ['Sunday','Saturday']:
                pkweekdayweekendflag = "weekend"
            else:
                pkweekdayweekendflag = "weekday"

            yield ( pkupmonth,TripRev) # key value: pickup month and revenue 
            yield ( pkuphour,TripRev) # key value: pickup hour (day/night) and revenue 
            yield ( pkweekdayweekendflag,TripRev) # key value: weekend flag and revenue 
        
    #reducer function to find mean revenue value for each key
    def reducer(self, key, values):
        yield key, sum(values)/len(key) #getting average

#main method
if __name__ == '__main__':
    MRAvgTripRev.run()

