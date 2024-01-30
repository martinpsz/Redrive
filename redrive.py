from dateutil import parser
from dateutil.relativedelta import relativedelta
import jsonlines
import json
import boto3
import itertools


#Function to take a datetime string and return a datetime string with a second added on:

def addOneSecond(dateString):
    parsed = parser.parse(dateString)
    oneSecondLater = parsed + relativedelta(seconds=1)
    return oneSecondLater.strftime('%Y-%m-%dT%H:%M:%SZ')

#Funtion to take JSON lines file and return parsed JSON
def parseJSONLines(file):
    data = []
    with open(file) as f:
        for lines in f:
            data.append(json.loads(lines))

    return data

#handle batching of events into batches of 10
def batchEvents(data, batchSize=5):
   #since I am looping through the data, I will also update the ts as I create the batches

   for i in range(0, len(data), batchSize):
        yield data[i:i+batchSize]
   


   
    

   


#Main function
def main(file):
    data = parseJSONLines(file)

    x = list(batchEvents(data[0:10]))

    print(x)

   

if __name__ == "__main__":
    main('person_nexus.jsonl')
