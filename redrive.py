from dateutil import parser
from dateutil.relativedelta import relativedelta
import json
import boto3
import time


#Function to take a datetime string and return a datetime string with a second added on:

def addFiveSeconds(dateString):
    parsed = parser.parse(dateString)
    oneSecondLater = parsed + relativedelta(seconds=5)
    formatted_date = oneSecondLater.strftime('%Y-%m-%dT%H:%M:%S') + '.' + str(oneSecondLater.microsecond // 1000) + 'Z'
    return formatted_date

#Funtion to take JSON lines file and return parsed JSON
def parseJSONLines(file):
    data = []
    with open(file) as f:
        for lines in f:
            data.append(json.loads(lines))

    return data


def createBatchesByDay(data, day):
    result = filter(lambda x: x['ts'].startswith(day), data)

    return result

#Create batches of 10 from the data and update timestamp for each record by 1 second   
def createChunks(data, maxBatchSize):
    chunks = [data[x: x+maxBatchSize] for x in range(0, len(data), maxBatchSize)] 

    payload = []
    for chunk in chunks:
        entries = []
        for x in chunk: 
            #add one second to the timestamp

            new_x = x.copy()
            new_x['ts'] = addFiveSeconds(new_x['ts'])
            entry = {
                'Id': str(new_x['data']['person_pk']),
                'MessageBody': json.dumps(new_x)
            }
            entries.append(entry)
        payload.append(entries)

    return payload

#Main function
def main(file):
    #parse data
    data = parseJSONLines(file)

    day = list(createBatchesByDay(data, '2024-01-30'))

    chunks = createChunks(day, 7)

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue = link_to_sqs


    for i, msg in enumerate(chunks):
        try: 
            response = sqs.send_message_batch(QueueUrl=queue, Entries=msg)

            # Print information about the batch
            print(f"Batch {i + 1} - Messages: {len(msg)}")

            # Print successful messages
            successful_ids = [m['Id'] for m in response.get('Successful', [])]
            print(f"Successfully sent IDs: {successful_ids}")

            time.sleep(1)

        except Exception as e:
            print(f"Error in Batch {i + 1}: {e}")
        
            with open(f'errors/error_batch_{i + 1}.json', 'a') as error_file:
                json.dump(msg, error_file)


if __name__ == "__main__":
    main('person_nexus.jsonl')

