import json
import base64
import gzip
import boto3
import os
from pprint import pprint


def lambda_handler(event, context):
    # TODO implement
    try:
        # get actual JSON
        # send data to firehose
        send_data_to_firehose(
            get_actual_json(event), 
            int(os.environ['max_batch_size']), 
            os.environ['delivery_stream_name'],
            int(os.environ['max_retry_attempts_per_batch']),
            boto3.client('firehose')
        )
    except Exception as e:
        print('Exception "{0}" occurred in lambda_handler'.format(e))
    return {
        "statusCode": 200,
        "body": json.dumps(event)
    }
    

def get_actual_json(event):
    ret = None
    try:
        record = event['awslogs']
        compressed_payload = base64.b64decode(record['data'])
        uncompressed_payload = gzip.decompress(compressed_payload)
        uncompressed_payload = json.loads(uncompressed_payload)
        if 'messageType' in uncompressed_payload and uncompressed_payload['messageType'] == 'DATA_MESSAGE' and 'logEvents' in uncompressed_payload and len(uncompressed_payload['logEvents']) > 0:
            ret = uncompressed_payload['logEvents']
    except Exception as e:
        print('Exception "{0}" occurred in get_actual_json'.format(e))
    return ret


def send_data_to_firehose(data, chunk_size, delivery_stream_name, max_retry_attempts_per_batch, client):
    error_count = 0
    total_records = 0
    print('Total Records: "{0}"'.format(len(data)))
    try:
        if type(data) == list:
            for batch in chunks(data, chunk_size):
                try:
                    firehose_request = build_firehose_record(batch)
                    response = m_put_record_batch(client, delivery_stream_name, firehose_request, retries=max_retry_attempts_per_batch)
                except Exception as e:
                    error_count += 1
                    print('Exception "{0}" occurred in for loop'.format(e))
    except Exception as e:
        print('Exception "{0}" occurred in send_data_to_firehose'.format(e))


def m_put_record_batch(client, delivery_stream_name, firehose_request, retries=3):
    # Message returned is {'message': 'some message', 'failed_records_count': count}
    response = ''
    
    # if there are any retries left
    if retries > 0:
        response = client.put_record_batch(
                        DeliveryStreamName=delivery_stream_name,
                        Records=firehose_request
                    )
        
        len_orig = len(firehose_request)
        failed_count = 0
        
        if 'FailedPutCount' in response:
            failed_count = response['FailedPutCount']
        
        print('Retry Attempt: {0}\tSuccessful Records Count: {1}\tFailed Records Count: {2}'.format(retries, len_orig - failed_count, failed_count))
                    
        if 'FailedPutCount' in response and response['FailedPutCount'] > 0 and retries > 0:
            m_put_record_batch(client, delivery_stream_name, get_only_failed_records(firehose_request, response), retries=retries-1)


def get_only_failed_records(original_request, response_request):
    '''
    if there are any failed responses, get the failed resposes only
    '''
    
    # if all failed, return original request
    if len(original_request) == len(response_request['RequestResponses']):
        return original_request
        
    ret = []
    for idx, item in enumerate(response_request['RequestResponses']):
        if 'ErrorCode' in item:
            ret.append(original_request[idx])
    return ret


def build_firehose_record(batch):
    ret = []
    try:
        for item in batch:
            ret.append({'Data': item['message']})
    except Exception as e:
        print('Exception "{0}" occurred in build_firehose_record'.format(e))
    return ret


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]