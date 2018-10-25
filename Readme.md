# Description
	Code required to push CloudTrail logs from Cloudwatch logs to Firehose stream via lambda.

# Required Parameters
  - **max_batch_size**: 
    maximum size of the records in batch that are sent collectively to Firehose Stream
  - **delivery_stream_name**:
    name of the firehose delivery stream where the records shall be sent to
  - **max_retry_attempts_per_batch**: _implemented but not tested_
    maximum number of retries to attempt in case of failed to send data to Firehose Stream

# Requirements
  - **Python 3.X**
  - **Environment Variables**: mentioned in required parametered
  - **Role**: 
    lambda_basic_execution access policy as well as permission to put_record_batch for firehose stream

# Flow
	CloudTrail -> Cloudwatch Log Group -> Stream to AWS Lambda (Subscripion Filter) -> Firehose Stream -> AWS ES Cluster