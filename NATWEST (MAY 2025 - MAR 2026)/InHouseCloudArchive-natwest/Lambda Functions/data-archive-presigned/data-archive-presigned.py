import boto3
import os
from botocore.exceptions import ClientError
import json
import base64
import mimetypes
from urllib.parse import unquote

def lambda_handler(event, context):
    """
    Reads a file from S3, encodes it in Base64, and returns it
    to the user for download through API Gateway.

    NOTE: This method is subject to a ~6MB file size limit due to
    API Gateway payload restrictions.
    """

    print(f"Received event: {json.dumps(event)}")

    try:
        s3_key = event['queryStringParameters']['key']
        s3_key = unquote(s3_key)
    except (KeyError, TypeError):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': "Missing or invalid 'key' query string parameter."})
        }

    bucket_name = os.environ.get('S3_BUCKET_NAME', 'natwest-data-archive-vault')
    s3_region = 'eu-west-1'
    
    print(f"Attempting to download object from bucket: {repr(bucket_name)}, key: {repr(s3_key)}")

    s3_client = boto3.client('s3', region_name=s3_region)

    try:
        # Get the object from S3
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # Read the raw bytes from the file
        file_content = s3_object['Body'].read()
        
        # Base64 encode the bytes to safely send them through API Gateway
        encoded_content = base64.b64encode(file_content).decode('utf-8')

        # Guess the content type (e.g., 'image/jpeg', 'application/pdf') from the filename
        content_type = mimetypes.guess_type(s3_key)[0] or 'application/octet-stream'
        
        # Extract just the filename from the key to suggest it to the browser
        filename = os.path.basename(s3_key)

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'The specified file key does not exist.'})
            }
        print(f"An S3 client error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Could not retrieve the file from storage.'})
        }

    # Return the file content directly to the user
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': content_type,
            # This header tells the browser to prompt a download with the specified filename
            'Content-Disposition': f'attachment; filename="{filename}"'
        },
        'body': encoded_content,
        # This flag is crucial to tell API Gateway to decode the body before sending
        'isBase64Encoded': True
    }
