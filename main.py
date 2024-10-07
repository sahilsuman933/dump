import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from dotenv import load_dotenv
import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, File
import logging
from logtail import LogtailHandler  

load_dotenv()

# Configure logging
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

# Create Logtail handler for BetterStack
logtail_handler = LogtailHandler(source_token=os.getenv('BETTERSTACK_SOURCE_TOKEN'))
logger.addHandler(logtail_handler)

# Optional: Keep logging to console
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)

# Configure AWS clients
AWS_REGION = os.getenv('AWS_REGION')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
textract_client = boto3.client(
    'textract',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def parse_s3_url(url):
    """
    Function parse_s3_url(url):
        Parses an S3 URL and returns the bucket and key.
    """
    if url.startswith('s3://'):
        url_without_protocol = url[5:]
        parts = url_without_protocol.split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        return bucket, key
    else:
        parsed_url = urlparse(url)
        bucket = parsed_url.netloc.split('.')[0]
        key = parsed_url.path.lstrip('/')
        return bucket, key

def tokenize_string(content):
    """
    Function tokenize_string(content):
        Tokenizes the input string into words.
    """
    tokens = content.split()
    return tokens

def process_file(file):
    """
    Function process_file(file):
        Processes a single file by extracting text using AWS Textract and updating the database.
    """
    # Create a new session for each file processing
    session = Session()
    try:
        logger.info(f"Processing file: {file.title}")

        bucket, key = parse_s3_url(file.url)
        original_file_name = os.path.basename(key)
        file_name_without_ext = os.path.splitext(original_file_name)[0]

        logger.info(f"Using asynchronous Textract API for file: {file.title} | {file.url}")

        textract_params = {
            'DocumentLocation': {
                'S3Object': {
                    'Bucket': S3_BUCKET_NAME,
                    'Name': key,
                },
            },
        }

        start_response = textract_client.start_document_text_detection(**textract_params)
        job_id = start_response['JobId']
        logger.info(f"Started Textract job with JobId: {job_id}")

        job_status = 'IN_PROGRESS'
        while job_status == 'IN_PROGRESS':
            time.sleep(5)
            job_status_response = textract_client.get_document_text_detection(JobId=job_id)
            job_status = job_status_response['JobStatus']
            logger.info(f"Textract Job Status: {job_status}")

            if job_status == 'SUCCEEDED':
                content = ''
                blocks = job_status_response['Blocks']

                content += '\n'.join([block['Text'] for block in blocks if block['BlockType'] == 'LINE'])

                next_token = job_status_response.get('NextToken')
                while next_token:
                    next_page_response = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
                    blocks = next_page_response['Blocks']
                    content += '\n' + '\n'.join([block['Text'] for block in blocks if block['BlockType'] == 'LINE'])
                    next_token = next_page_response.get('NextToken')

                logger.info(f"Extracted text from file using Textract asynchronous API")

                text_file_key = f"pageContents/{file_name_without_ext}.txt"
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=text_file_key,
                    Body=content,
                    ContentType='text/plain'
                )

                page_content_upload_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{text_file_key}"
                logger.info(f"Uploaded text file to S3: {page_content_upload_url}")

                word_count = len(content.split())
                token_count_estimate = len(tokenize_string(content))

                # Update the database
                file_obj = session.merge(file)  # Merge the file object into the current session
                file_obj.pageContentUrl = page_content_upload_url
                file_obj.wordCount = word_count
                file_obj.tokenCountEstimate = token_count_estimate
                session.commit()
                logger.info(f"Updated file record in database: {file.id}")

                break

            elif job_status == 'FAILED':
                logger.error(f"Textract job failed for file {file.id}, File might be broken.")
                return

    except Exception as e:
        logger.error(f"Error processing file {file.id}: {str(e)}")
        session.rollback()  # Rollback the session in case of an error
    finally:
        session.close()  # Always close the session

def main():
    """
    Function main():
        Entry point of the script. Retrieves files to process and starts multithreaded processing.
    """
    try:
        # Create a session for the main function
        session = Session()
        files = session.query(File).filter(File.pageContentUrl == None, File.folderId == '278c9c29-1a99-4f32-a9f8-bce6b737a8b0').all()

        logger.info(f"Found {len(files)} files to process")

        # Close the session after querying
        session.close()

        max_workers = 20
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_file, file) for file in files]
            for future in as_completed(futures):
                future.result()

        logger.info("Processing complete")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()