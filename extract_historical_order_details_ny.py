import os
import json
import logging
import boto3
import requests
import tempfile
import pdfplumber
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError, BotoCoreError
from pypdf import PdfReader, PdfWriter
from config import get_db_connection, db_upsert, db_insert_or_update

# Configure structured logging
logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

# Initialize AWS clients
textract = boto3.client('textract')
s3_client = boto3.client('s3')

# Retry-enabled session for HTTP requests
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount('http://', adapter)
session.mount('https://', adapter)

def download_pdf(pdf_url, temp_dir):
    """Download a PDF with retries and save locally."""
    try:
        response = session.get(pdf_url, timeout=10)
        response.raise_for_status()
        pdf_path = os.path.join(temp_dir, "order.pdf")
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded PDF from {pdf_url}")
        return pdf_path
    except requests.RequestException as e:
        logger.error(f"Failed to download PDF {pdf_url}: {e}")
        return None

def upload_to_s3(pdf_path, bucket_name):
    """Upload the PDF to S3 and return the S3 key."""
    try:
        s3_key = f"textract-input/{os.path.basename(pdf_path)}_{int(time.time())}.pdf"
        s3_client.upload_file(pdf_path, bucket_name, s3_key)
        logger.info(f"Uploaded PDF to S3: {bucket_name}/{s3_key}")
        return s3_key
    except ClientError as e:
        logger.error(f"Failed to upload PDF to S3: {e}")
        return None

def is_valid_pdf(pdf_path):
    """Check if the PDF is valid and readable."""
    try:
        with open(pdf_path, 'rb') as f:
            PdfReader(f)
        return True
    except Exception as e:
        logger.error(f"Invalid PDF format: {e}")
        return False

def is_encrypted_pdf(pdf_path):
    """Check if the PDF is encrypted."""
    try:
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            return reader.is_encrypted
    except:
        return False

def is_scanned_pdf(pdf_path):
    """Check if the PDF is scanned (no selectable text)."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                if page.extract_text():
                    return False
        return True
    except:
        return True

def get_pdf_metadata(pdf_path):
    """Extract metadata from the PDF."""
    try:
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            return dict(reader.metadata)
    except Exception as e:
        logger.error(f"Failed to read PDF metadata: {e}")
        return {}

def reformat_pdf(pdf_path, output_path):
    """Reformat the PDF to fix potential format issues."""
    try:
        reader = PdfReader(pdf_path)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        with open(output_path, 'wb') as f:
            writer.write(f)
        logger.info(f"Reformatted PDF saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to reformat PDF: {e}")
        return None

def extract_text_from_pdf(pdf_path, bucket_name):
    """Extract text from a PDF file, optimized for scanned documents."""
    file_size = os.path.getsize(pdf_path) / (1024 * 1024)  # MB
    page_count = 0
    is_scanned = False

    # Try pdfplumber for selectable text
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_count = len(pdf.pages)
            if page_count > 3000:
                logger.error(f"PDF has {page_count} pages, exceeds Textract limit")
                return ""
            text = "\n".join(page.extract_text() or "" for page in pdf.pages).strip()
            if text:
                logger.info("Successfully extracted text using pdfplumber")
                return text
            is_scanned = True
    except Exception as e:
        logger.info(f"pdfplumber failed (likely scanned PDF): {e}")
        is_scanned = True

    # Log PDF details
    logger.info(f"Processing PDF: {pdf_path}, Size: {file_size:.2f} MB, Pages: {page_count}")
    if is_scanned:
        logger.info("PDF appears to be scanned (no selectable text)")
    metadata = get_pdf_metadata(pdf_path)
    logger.info(f"PDF Metadata: {metadata}")

    # Validate PDF
    if not is_valid_pdf(pdf_path):
        logger.error("Skipping invalid PDF")
        return ""
    if is_encrypted_pdf(pdf_path):
        logger.error("Skipping encrypted PDF")
        return ""

    # Reformat PDF to fix potential format issues
    temp_reformatted = os.path.join(os.path.dirname(pdf_path), "reformatted.pdf")
    reformatted_path = reformat_pdf(pdf_path, temp_reformatted)
    if reformatted_path:
        pdf_path = reformatted_path

    # Use asynchronous Textract for scanned or large PDFs
    s3_key = upload_to_s3(pdf_path, bucket_name)
    if not s3_key:
        logger.error("Failed to upload PDF to S3 for asynchronous processing")
        return ""

    try:
        response = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': s3_key}}
        )
        job_id = response['JobId']
        logger.info(f"Started Textract job: {job_id}")
        max_wait = 600  # 10 minutes
        start_time = time.time()
        while time.time() - start_time < max_wait:
            response = textract.get_document_text_detection(JobId=job_id)
            status = response['JobStatus']
            if status in ['SUCCEEDED', 'FAILED']:
                break
            logger.info(f"Waiting for Textract job {job_id} to complete...")
            time.sleep(5)

        if time.time() - start_time >= max_wait:
            logger.error(f"Textract job {job_id} timed out after {max_wait}s")
            return ""
        if status == 'FAILED':
            logger.error(f"Textract job {job_id} failed: {response.get('StatusMessage', 'Unknown error')}")
            return ""

        extracted_text = []
        for block in response['Blocks']:
            if block['BlockType'] == 'LINE':
                extracted_text.append(block['Text'])

        next_token = response.get('NextToken')
        while next_token:
            response = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
            for block in response['Blocks']:
                if block['BlockType'] == 'LINE':
                    extracted_text.append(block['Text'])
            next_token = response.get('NextToken')

        full_text = "\n".join(extracted_text)
        logger.info("Successfully extracted text using Textract asynchronous API")
        return full_text
    except textract.exceptions.InvalidParameterException as e:
        logger.error(f"Textract async parameter error: {e}")
        return ""
    except (textract.exceptions.BadDocumentException, textract.exceptions.UnsupportedDocumentException) as e:
        logger.error(f"Textract async document error: {e}")
        return ""
    except textract.exceptions.ThrottlingException as e:
        logger.error(f"Textract async throttling error: {e}")
        return ""
    except ClientError as e:
        logger.error(f"ClientError during Textract async processing: {e}")
        return ""
    except BotoCoreError as e:
        logger.error(f"BotoCoreError during Textract async processing: {e}")
        return ""
    except Exception as e:
        logger.error(f"Unexpected error during Textract async extraction: {e}")
        return ""
    finally:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        except ClientError as e:
            logger.error(f"Failed to delete S3 object {s3_key}: {e}")

def process_order(order_id, order_data, temp_dir, engine, bucket_name, max_retries=3):
    """Process a single order: download, extract, insert with retries."""
    logger.info(f"Processing order {order_id}")

    try:
        order_num = float(order_data['order_num']) if '.' in str(order_data['order_num']) else int(order_data['order_num'])
    except (ValueError, TypeError):
        logger.warning(f"Invalid order_num format for {order_id}, defaulting to 0")
        order_num = 0

    order_entry = {
        'order_id': order_id,
        'title': order_data['title'],
        'signed_date': order_data['signed_date'],
        'description': None,
        'src': order_data['src'],
        'row_ct_dt': datetime.utcnow().isoformat(),
        'row_ct_user': 'lambda',
        'row_updt_dt': datetime.utcnow().isoformat(),
        'row_updt_user': 'lambda',
        'order_num': order_num
    }

    # Upsert into ny.executive_orders (assumes unique constraint on order_id)
    try:
        db_upsert(engine, 'executive_orders', order_entry, conflict_key=['order_id'], schema='ny')
        logger.info(f"Upserted executive order {order_id}")
    except Exception as e:
        logger.error(f"Database error (executive_orders) for {order_id}: {e}")
        return False

    pdf_path = download_pdf(order_data['pdf_url'], temp_dir)
    if not pdf_path:
        logger.warning(f"Skipping text extraction for {order_id}")
        return False

    text = extract_text_from_pdf(pdf_path, bucket_name)

    # Prepare data for ny.order_texts
    text_entry = {
        'order_id': order_id,
        'text': text,
        'src': order_data['src'],
        'row_ct_dt': datetime.utcnow().isoformat(),
        'row_ct_user': 'lambda',
        'row_updt_dt': datetime.utcnow().isoformat(),
        'row_updt_user': 'lambda'
    }

    # Insert or update into ny.order_texts using the new function
    # This handles tables without unique constraints
    for attempt in range(max_retries):
        try:
            db_insert_or_update(engine, 'order_texts', text_entry, conflict_key='order_id', schema='ny')
            logger.info(f"Inserted/updated order text for {order_id}")
            return True
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed for {order_id}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Max retries reached for {order_id}. Skipping.")
                return False
            time.sleep(2 ** attempt)  # Exponential backoff

def lambda_handler(event, context):
    """AWS Lambda main handler for processing a single order."""
    logger.info(f"Lambda triggered with event: {json.dumps(event, indent=2)}")

    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("Environment variable 'S3_BUCKET_NAME' not set")

        order_id = event.get('order_id')
        order_data = event.get('order_data')
        if not order_id or not order_data:
            raise ValueError("Missing order_id or order_data in event")

        engine = get_db_connection()
        if not engine:
            raise RuntimeError("Database connection failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            success = process_order(order_id, order_data, temp_dir, engine, bucket_name)
            if success:
                logger.info(f"Successfully processed order {order_id}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'Processed order {order_id}', 'order_id': order_id})
                }
            else:
                logger.error(f"Failed to process order {order_id}")
                raise RuntimeError(f"Failed to process order {order_id}")

    except ValueError as e:
        logger.error(f"ValueError: {e}")
        return {'statusCode': 400, 'body': json.dumps({'error': str(e)})}
    except ClientError as e:
        logger.error(f"S3 ClientError: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
    except BotoCoreError as e:
        logger.error(f"BotoCoreError: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
    except Exception as e:
        logger.error(f"Unhandled Exception: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}