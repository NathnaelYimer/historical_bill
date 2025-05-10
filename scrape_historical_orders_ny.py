import os
import json
import requests
from bs4 import BeautifulSoup
import re
import logging
import boto3
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import BASE_URL, S3_BUCKET_NAME, SRC_VALUE, save_to_s3

# Configure logging
logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

# Initialize S3 client for caching
s3_client = boto3.client('s3')

# Configure requests with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def parse_date(date_str):
    """Parse a date string like 'January 1, 2011' into 'YYYY-MM-DD' format."""
    try:
        return datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
    except ValueError as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return date_str  # Return as-is if parsing fails

def extract_historical_orders():
    """
    Extracts historical executive orders from the specified URL and returns a list of order data.
    """
    logger.info("Starting extraction of historical executive orders...")
    logger.info(f"Fetching URL: {BASE_URL}")
    try:
        response = session.get(BASE_URL, timeout=10)
        response.raise_for_status()
        # Cache webpage response for debugging
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"debug/webpage_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.html",
            Body=response.text
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {BASE_URL}: {e}")
        raise

    soup = BeautifulSoup(response.text, 'html.parser')
    # Find all sections containing executive orders
    sections = soup.find_all('div', class_='t-section__wrapper')

    if not sections:
        logger.warning("No sections found on the page")
        return []

    orders_data = []
    total_links_processed = 0
    total_links_skipped = 0

    for section in sections:
        # Extract governor name (for logging/context)
        governor = section.find('h2', class_='t-section__title')
        governor_name = governor.text.strip() if governor else "Unknown Governor"
        logger.info(f"Processing section for {governor_name}")

        # Find the content area with the executive orders
        content = section.find('div', class_='a-text__html')
        if not content:
            logger.warning(f"No content found in section for {governor_name}")
            continue

        # Extract all <p> tags containing executive orders
        paragraphs = content.find_all('p')
        if not paragraphs:
            logger.warning(f"No paragraphs found in section for {governor_name}")
            continue

        for p in paragraphs:
            # Extract all <a> tags (PDF links) within the paragraph
            pdf_links = p.find_all('a', href=re.compile(r'\.pdf$'))
            if not pdf_links:
                logger.warning(f"No PDF links found in paragraph: {p.text.strip()}")
                total_links_skipped += 1
                continue

            # Split the paragraph text into segments based on semicolons (for multiple orders in one <p>)
            segments = [seg.strip() for seg in p.text.split(';')]
            link_index = 0  # Track which PDF link corresponds to which segment

            for seg in segments:
                if not seg:
                    continue

                # Handle multiple orders in one segment
                if " and Executive Order No." in seg:
                    # Split by " and Executive Order No."
                    sub_segments = seg.split(" and Executive Order No.")
                    # Process first segment normally
                    first_segment = sub_segments[0]
                    # Add "Executive Order No." back to the remaining segments
                    remaining_segments = ["Executive Order No." + s for s in sub_segments[1:]]
                    # Combine all segments for processing
                    all_segments = [first_segment] + remaining_segments
                    
                    for sub_seg in all_segments:
                        if sub_seg.startswith('Executive Order No.'):
                            # Updated regex to match titles in parentheses
                            match = re.match(
                                r'Executive Order No\.?\s*([\d.]+),\s*issued\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})\s*(?:\((.*?)\))?',
                                sub_seg,
                                re.IGNORECASE
                            )
                            if not match:
                                logger.warning(f"Could not parse order in sub-segment: '{sub_seg}'")
                                continue

                            order_num = match.group(1)  # e.g., "26" or "26.1"
                            signed_date = parse_date(match.group(2))  # e.g., "October 6, 2011"
                            title = match.group(3) if match.group(3) else "No title available"  # e.g., "Statewide Language Access Policy"
                            
                            if title == "No title available":
                                logger.info(f"No title extracted for sub-segment: '{sub_seg}'")
                            
                            if link_index >= len(pdf_links):
                                logger.warning(f"No PDF link available for order number {order_num} in sub-segment: '{sub_seg}'")
                                total_links_skipped += 1
                                continue
                                
                            pdf_url = pdf_links[link_index]['href']
                            link_index += 1
                            
                            # Generate a unique order_id
                            order_id = f'NYORDER{order_num.replace(".", "_")}'

                            # Ensure PDF URL is absolute
                            if not pdf_url.startswith('http'):
                                pdf_url = f"https://www.governor.ny.gov{pdf_url}"

                            logger.info(f"Processing order: {order_id}, Order Num: {order_num}, Title: {title}, Signed Date: {signed_date}, PDF URL: {pdf_url}")

                            orders_data.append({
                                'order_id': order_id,
                                'order_num': order_num,
                                'title': title,
                                'signed_date': signed_date,
                                'pdf_url': pdf_url,
                                'src': SRC_VALUE,
                                'governor': governor_name
                            })
                            total_links_processed += 1
                            logger.info(f"Successfully extracted order: {order_id}")
                
                # Handle single order in the segment
                elif seg.startswith('Executive Order No.'):
                    # Updated regex to match titles in parentheses
                    match = re.match(
                        r'Executive Order No\.?\s*([\d.]+),\s*issued\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})\s*(?:\((.*?)\))?',
                        seg,
                        re.IGNORECASE
                    )
                    if not match:
                        logger.warning(f"Could not parse main order in segment: '{seg}'")
                        continue

                    order_num = match.group(1)  # e.g., "1" or "147"
                    signed_date = parse_date(match.group(2))  # e.g., "January 1, 2011"
                    title = match.group(3) if match.group(3) else "No title available"  # e.g., "Removing the Barriers to State Government"
                    
                    if title == "No title available":
                        logger.info(f"No title extracted for segment: '{seg}'")
                    
                    if link_index >= len(pdf_links):
                        logger.warning(f"No PDF link available for order number {order_num} in segment: '{seg}'")
                        total_links_skipped += 1
                        continue
                        
                    pdf_url = pdf_links[link_index]['href']
                    link_index += 1

                    # Generate a unique order_id
                    order_id = f'NYORDER{order_num.replace(".", "_")}'

                    # Ensure PDF URL is absolute
                    if not pdf_url.startswith('http'):
                        pdf_url = f"https://www.governor.ny.gov{pdf_url}"

                    logger.info(f"Processing order: {order_id}, Order Num: {order_num}, Title: {title}, Signed Date: {signed_date}, PDF URL: {pdf_url}")

                    orders_data.append({
                        'order_id': order_id,
                        'order_num': order_num,
                        'title': title,
                        'signed_date': signed_date,
                        'pdf_url': pdf_url,
                        'src': SRC_VALUE,
                        'governor': governor_name
                    })
                    total_links_processed += 1
                    logger.info(f"Successfully extracted order: {order_id}")

                # Handle subsequent orders in the segment (e.g., "147.28, issued October 4, 2019")
                else:
                    match = re.match(
                        r'([\d.]+),\s*issued\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})',
                        seg,
                        re.IGNORECASE
                    )
                    if not match:
                        logger.warning(f"Could not parse subsequent order in segment: '{seg}'")
                        continue

                    order_num = match.group(1)  # e.g., "147.28"
                    signed_date = parse_date(match.group(2))  # e.g., "October 4, 2019"
                    title = orders_data[-1]['title'] if orders_data else "No title available"  # Use previous title if available
                    
                    if title == "No title available":
                        logger.info(f"No title extracted for subsequent segment: '{seg}'")
                    
                    if link_index >= len(pdf_links):
                        logger.warning(f"No PDF link available for order number {order_num} in segment: '{seg}'")
                        total_links_skipped += 1
                        continue
                        
                    pdf_url = pdf_links[link_index]['href']
                    link_index += 1

                    # Generate a unique order_id
                    order_id = f'NYORDER{order_num.replace(".", "_")}'

                    # Ensure PDF URL is absolute
                    if not pdf_url.startswith('http'):
                        pdf_url = f"https://www.governor.ny.gov{pdf_url}"

                    logger.info(f"Processing order: {order_id}, Order Num: {order_num}, Title: {title}, Signed Date: {signed_date}, PDF URL: {pdf_url}")

                    orders_data.append({
                        'order_id': order_id,
                        'order_num': order_num,
                        'title': title,
                        'signed_date': signed_date,
                        'pdf_url': pdf_url,
                        'src': SRC_VALUE,
                        'governor': governor_name
                    })
                    total_links_processed += 1
                    logger.info(f"Successfully extracted order: {order_id}")

    logger.info(f"Processed {total_links_processed} total links, skipped {total_links_skipped} links (no PDFs or parsing issues)")
    logger.info(f"Extracted metadata for {len(orders_data)} historical executive orders")
    return orders_data

def lambda_handler(event, context):
    """
    Lambda handler to scrape historical executive orders and save them to S3.
    """
    logger.info("Lambda function started")
    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        if not bucket_name:
            logger.error("S3_BUCKET_NAME not set")
            raise ValueError("S3_BUCKET_NAME not set")

        orders_data = extract_historical_orders()
        if not orders_data:
            logger.warning("No historical executive orders data extracted")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No data extracted'})
            }

        file_name = save_to_s3(orders_data, bucket_name=bucket_name)
        if not file_name:
            logger.error("Failed to save to S3")
            raise RuntimeError("Failed to save data to S3")

        logger.info(f"Data extraction complete. File saved to S3: {file_name}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data extraction complete',
                'compiled_file_name': file_name,
                'bucket_name': bucket_name,
                'orders': orders_data
            })
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Request error: {str(e)}"})
        }
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"S3 error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"S3 error: {str(e)}"})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Unexpected error: {str(e)}"})
        }