import streamlit as st
import requests
import pandas as pd
import time
import json
import re
from io import BytesIO
from datetime import datetime
import logging
from urllib.parse import urljoin
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TargetScraper:
    def __init__(self):
        self.session = requests.Session()
        self.setup_session()
        
    def setup_session(self):
        """Setup requests session with browser-like headers"""
        # Rotate user agents to avoid detection
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
    
    def get_target_api_data(self, tcin, max_retries=3):
        """Try to fetch data from Target's internal APIs"""
        
        # Target's Redsky API endpoints - these are the correct ones based on research
        api_endpoints = [
            # Main product data endpoint
            f"https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1?key=ff457966e64d5e877fdbad070f276d18ecec4a01&tcin={tcin}&store_id=3991&pricing_store_id=3991&has_pricing_store_id=true&scheduled_delivery_store_id=3991&has_scheduled_delivery_store_id=true&is_bot=false&channel=WEB&page=%2Fp%2FA-{tcin}",
            
            # Alternative product endpoint
            f"https://redsky.target.com/v3/pdp/tcin/{tcin}?key=ff457966e64d5e877fdbad070f276d18ecec4a01&channel=WEB&page=%2Fp%2FA-{tcin}&store_id=3991&pricing_store_id=3991&has_pricing_store_id=true",
            
            # Backup endpoint
            f"https://redsky.target.com/v2/pdp/tcin/{tcin}?key=ff457966e64d5e877fdbad070f276d18ecec4a01&channel=WEB"
        ]
        
        for attempt in range(max_retries):
            for endpoint in api_endpoints:
                try:
                    # Add jitter to delay
                    delay = 2 + random.uniform(0.5, 2.0)
                    if attempt > 0:
                        delay += (2 ** attempt)
                    time.sleep(delay)
                    
                    # Randomize headers slightly
                    headers = self.session.headers.copy()
                    headers['X-Requested-With'] = 'XMLHttpRequest'
                    headers['Referer'] = f'https://www.target.com/p/-/A-{tcin}'
                    
                    response = self.session.get(endpoint, headers=headers, timeout=15)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            
                            result = self._parse_api_response(tcin, data)
                            if result and result['Title'] != 'N/A':
                                logger.info(f"Successfully extracted data from API for TCIN {tcin}")
                                return result
                        except json.JSONDecodeError:
                            continue
                    
                    elif response.status_code == 404:
                        return self._create_invalid_tcin_record(tcin, "Product not found")
                    
                    elif response.status_code == 429:  # Rate limited
                        logger.warning(f"Rate limited on attempt {attempt + 1} for TCIN {tcin}")
                        time.sleep(5 + random.uniform(0, 5))
                        continue
                
                except requests.RequestException as e:
                    logger.warning(f"API request failed for {endpoint}: {str(e)}")
                    continue
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying TCIN {tcin}, attempt {attempt + 2}")
        
        # If API fails, try scraping the page
        return self.extract_from_page(tcin, max_retries)
    
    def extract_from_page(self, tcin, max_retries=3):
        """Fallback method: scrape the actual product page"""
        
        for attempt in range(max_retries):
            try:
                url = f"https://www.target.com/p/-/A-{tcin}"
                
                # Add delay
                delay = 3 + random.uniform(0.5, 2.0)
                if attempt > 0:
                    delay += (2 ** attempt)
                time.sleep(delay)
                
                # Rotate user agent
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                ]
                
                headers = {
                    'User-Agent': random.choice(user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0'
                }
                
                response = self.session.get(url, headers=headers, timeout=15)
                
                if response.status_code == 404:
                    return self._create_invalid_tcin_record(tcin, "Product not found")
                
                if response.status_code != 200:
                    if attempt < max_retries - 1:
                        logger.warning(f"HTTP {response.status_code} for TCIN {tcin}, attempt {attempt + 1}")
                        continue
                    return self._create_invalid_tcin_record(tcin, f"HTTP {response.status_code}")
                
                # Check if we got blocked
                if "blocked" in response.text.lower() or "access denied" in response.text.lower():
                    if attempt < max_retries - 1:
                        logger.warning(f"Potentially blocked for TCIN {tcin}, attempt {attempt + 1}")
                        time.sleep(10 + random.uniform(0, 10))
                        continue
                    return self._create_invalid_tcin_record(tcin, "Access blocked")
                
                # Parse the page
                result = self._parse_html_response(tcin, response.text)
                if result and result['Title'] != 'N/A':
                    return result
                
                if attempt < max_retries - 1:
                    logger.warning(f"No data extracted for TCIN {tcin}, attempt {attempt + 1}")
                    continue
                
                return self._create_invalid_tcin_record(tcin, "Could not extract product data")
                
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed for TCIN {tcin}, attempt {attempt + 1}: {str(e)}")
                    continue
                return self._create_invalid_tcin_record(tcin, f"Request failed: {str(e)}")
        
        return self._create_invalid_tcin_record(tcin, "Max retries exceeded")
    
    def _parse_api_response(self, tcin, data):
        """Parse Target API JSON response using correct Redsky structure"""
        result = self._create_base_record(tcin)
        
        try:
            # Target Redsky API structure: data -> product
            product = None
            
            # Handle different API response structures
            if 'data' in data and 'product' in data['data']:
                product = data['data']['product']
            elif 'product' in data:
                product = data['product']
            else:
                logger.warning(f"Unexpected API structure for TCIN {tcin}")
                return None
            
            if not product:
                return None
            
            # Extract title from item.product_description
            if 'item' in product and product['item']:
                item = product['item']
                
                if 'product_description' in item:
                    desc = item['product_description']
                    if 'title' in desc:
                        result['Title'] = desc['title']
                
                # Extract brand from product_classification
                if 'product_classification' in item:
                    classification = item['product_classification']
                    if 'product_type' in classification:
                        product_type = classification['product_type']
                        if 'name' in product_type:
                            result['Brand'] = product_type['name']
                
                # Try to get brand from product_vendors
                if 'product_vendors' in item and len(item['product_vendors']) > 0:
                    vendor = item['product_vendors'][0]
                    if 'vendor_name' in vendor:
                        result['Brand'] = vendor['vendor_name']
            
            # Extract pricing from price
            if 'price' in product and product['price']:
                price_data = product['price']
                
                # Current retail price (sale price)
                if 'current_retail' in price_data and price_data['current_retail']:
                    result['Sale_Price'] = f"${price_data['current_retail']}"
                
                # Regular retail price
                if 'regular_retail' in price_data and price_data['regular_retail']:
                    result['Regular_Price'] = f"${price_data['regular_retail']}"
                elif 'current_retail' in price_data:
                    # If no separate regular price, use current as both
                    result['Regular_Price'] = result['Sale_Price']
                
                # Handle price ranges (some products have min/max)
                if 'current_retail_min' in price_data and 'current_retail_max' in price_data:
                    min_price = price_data['current_retail_min']
                    max_price = price_data['current_retail_max']
                    if min_price == max_price:
                        result['Sale_Price'] = f"${min_price}"
                    else:
                        result['Sale_Price'] = f"${min_price} - ${max_price}"
            
            # Extract reviews and ratings from ratings_and_reviews
            if 'ratings_and_reviews' in product and product['ratings_and_reviews']:
                reviews = product['ratings_and_reviews']
                
                if 'statistics' in reviews:
                    stats = reviews['statistics']
                    
                    # Review count
                    if 'rating' in stats and 'count' in stats['rating']:
                        result['Number_of_Reviews'] = str(stats['rating']['count'])
                    elif 'review_count' in stats:
                        result['Number_of_Reviews'] = str(stats['review_count'])
                    
                    # Star rating
                    if 'rating' in stats and 'average' in stats['rating']:
                        result['Star_Rating'] = str(stats['rating']['average'])
                    elif 'average_overall_rating' in stats:
                        result['Star_Rating'] = str(stats['average_overall_rating'])
            
            # Extract images from enrichment.images
            if 'enrichment' in product and 'images' in product['enrichment']:
                images = product['enrichment']['images']
                
                if 'primary_image_url' in images:
                    result['Image_1_URL'] = images['primary_image_url']
                
                if 'alternate_image_urls' in images and images['alternate_image_urls']:
                    alt_images = images['alternate_image_urls']
                    for i, img_url in enumerate(alt_images[:2], 2):  # Start from Image_2
                        if img_url and i <= 3:
                            result[f'Image_{i}_URL'] = img_url
            
            # Alternative image extraction from item.enrichment
            elif 'item' in product and 'enrichment' in product['item']:
                enrichment = product['item']['enrichment']
                if 'images' in enrichment:
                    images = enrichment['images']
                    
                    # Primary image
                    if 'primary_image_url' in images:
                        result['Image_1_URL'] = images['primary_image_url']
                    
                    # Alternative images
                    if 'alternate_image_urls' in images and images['alternate_image_urls']:
                        for i, img_url in enumerate(images['alternate_image_urls'][:2], 2):
                            if img_url:
                                result[f'Image_{i}_URL'] = img_url
            
            # Fallback: look for any images in the product data
            if result['Image_1_URL'] == 'N/A':
                self._extract_fallback_images(product, result)
            
            logger.info(f"Successfully extracted data for TCIN {tcin}: {result['Title'][:50]}...")
            return result if result['Title'] != 'N/A' else None
            
        except Exception as e:
            logger.error(f"Error parsing API response for TCIN {tcin}: {str(e)}")
            # Log the structure for debugging
            logger.debug(f"API response structure keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            return None
    
    def _extract_fallback_images(self, product, result):
        """Fallback method to extract images from various locations in product data"""
        try:
            # Search recursively for image URLs
            def find_images_recursive(obj, images_found):
                if len(images_found) >= 3:
                    return images_found
                
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if 'image' in key.lower() and isinstance(value, str) and 'http' in value:
                            if value not in images_found:
                                images_found.append(value)
                        elif isinstance(value, (dict, list)):
                            images_found = find_images_recursive(value, images_found)
                elif isinstance(obj, list):
                    for item in obj:
                        images_found = find_images_recursive(item, images_found)
                
                return images_found
            
            images = find_images_recursive(product, [])
            
            for i, img_url in enumerate(images[:3], 1):
                result[f'Image_{i}_URL'] = img_url
                
        except Exception as e:
            logger.error(f"Error in fallback image extraction: {str(e)}")
    
    def _parse_html_response(self, tcin, html_content):
        """Parse HTML page content"""
        result = self._create_base_record(tcin)
        
        try:
            # Check for 404 or error pages
            error_indicators = [
                "Oops! Something went wrong",
                "Page not found",
                "product is no longer available",
                "404"
            ]
            
            for indicator in error_indicators:
                if indicator.lower() in html_content.lower():
                    return self._create_invalid_tcin_record(tcin, "Product not found")
            
            # Look for JSON-LD structured data
            jsonld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
            jsonld_matches = re.findall(jsonld_pattern, html_content, re.DOTALL)
            
            for jsonld_content in jsonld_matches:
                try:
                    jsonld_data = json.loads(jsonld_content.strip())
                    if self._extract_from_jsonld(result, jsonld_data):
                        return result
                except json.JSONDecodeError:
                    continue
            
            # Look for Target's internal data structures
            data_patterns = [
                r'window\.__TGT_DATA__\s*=\s*({.+?});',
                r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
                r'window\.__APOLLO_STATE__\s*=\s*({.+?});'
            ]
            
            for pattern in data_patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        extracted_data = self._extract_from_page_data(data)
                        if extracted_data:
                            result.update(extracted_data)
                            if result['Title'] != 'N/A':
                                return result
                    except json.JSONDecodeError:
                        continue
            
            # Fallback: Try to extract from HTML directly
            title_patterns = [
                r'<h1[^>]*>([^<]+)</h1>',
                r'<title>([^<|]+)',
                r'data-test="product-title"[^>]*>([^<]+)<'
            ]
            
            for pattern in title_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    title = match.group(1).strip()
                    if len(title) > 10 and 'target' not in title.lower():  # Basic validation
                        result['Title'] = title[:200]  # Limit length
                        break
            
            # Try to extract price from HTML
            price_patterns = [
                r'\$(\d+\.?\d*)',
                r'price["\'][^>]*>[\s]*\$?(\d+\.?\d*)',
                r'data-test="product-price"[^>]*>([^<]*\$[^<]*)<'
            ]
            
            for pattern in price_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    # Take first valid price
                    for price in matches:
                        try:
                            float(price)
                            result['Regular_Price'] = f"${price}"
                            result['Sale_Price'] = f"${price}"
                            break
                        except ValueError:
                            continue
                    if result['Regular_Price'] != 'N/A':
                        break
            
            return result if result['Title'] != 'N/A' else None
            
        except Exception as e:
            logger.error(f"Error parsing HTML for TCIN {tcin}: {str(e)}")
            return None
    
    def _extract_from_jsonld(self, result, data):
        """Extract data from JSON-LD structured data"""
        try:
            if isinstance(data, list):
                for item in data:
                    if self._extract_from_jsonld(result, item):
                        return True
            elif isinstance(data, dict):
                if data.get('@type') == 'Product':
                    result['Title'] = data.get('name', result['Title'])
                    
                    if 'brand' in data:
                        brand = data['brand']
                        if isinstance(brand, dict):
                            result['Brand'] = brand.get('name', result['Brand'])
                        else:
                            result['Brand'] = str(brand)
                    
                    if 'offers' in data:
                        offers = data['offers']
                        if isinstance(offers, dict):
                            price = offers.get('price', '')
                            if price:
                                result['Regular_Price'] = f"${price}" if not str(price).startswith('$') else str(price)
                                result['Sale_Price'] = result['Regular_Price']
                        elif isinstance(offers, list) and len(offers) > 0:
                            price = offers[0].get('price', '')
                            if price:
                                result['Regular_Price'] = f"${price}" if not str(price).startswith('$') else str(price)
                                result['Sale_Price'] = result['Regular_Price']
                    
                    if 'aggregateRating' in data:
                        rating = data['aggregateRating']
                        result['Star_Rating'] = rating.get('ratingValue', result['Star_Rating'])
                        result['Number_of_Reviews'] = rating.get('reviewCount', result['Number_of_Reviews'])
                    
                    if 'image' in data:
                        images = data['image']
                        if isinstance(images, list):
                            for i, img in enumerate(images[:3]):
                                result[f'Image_{i+1}_URL'] = img
                        elif isinstance(images, str):
                            result['Image_1_URL'] = images
                    
                    return True
            
            return False
        except Exception:
            return False
    
    def _extract_from_page_data(self, data):
        """Extract data from page's JavaScript data structures"""
        extracted = {}
        
        try:
            # This is a simplified extraction - Target's structure is complex
            def search_recursive(obj, target_keys):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if key in target_keys:
                            return value
                        if isinstance(value, (dict, list)):
                            result = search_recursive(value, target_keys)
                            if result:
                                return result
                elif isinstance(obj, list):
                    for item in obj:
                        result = search_recursive(item, target_keys)
                        if result:
                            return result
                return None
            
            # Look for common field names
            title = search_recursive(data, ['title', 'display_name', 'product_name'])
            if title and isinstance(title, str):
                extracted['Title'] = title
            
            brand = search_recursive(data, ['brand', 'manufacturer'])
            if brand:
                if isinstance(brand, dict):
                    extracted['Brand'] = brand.get('name', brand.get('display_name', ''))
                elif isinstance(brand, str):
                    extracted['Brand'] = brand
            
            return extracted if extracted else None
            
        except Exception:
            return None
    
    def _create_base_record(self, tcin):
        """Create base record structure"""
        return {
            'TCIN': tcin,
            'Title': 'N/A',
            'Brand': 'N/A',
            'Regular_Price': 'N/A',
            'Sale_Price': 'N/A',
            'Number_of_Reviews': 'N/A',
            'Star_Rating': 'N/A',
            'Image_1_URL': 'N/A',
            'Image_2_URL': 'N/A',
            'Image_3_URL': 'N/A',
            'Status': 'Success'
        }
    
    def _create_invalid_tcin_record(self, tcin, reason):
        """Create a record for invalid TCIN"""
        return {
            'TCIN': tcin,
            'Title': 'TCIN not valid',
            'Brand': 'N/A',
            'Regular_Price': 'N/A',
            'Sale_Price': 'N/A',
            'Number_of_Reviews': 'N/A',
            'Star_Rating': 'N/A',
            'Image_1_URL': 'N/A',
            'Image_2_URL': 'N/A',
            'Image_3_URL': 'N/A',
            'Status': f'Invalid: {reason}'
        }

def main():
    st.set_page_config(
        page_title="Target TCIN Scraper",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Target TCIN Product Data Scraper v4")
    st.markdown("Extract product data from Target.com using TCINs (Advanced HTTP Scraping)")
    
    # Success notice
    st.success("✅ **No Chrome WebDriver Required** - This version uses advanced HTTP scraping techniques!")
    
    # Sidebar for instructions
    with st.sidebar:
        st.header("ℹ️ Instructions")
        st.markdown("""
        1. Enter TCINs (one per line) or upload a file
        2. Click 'Scrape Products' to start
        3. Download results as CSV or Excel
        
        **New Approach:**
        - Uses Target's internal API endpoints
        - Fallback to advanced HTML scraping
        - Multiple extraction strategies
        - Smart retry logic with backoff
        
        **Features:** 
        - 2-5 second delays between requests
        - User agent rotation
        - Multiple retry strategies
        - Rate limit handling
        
        **Supported Formats:**
        - Text input (one TCIN per line)
        - CSV file with TCIN column
        - Excel file with TCIN column
        """)
        
        st.header("🔍 Extraction Strategy")
        st.markdown("""
        **Primary:** Target API endpoints
        **Secondary:** Page HTML parsing
        **Fallback:** JSON-LD structured data
        
        **Data Sources:**
        - Product title and brand
        - Regular and sale prices
        - Review counts and ratings
        - First 3 product images
        """)
    
    # Input methods
    tab1, tab2 = st.tabs(["📝 Text Input", "📁 File Upload"])
    
    tcins = []
    
    with tab1:
        st.subheader("Enter TCINs")
        tcin_input = st.text_area(
            "Enter TCINs (one per line):",
            placeholder="94635949\n91938797\n92165451\n92234671",
            height=150
        )
        
        if tcin_input.strip():
            tcins = [tcin.strip() for tcin in tcin_input.strip().split('\n') if tcin.strip()]
    
    with tab2:
        st.subheader("Upload File")
        uploaded_file = st.file_uploader(
            "Upload CSV or Excel file with TCINs",
            type=['csv', 'xlsx', 'xls']
        )
        
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                # Look for TCIN column
                tcin_columns = [col for col in df.columns if 'tcin' in col.lower()]
                if tcin_columns:
                    tcin_col = st.selectbox("Select TCIN column:", tcin_columns)
                    tcins = df[tcin_col].astype(str).tolist()
                else:
                    st.error("No TCIN column found. Please ensure your file has a column with 'TCIN' in the name.")
                    
                if tcins:
                    st.success(f"Found {len(tcins)} TCINs in uploaded file")
                    
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
    
    # Display TCINs to be processed
    if tcins:
        st.subheader(f"📋 TCINs to Process ({len(tcins)})")
        with st.expander("View TCINs", expanded=False):
            st.write(tcins)
        
        # Performance warning
        if len(tcins) > 50:
            st.warning(f"⚠️ Processing {len(tcins)} TCINs will take approximately {len(tcins) * 4 // 60} minutes due to rate limiting.")
    
    # Scraping section
    if tcins and st.button("🚀 Scrape Products", type="primary"):
        scraper = TargetScraper()
        
        try:
            st.info("🔄 Starting advanced HTTP scraping...")
            
            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            results = []
            
            # Show live results
            results_df_placeholder = st.empty()
            
            # Process each TCIN
            for i, tcin in enumerate(tcins):
                status_text.text(f"Processing TCIN {tcin} ({i+1}/{len(tcins)}) - Trying API first...")
                
                result = scraper.get_target_api_data(tcin)
                results.append(result)
                
                progress_bar.progress((i + 1) / len(tcins))
                
                # Update live results every 5 items
                if (i + 1) % 5 == 0 or i == 0:
                    temp_df = pd.DataFrame(results)
                    results_df_placeholder.dataframe(temp_df, use_container_width=True)
            
            status_text.text("✅ Processing complete!")
            
            # Convert to DataFrame
            df_results = pd.DataFrame(results)
            
            # Display final results
            st.subheader("📊 Final Results")
            
            # Summary stats
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_processed = len(df_results)
                st.metric("Total Processed", total_processed)
            
            with col2:
                successful = len(df_results[df_results['Status'] == 'Success'])
                st.metric("Successful", successful)
            
            with col3:
                failed = total_processed - successful
                st.metric("Failed/Invalid", failed)
            
            with col4:
                success_rate = f"{(successful/total_processed)*100:.1f}%" if total_processed > 0 else "0%"
                st.metric("Success Rate", success_rate)
            
            # Show results table
            st.dataframe(df_results, use_container_width=True)
            
            # Show sample successful result
            successful_results = df_results[df_results['Status'] == 'Success']
            if len(successful_results) > 0:
                st.subheader("✅ Sample Successful Result")
                sample = successful_results.iloc[0].to_dict()
                st.json(sample)
            
            # Download options
            st.subheader("💾 Download Results")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # CSV download
                csv_buffer = BytesIO()
                df_results.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                st.download_button(
                    label="📄 Download CSV",
                    data=csv_data,
                    file_name=f"target_products_{timestamp}.csv",
                    mime="text/csv"
                )
            
            with col2:
                # Excel download
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df_results.to_excel(writer, index=False, sheet_name='Target_Products')
                excel_data = excel_buffer.getvalue()
                
                st.download_button(
                    label="📊 Download Excel",
                    data=excel_data,
                    file_name=f"target_products_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
            st.info("💡 Try reducing the number of TCINs or check your internet connection.")
    
    elif not tcins:
        st.info("👆 Please enter TCINs or upload a file to get started")

if __name__ == "__main__":
    main()