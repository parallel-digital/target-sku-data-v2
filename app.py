import streamlit as st
import requests
import pandas as pd
import time
import json
import re
from io import BytesIO
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TargetScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
    def extract_product_data(self, tcin, max_retries=3):
        """Extract product data from Target page for given TCIN"""
        
        for attempt in range(max_retries):
            try:
                # Construct Target URL
                url = f"https://www.target.com/p/-/A-{tcin}"
                
                # Add delay to respect rate limits
                time.sleep(1.5)  # 1.5 second delay between requests
                
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 404:
                    return self._create_invalid_tcin_record(tcin, "Product not found")
                
                if response.status_code != 200:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed for TCIN {tcin}. Status: {response.status_code}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return self._create_invalid_tcin_record(tcin, f"HTTP {response.status_code}")
                
                # Extract JSON data from the page
                html_content = response.text
                
                # Look for __TGT_DATA__ which contains product information
                tgt_data_match = re.search(r'window\.__TGT_DATA__\s*=\s*({.+?});', html_content, re.DOTALL)
                if tgt_data_match:
                    try:
                        tgt_data = json.loads(tgt_data_match.group(1))
                        product_data = self._parse_tgt_data(tcin, tgt_data)
                        if product_data:
                            return product_data
                    except json.JSONDecodeError:
                        pass
                
                # Fallback: Look for Apollo State data
                apollo_match = re.search(r'window\.__APOLLO_STATE__\s*=\s*({.+?});', html_content, re.DOTALL)
                if apollo_match:
                    try:
                        apollo_data = json.loads(apollo_match.group(1))
                        product_data = self._parse_apollo_data(tcin, apollo_data)
                        if product_data:
                            return product_data
                    except json.JSONDecodeError:
                        pass
                
                # Fallback: Try to extract from JSON-LD structured data
                jsonld_matches = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
                for jsonld_content in jsonld_matches:
                    try:
                        jsonld_data = json.loads(jsonld_content.strip())
                        product_data = self._parse_jsonld_data(tcin, jsonld_data)
                        if product_data:
                            return product_data
                    except json.JSONDecodeError:
                        continue
                
                # If no structured data found, return invalid
                return self._create_invalid_tcin_record(tcin, "Could not extract product data")
                
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed for TCIN {tcin}, attempt {attempt + 1}: {str(e)}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return self._create_invalid_tcin_record(tcin, f"Request failed: {str(e)}")
        
        return self._create_invalid_tcin_record(tcin, "Max retries exceeded")
    
    def _parse_tgt_data(self, tcin, data):
        """Parse Target's __TGT_DATA__ structure"""
        try:
            # Navigate through the data structure to find product info
            for key, value in data.items():
                if isinstance(value, dict) and 'product' in value:
                    product = value['product']
                    return self._extract_product_fields(tcin, product)
                    
            # Alternative path - look in different locations
            if 'product' in data:
                return self._extract_product_fields(tcin, data['product'])
                
        except Exception as e:
            logger.error(f"Error parsing TGT data for TCIN {tcin}: {str(e)}")
        
        return None
    
    def _parse_apollo_data(self, tcin, data):
        """Parse Apollo GraphQL state data"""
        try:
            # Look for product objects in Apollo state
            for key, value in data.items():
                if key.startswith('Product:') and isinstance(value, dict):
                    return self._extract_product_fields(tcin, value)
                    
        except Exception as e:
            logger.error(f"Error parsing Apollo data for TCIN {tcin}: {str(e)}")
        
        return None
    
    def _parse_jsonld_data(self, tcin, data):
        """Parse JSON-LD structured data"""
        try:
            if isinstance(data, dict) and data.get('@type') == 'Product':
                return self._extract_product_fields(tcin, data)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') == 'Product':
                        return self._extract_product_fields(tcin, item)
                        
        except Exception as e:
            logger.error(f"Error parsing JSON-LD data for TCIN {tcin}: {str(e)}")
        
        return None
    
    def _extract_product_fields(self, tcin, product_data):
        """Extract required fields from product data"""
        try:
            # Initialize result
            result = {
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
            
            # Extract title
            title_fields = ['title', 'display_name', 'name', 'product_description', 'item']
            for field in title_fields:
                if field in product_data and product_data[field]:
                    if isinstance(product_data[field], dict):
                        result['Title'] = product_data[field].get('title', product_data[field].get('name', 'N/A'))
                    else:
                        result['Title'] = str(product_data[field])
                    break
            
            # Extract brand
            brand_fields = ['brand', 'manufacturer']
            for field in brand_fields:
                if field in product_data:
                    if isinstance(product_data[field], dict):
                        result['Brand'] = product_data[field].get('name', 'N/A')
                    else:
                        result['Brand'] = str(product_data[field])
                    break
            
            # Extract pricing
            if 'price' in product_data:
                price_data = product_data['price']
                if isinstance(price_data, dict):
                    # Regular price
                    if 'current_retail' in price_data:
                        result['Regular_Price'] = price_data['current_retail']
                    elif 'list_price' in price_data:
                        result['Regular_Price'] = price_data['list_price']
                    
                    # Sale price
                    if 'current_retail_min' in price_data:
                        result['Sale_Price'] = price_data['current_retail_min']
                    elif 'offers' in price_data:
                        offers = price_data['offers']
                        if isinstance(offers, dict) and 'price' in offers:
                            result['Sale_Price'] = offers['price']
                        elif isinstance(offers, list) and len(offers) > 0:
                            result['Sale_Price'] = offers[0].get('price', 'N/A')
            
            # Extract reviews
            if 'reviews' in product_data:
                reviews = product_data['reviews']
                if isinstance(reviews, dict):
                    result['Number_of_Reviews'] = reviews.get('count', reviews.get('total_count', 'N/A'))
                    result['Star_Rating'] = reviews.get('average_overall_rating', reviews.get('rating', 'N/A'))
            
            # Extract images
            images = []
            image_fields = ['images', 'image', 'media']
            
            for field in image_fields:
                if field in product_data:
                    image_data = product_data[field]
                    if isinstance(image_data, list):
                        for img in image_data[:3]:  # First 3 images
                            if isinstance(img, dict):
                                url = img.get('base_url', img.get('url', img.get('src', '')))
                                if url:
                                    images.append(url)
                            elif isinstance(img, str):
                                images.append(img)
                    elif isinstance(image_data, dict):
                        url = image_data.get('base_url', image_data.get('url', ''))
                        if url:
                            images.append(url)
                    break
            
            # Assign image URLs
            for i, img_url in enumerate(images[:3]):
                result[f'Image_{i+1}_URL'] = img_url
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting fields for TCIN {tcin}: {str(e)}")
            return self._create_invalid_tcin_record(tcin, f"Field extraction error: {str(e)}")
    
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
    
    st.title("🎯 Target TCIN Product Data Scraper")
    st.markdown("Extract product data from Target.com using TCINs")
    
    # Sidebar for instructions
    with st.sidebar:
        st.header("ℹ️ Instructions")
        st.markdown("""
        1. Enter TCINs (one per line) or upload a file
        2. Click 'Scrape Products' to start
        3. Download results as CSV or Excel
        
        **Rate Limiting:** 
        - 1.5 second delay between requests
        - Automatic retries on failures
        
        **Supported Formats:**
        - Text input (one TCIN per line)
        - CSV file with TCIN column
        - Excel file with TCIN column
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
    
    # Scraping section
    if tcins and st.button("🚀 Scrape Products", type="primary"):
        scraper = TargetScraper()
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        results = []
        
        # Process each TCIN
        for i, tcin in enumerate(tcins):
            status_text.text(f"Processing TCIN {tcin} ({i+1}/{len(tcins)})")
            
            result = scraper.extract_product_data(tcin)
            results.append(result)
            
            progress_bar.progress((i + 1) / len(tcins))
        
        status_text.text("✅ Processing complete!")
        
        # Convert to DataFrame
        df_results = pd.DataFrame(results)
        
        # Display results
        st.subheader("📊 Results")
        
        # Summary stats
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_processed = len(df_results)
            st.metric("Total Processed", total_processed)
        
        with col2:
            successful = len(df_results[df_results['Status'] == 'Success'])
            st.metric("Successful", successful)
        
        with col3:
            failed = total_processed - successful
            st.metric("Failed/Invalid", failed)
        
        # Show results table
        st.dataframe(df_results, use_container_width=True)
        
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
    
    elif not tcins:
        st.info("👆 Please enter TCINs or upload a file to get started")

if __name__ == "__main__":
    main()