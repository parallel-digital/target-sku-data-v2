import streamlit as st
import pandas as pd
import time
import json
import re
from io import BytesIO
from datetime import datetime
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TargetScraper:
    def __init__(self):
        self.driver = None
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in background
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Faster loading
            chrome_options.add_argument("--disable-javascript")  # We'll enable this selectively
            
            # User agent to appear more legitimate
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Install and setup ChromeDriver
            service = Service(ChromeDriverManager().install())
            
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(30)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup driver: {str(e)}")
            return False
    
    def extract_product_data(self, tcin, max_retries=3):
        """Extract product data from Target page for given TCIN"""
        
        if not self.driver:
            if not self.setup_driver():
                return self._create_invalid_tcin_record(tcin, "Driver setup failed")
        
        for attempt in range(max_retries):
            try:
                # Construct Target URL
                url = f"https://www.target.com/p/-/A-{tcin}"
                
                # Add delay to respect rate limits
                if attempt > 0:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    time.sleep(2)  # Initial delay
                
                logger.info(f"Attempting to load {url}")
                self.driver.get(url)
                
                # Wait for page to load
                wait = WebDriverWait(self.driver, 15)
                
                # Check if product exists by looking for error indicators
                try:
                    # Look for "page not found" or similar error messages
                    error_indicators = [
                        "//h1[contains(text(), 'Oops!')]",
                        "//h1[contains(text(), 'Page not found')]",
                        "//div[contains(text(), 'product is no longer available')]",
                        "//*[contains(text(), '404')]"
                    ]
                    
                    for indicator in error_indicators:
                        try:
                            error_element = self.driver.find_element(By.XPATH, indicator)
                            if error_element:
                                return self._create_invalid_tcin_record(tcin, "Product not found")
                        except NoSuchElementException:
                            continue
                    
                    # Wait for main product content to load
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "main")))
                    
                    # Additional wait for dynamic content
                    time.sleep(3)
                    
                    # Extract product data using multiple strategies
                    product_data = self._extract_product_fields_selenium(tcin)
                    
                    if product_data['Title'] != 'N/A' or product_data['Status'] == 'Success':
                        return product_data
                    
                    # If no data extracted, try page source parsing
                    page_source = self.driver.page_source
                    fallback_data = self._parse_page_source(tcin, page_source)
                    if fallback_data:
                        return fallback_data
                    
                    if attempt < max_retries - 1:
                        logger.warning(f"No data extracted for TCIN {tcin}, attempt {attempt + 1}")
                        continue
                    
                    return self._create_invalid_tcin_record(tcin, "Could not extract product data")
                
                except TimeoutException:
                    if attempt < max_retries - 1:
                        logger.warning(f"Timeout for TCIN {tcin}, attempt {attempt + 1}")
                        continue
                    return self._create_invalid_tcin_record(tcin, "Page load timeout")
                
            except WebDriverException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"WebDriver error for TCIN {tcin}, attempt {attempt + 1}: {str(e)}")
                    continue
                return self._create_invalid_tcin_record(tcin, f"WebDriver error: {str(e)}")
            
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Unexpected error for TCIN {tcin}, attempt {attempt + 1}: {str(e)}")
                    continue
                return self._create_invalid_tcin_record(tcin, f"Unexpected error: {str(e)}")
        
        return self._create_invalid_tcin_record(tcin, "Max retries exceeded")
    
    def _extract_product_fields_selenium(self, tcin):
        """Extract product fields using Selenium element finding"""
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
        
        try:
            # Extract title using multiple selectors
            title_selectors = [
                "h1[data-test='product-title']",
                "h1",
                "[data-test='product-title']",
                "h1.ProductTitle-sc-1j4q1yp-0",
                ".ProductTitle__Title"
            ]
            
            for selector in title_selectors:
                try:
                    title_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if title_element and title_element.text.strip():
                        result['Title'] = title_element.text.strip()
                        break
                except NoSuchElementException:
                    continue
            
            # Extract brand
            brand_selectors = [
                "[data-test='product-brand'] a",
                "[data-test='product-brand']",
                ".ProductBrand",
                "a[href*='/b/']"
            ]
            
            for selector in brand_selectors:
                try:
                    brand_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if brand_element and brand_element.text.strip():
                        result['Brand'] = brand_element.text.strip()
                        break
                except NoSuchElementException:
                    continue
            
            # Extract pricing
            price_selectors = [
                "[data-test='product-price'] span",
                "[data-test='product-price']",
                ".ProductPrice",
                ".Price__Current"
            ]
            
            for selector in price_selectors:
                try:
                    price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if price_elements:
                        prices = [elem.text.strip() for elem in price_elements if elem.text.strip() and '$' in elem.text]
                        if len(prices) >= 2:
                            result['Regular_Price'] = prices[1] if len(prices) > 1 else prices[0]
                            result['Sale_Price'] = prices[0]
                        elif len(prices) == 1:
                            result['Regular_Price'] = prices[0]
                            result['Sale_Price'] = prices[0]
                        break
                except NoSuchElementException:
                    continue
            
            # Extract reviews and ratings
            try:
                rating_element = self.driver.find_element(By.CSS_SELECTOR, "[data-test='ratings-and-reviews'] span")
                if rating_element:
                    rating_text = rating_element.text.strip()
                    # Try to extract rating number (e.g., "4.5 out of 5 stars")
                    rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                    if rating_match:
                        result['Star_Rating'] = rating_match.group(1)
            except NoSuchElementException:
                pass
            
            try:
                reviews_element = self.driver.find_element(By.CSS_SELECTOR, "[data-test='ratings-and-reviews'] button")
                if reviews_element:
                    reviews_text = reviews_element.text.strip()
                    # Try to extract review count
                    reviews_match = re.search(r'(\d+)', reviews_text)
                    if reviews_match:
                        result['Number_of_Reviews'] = reviews_match.group(1)
            except NoSuchElementException:
                pass
            
            # Extract images
            image_selectors = [
                "[data-test='hero-image-carousel'] img",
                "[data-test='product-image'] img",
                ".ProductImages img",
                "img[src*='target.com']"
            ]
            
            for selector in image_selectors:
                try:
                    image_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    image_urls = []
                    
                    for img in image_elements[:3]:  # First 3 images
                        src = img.get_attribute('src')
                        if src and ('target.com' in src or src.startswith('http')):
                            image_urls.append(src)
                    
                    for i, url in enumerate(image_urls[:3]):
                        result[f'Image_{i+1}_URL'] = url
                    
                    if image_urls:
                        break
                        
                except NoSuchElementException:
                    continue
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting fields for TCIN {tcin}: {str(e)}")
            return self._create_invalid_tcin_record(tcin, f"Field extraction error: {str(e)}")
    
    def _parse_page_source(self, tcin, page_source):
        """Fallback method to parse page source HTML"""
        try:
            # Look for JSON-LD structured data
            jsonld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
            jsonld_matches = re.findall(jsonld_pattern, page_source, re.DOTALL)
            
            for jsonld_content in jsonld_matches:
                try:
                    jsonld_data = json.loads(jsonld_content.strip())
                    if isinstance(jsonld_data, dict) and jsonld_data.get('@type') == 'Product':
                        return self._extract_from_jsonld(tcin, jsonld_data)
                    elif isinstance(jsonld_data, list):
                        for item in jsonld_data:
                            if isinstance(item, dict) and item.get('@type') == 'Product':
                                return self._extract_from_jsonld(tcin, item)
                except json.JSONDecodeError:
                    continue
            
            # Look for window.__TGT_DATA__ or similar
            tgt_pattern = r'window\.__TGT_DATA__\s*=\s*({.+?});'
            tgt_match = re.search(tgt_pattern, page_source, re.DOTALL)
            if tgt_match:
                try:
                    tgt_data = json.loads(tgt_match.group(1))
                    result = self._extract_from_tgt_data(tcin, tgt_data)
                    if result:
                        return result
                except json.JSONDecodeError:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing page source for TCIN {tcin}: {str(e)}")
            return None
    
    def _extract_from_jsonld(self, tcin, data):
        """Extract data from JSON-LD structured data"""
        result = self._create_base_record(tcin)
        
        try:
            result['Title'] = data.get('name', 'N/A')
            
            if 'brand' in data:
                brand = data['brand']
                if isinstance(brand, dict):
                    result['Brand'] = brand.get('name', 'N/A')
                else:
                    result['Brand'] = str(brand)
            
            if 'offers' in data:
                offers = data['offers']
                if isinstance(offers, dict):
                    result['Regular_Price'] = offers.get('price', 'N/A')
                    result['Sale_Price'] = offers.get('price', 'N/A')
                elif isinstance(offers, list) and len(offers) > 0:
                    result['Regular_Price'] = offers[0].get('price', 'N/A')
                    result['Sale_Price'] = offers[0].get('price', 'N/A')
            
            if 'aggregateRating' in data:
                rating = data['aggregateRating']
                result['Star_Rating'] = rating.get('ratingValue', 'N/A')
                result['Number_of_Reviews'] = rating.get('reviewCount', 'N/A')
            
            if 'image' in data:
                images = data['image']
                if isinstance(images, list):
                    for i, img in enumerate(images[:3]):
                        result[f'Image_{i+1}_URL'] = img
                elif isinstance(images, str):
                    result['Image_1_URL'] = images
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting from JSON-LD for TCIN {tcin}: {str(e)}")
            return None
    
    def _extract_from_tgt_data(self, tcin, data):
        """Extract data from Target's __TGT_DATA__"""
        # This would need to be implemented based on Target's current structure
        # For now, return None to trigger other extraction methods
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
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

def main():
    st.set_page_config(
        page_title="Target TCIN Scraper",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Target TCIN Product Data Scraper v2")
    st.markdown("Extract product data from Target.com using TCINs (Browser-based)")
    
    # Important notice
    st.warning("⚠️ **Requirements**: This app uses Selenium Chrome WebDriver. Make sure Chrome is installed on the server where this is deployed.")
    
    # Sidebar for instructions
    with st.sidebar:
        st.header("ℹ️ Instructions")
        st.markdown("""
        1. Enter TCINs (one per line) or upload a file
        2. Click 'Scrape Products' to start
        3. Download results as CSV or Excel
        
        **New Features:**
        - Browser-based scraping (more reliable)
        - Better handling of dynamic content
        - Improved error detection
        
        **Rate Limiting:** 
        - 2-3 second delays between requests
        - Automatic retries on failures
        - Exponential backoff for rate limits
        
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
        
        # Limit for demo purposes
        if len(tcins) > 20:
            st.warning(f"⚠️ Processing {len(tcins)} TCINs may take a long time. Consider testing with fewer TCINs first.")
    
    # Scraping section
    if tcins and st.button("🚀 Scrape Products", type="primary"):
        scraper = None
        
        try:
            st.info("🔄 Setting up Chrome WebDriver...")
            scraper = TargetScraper()
            
            if not scraper.setup_driver():
                st.error("❌ Failed to setup Chrome WebDriver. Make sure Chrome is installed.")
                return
            
            st.success("✅ WebDriver setup complete!")
            
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
                
                # Show live results
                if i == 0:  # Show first result as example
                    st.write("**Sample result:**", result)
            
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
        
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
            
        finally:
            # Clean up WebDriver
            if scraper:
                scraper.close()
                st.info("🧹 WebDriver cleaned up")
    
    elif not tcins:
        st.info("👆 Please enter TCINs or upload a file to get started")

if __name__ == "__main__":
    main()