import streamlit as st
import pandas as pd
import time
import math
import logging
import json
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from functools import partial

# Set up logging
logging.basicConfig(filename="geocoding_logs.txt", level=logging.INFO)

# Function to calculate the Haversine distance between two coordinates
def haversine_distance(coords1, coords2):
    R = 6371  # Radius of Earth in km
    lat1 = coords1[0] * (math.pi / 180)
    lat2 = coords2[0] * (math.pi / 180)
    delta_lat = (coords2[0] - coords1[0]) * (math.pi / 180)
    delta_lng = (coords2[1] - coords1[1]) * (math.pi / 180)

    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c  # Distance in km

# Persistent cache for geocoded addresses
class GeocodingCache:
    def __init__(self, cache_file="geocode_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()
    
    def _load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading cache: {e}")
                return {}
        return {}
    
    def _save_cache(self):
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
        except Exception as e:
            logging.error(f"Error saving cache: {e}")
    
    def get(self, address):
        return self.cache.get(address)
    
    def set(self, address, coords):
        self.cache[address] = coords
        self._save_cache()

# Function to get latitude and longitude with retries and caching
def get_lat_long_with_retry(address, geolocator, cache, retries=3, delay=2):
    # Check cache first
    cached_coords = cache.get(address)
    if cached_coords:
        return cached_coords
    
    for attempt in range(retries):
        try:
            # Geocoding request using Nominatim
            location = geolocator(address)
            if location:
                coords = (location.latitude, location.longitude)
                # Save to cache
                cache.set(address, coords)
                return coords
            else:
                logging.warning(f"Geocoding failed for address: {address}")
                return None
        except GeocoderTimedOut:
            logging.warning(f"Geocoding timed out for address: {address}")
            time.sleep(delay * (attempt + 1))
        except GeocoderServiceError:
            logging.warning(f"Geocoding service error for address: {address}")
            time.sleep(delay * (attempt + 1))
        except Exception as e:
            logging.error(f"Error geocoding address: {address} - {e}")
            time.sleep(delay * (attempt + 1))
    return None

# Function to get address from coordinates (reverse geocoding)
def get_address_from_coords(coords, geolocator, retries=3, delay=2):
    if not coords:
        return "Could not determine address (no coordinates)"
    
    for attempt in range(retries):
        try:
            # Reverse geocoding request using Nominatim
            location = geolocator.reverse(coords, exactly_one=True)
            if location:
                return location.address
            else:
                logging.warning(f"Reverse geocoding failed for coordinates: {coords}")
                return "Could not determine complete address"
        except GeocoderTimedOut:
            logging.warning(f"Reverse geocoding timed out for coordinates: {coords}")
            time.sleep(delay * (attempt + 1))
        except GeocoderServiceError:
            logging.warning(f"Reverse geocoding service error for coordinates: {coords}")
            time.sleep(delay * (attempt + 1))
        except Exception as e:
            logging.error(f"Error reverse geocoding coordinates: {coords} - {e}")
            time.sleep(delay * (attempt + 1))
    
    return "Could not determine complete address"

# Batch processing function
def process_address_batch(addresses, batch_size=50):
    """Split addresses into manageable batches"""
    for i in range(0, len(addresses), batch_size):
        yield addresses[i:i + batch_size]

# Function to find the closest addresses with batching and progress tracking
def find_closest_addresses(addresses, geolocator, cache, progress_bar, progress_text):
    results = {addr: {"closest": "", "distance": float('inf')} for addr in addresses}
    
    # First, geocode all addresses in batches
    total_addresses = len(addresses)
    geocoded_addresses = {}
    
    progress_text.text("Phase 1/2: Geocoding all addresses")
    
    address_count = 0
    for batch in process_address_batch(addresses, 50):
        for address in batch:
            coords = get_lat_long_with_retry(address, geolocator, cache)
            if coords:
                geocoded_addresses[address] = coords
            # Respect Nominatim's usage policy
            time.sleep(1)
            address_count += 1
            progress_bar.progress(address_count / (total_addresses * 2))  # First half of progress
            progress_text.text(f"Phase 1/2: Geocoding addresses ({address_count}/{total_addresses})")
    
    # Now calculate distances between all geocoded addresses
    progress_text.text("Phase 2/2: Calculating distances")
    
    processed_count = 0
    geocoded_items = list(geocoded_addresses.items())
    total_comparisons = len(geocoded_items)
    
    for i, (origin, origin_coords) in enumerate(geocoded_items):
        for j, (destination, destination_coords) in enumerate(geocoded_items):
            if origin != destination:
                distance = haversine_distance(origin_coords, destination_coords)
                if distance < results[origin]["distance"]:
                    results[origin]["closest"] = destination
                    results[origin]["distance"] = distance
        
        processed_count += 1
        current_progress = (total_addresses + processed_count) / (total_addresses * 2)  # Second half of progress
        progress_bar.progress(current_progress)
        progress_text.text(f"Phase 2/2: Calculating distances ({processed_count}/{total_comparisons})")
    
    # Format results
    formatted_results = []
    for address in addresses:
        if address in results and results[address]["closest"]:
            formatted_results.append(f"{results[address]['closest']} ({results[address]['distance']:.2f} km)")
        else:
            formatted_results.append("Geocoding error or no close address found")
    
    return formatted_results

# Function to find the nearest convenience store to a single address
def find_nearest_convenience_store(address, geolocator, cache):
    # Geocode the input address
    coords = get_lat_long_with_retry(address, geolocator, cache)
    if not coords:
        return None, "Could not geocode the provided address"
    
    # Get the complete address from coordinates
    complete_address = get_address_from_coords(coords, geolocator)
    
    # Search for convenience stores nearby
    try:
        # In a real implementation, you would use Overpass API or Google Places API
        # For demonstration, we'll create some sample stores around the given coordinates
        
        # Sample convenience stores with real-world names (simulated)
        sample_stores = [
            {"name": "7-Eleven", "lat": coords[0] + 0.01, "lon": coords[1] + 0.005},
            {"name": "Circle K", "lat": coords[0] - 0.008, "lon": coords[1] + 0.002},
            {"name": "Walgreens", "lat": coords[0] + 0.003, "lon": coords[1] - 0.007},
            {"name": "CVS Pharmacy", "lat": coords[0] - 0.005, "lon": coords[1] - 0.003},
            {"name": "Family Mart", "lat": coords[0] + 0.007, "lon": coords[1] - 0.001},
        ]
        
        # Calculate distances to each store
        nearest_store = None
        min_distance = float('inf')
        
        for store in sample_stores:
            store_coords = (store["lat"], store["lon"])
            distance = haversine_distance(coords, store_coords)
            
            # Get the complete store address from coordinates
            store_address = get_address_from_coords(store_coords, geolocator)
            
            store["distance"] = distance
            store["address"] = store_address
            
            if distance < min_distance:
                min_distance = distance
                nearest_store = store
        
        if nearest_store:
            # Add the user's complete address to the result
            nearest_store["user_address"] = complete_address
            nearest_store["user_coords"] = coords
            return nearest_store, None
        else:
            return None, "No convenience stores found nearby"
            
    except Exception as e:
        logging.error(f"Error finding convenience stores: {e}")
        return None, f"Error finding convenience stores: {str(e)}"

# Streamlit interface
st.title("Address Processor")

# Create tabs for different functionalities
tab1, tab2 = st.tabs(["Batch Processing", "Single Address Search"])

# Tab 1: Original batch processing functionality
with tab1:
    st.header("Large Address Dataset Processor")
    
    # Upload file (CSV format)
    uploaded_file = st.file_uploader("Upload your CSV file with addresses", type=["csv"])

    if uploaded_file is not None:
        # Read the CSV
        df = pd.read_csv(uploaded_file)
        
        # Display sample of data
        st.write("Preview of your data (first 5 rows):", df.head())
        
        # Get the list of addresses (assuming addresses are in the first column)
        addresses = df.iloc[:, 0].dropna().tolist()
        
        st.write(f"Total addresses detected: {len(addresses)}")
        
        # Warning for large datasets
        if len(addresses) > 100:
            st.warning(f"""
            ⚠️ You have {len(addresses)} addresses. Processing may take a long time due to rate limits.
            
            Estimated processing time: {len(addresses) * 2} minutes or more.
            
            Consider using a smaller dataset for testing or running this process in the background.
            """)
        
        # Options for processing
        st.subheader("Processing Options")
        
        col1, col2 = st.columns(2)
        with col1:
            use_cache = st.checkbox("Use persistent cache (recommended)", value=True, 
                                   help="Saves geocoded addresses to disk to avoid re-geocoding if process is interrupted")
        
        with col2:
            batch_size = st.slider("Batch size", min_value=10, max_value=100, value=50, 
                                  help="Number of addresses to process in each batch")
        
        # Show the progress text and bar
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        # Button to run the calculation
        if st.button('Find Closest Addresses'):
            if len(addresses) > 0:
                start_time = time.time()
                
                # Initialize cache
                cache = GeocodingCache() if use_cache else GeocodingCache("temp_cache.json")
                
                # Initialize Nominatim geolocator
                try:
                    geolocator = Nominatim(user_agent="large_address_processor")
                    geocode = partial(geolocator.geocode, timeout=10)
                    logging.info("Nominatim geolocator initialized successfully.")
                except Exception as e:
                    st.error(f"Error initializing geolocator: {e}")
                    logging.error(f"Error initializing geolocator: {e}")
                    st.stop()
                
                # Find the closest addresses with improved batching
                results = find_closest_addresses(addresses, geocode, cache, progress_bar, progress_text)
                
                # Add results to the dataframe
                df['Closest Address'] = [result.split(' (')[0] if '(' in result else result for result in results]
                df['Distance (km)'] = [result.split('(')[-1].replace(')', '').strip() if '(' in result else 'N/A' for result in results]
                
                # Display results
                st.subheader("Results")
                st.dataframe(df)
                
                # Download button for the results
                csv = df.to_csv(index=False)
                st.download_button("Download results as CSV", csv, "results.csv", "text/csv")
                
                # Display processing time
                end_time = time.time()
                processing_time = end_time - start_time
                st.success(f"Processing completed in {processing_time:.2f} seconds ({processing_time/60:.2f} minutes)")
                
            else:
                st.error("Please upload a file with addresses.")

# Tab 2: Single address search functionality
with tab2:
    st.header("Find Nearest Convenience Store")
    
    # Input for single address
    single_address = st.text_input("Enter an address to find the nearest convenience store")
    
    # Use the same cache option
    use_cache_single = st.checkbox("Use persistent cache", value=True, 
                                  help="Saves geocoded addresses to disk to avoid re-geocoding")
    
    # Button to search for the nearest convenience store
    if st.button('Find Nearest Convenience Store'):
        if single_address:
            # Initialize cache
            cache = GeocodingCache() if use_cache_single else GeocodingCache("temp_cache.json")
            
            # Initialize Nominatim geolocator
            try:
                geolocator = Nominatim(user_agent="single_address_processor")
                logging.info("Nominatim geolocator initialized successfully for single address search.")
            except Exception as e:
                st.error(f"Error initializing geolocator: {e}")
                logging.error(f"Error initializing geolocator: {e}")
                st.stop()
            
            # Show a spinner while processing
            with st.spinner('Searching for the nearest convenience store...'):
                # Find the nearest convenience store
                nearest_store, error = find_nearest_convenience_store(single_address, geolocator, cache)
                
                if error:
                    st.error(error)
                elif nearest_store:
                    # Display the result in a nice format
                    st.success(f"Found the nearest store!")
                    
                    # Create a card-like display for the result
                    st.markdown("### Your Address")
                    st.markdown(f"{nearest_store['user_address']}")
                    
                    st.markdown("### Nearest Store")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Store Name:** {nearest_store['name']}")
                        st.markdown(f"**Distance:** {nearest_store['distance']:.2f} km")
                    
                    with col2:
                        st.markdown(f"**Store Address:** {nearest_store['address']}")
                        st.markdown(f"**Coordinates:** {nearest_store['lat']:.6f}, {nearest_store['lon']:.6f}")
                    
                    # Display on a map if possible
                    try:
                        # Create a DataFrame for the map with both points
                        map_data = pd.DataFrame({
                            'lat': [nearest_store['lat'], nearest_store['user_coords'][0]],
                            'lon': [nearest_store['lon'], nearest_store['user_coords'][1]],
                            'name': [nearest_store['name'], "Your Location"]
                        })
                        
                        # Display the map
                        st.map(map_data)
                    except Exception as e:
                        st.warning(f"Could not display map: {e}")
                else:
                    st.warning("No stores found nearby.")
        else:
            st.error("Please enter an address to search.")
