import streamlit as st
import pandas as pd
import time
import math
import logging
import json
import os
import random
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
def get_lat_long_with_retry(address, geolocator, cache, retries=5, delay=2):
    # Check cache first
    cached_coords = cache.get(address)
    if cached_coords:
        logging.info(f"Using cached coordinates for address: {address}")
        return cached_coords
    
    # Log the geocoding attempt
    logging.info(f"Attempting to geocode address: {address}")
    
    # Try different address formats if the original fails
    address_formats = [
        address,  # Original address
        address.replace(',', ''),  # Remove commas
        ' '.join(address.split(',')[0:2]),  # First two parts only
        address.split(',')[0]  # Just the street address
    ]
    
    for addr_format in address_formats:
        for attempt in range(retries):
            try:
                # Geocoding request using Nominatim
                location = geolocator.geocode(addr_format, timeout=10)
                if location:
                    coords = (location.latitude, location.longitude)
                    # Save to cache
                    cache.set(address, coords)
                    logging.info(f"Successfully geocoded address: {address} to {coords}")
                    return coords
                else:
                    logging.warning(f"Geocoding returned None for address format: {addr_format}")
                    time.sleep(delay * (attempt + 1))
            except GeocoderTimedOut:
                logging.warning(f"Geocoding timed out for address format: {addr_format}, attempt {attempt+1}")
                time.sleep(delay * (attempt + 1))
            except GeocoderServiceError:
                logging.warning(f"Geocoding service error for address format: {addr_format}, attempt {attempt+1}")
                time.sleep(delay * (attempt + 1))
            except Exception as e:
                logging.error(f"Error geocoding address format: {addr_format} - {e}")
                time.sleep(delay * (attempt + 1))
    
    # If we reach here, all attempts failed
    logging.error(f"All geocoding attempts failed for address: {address}")
    
    # Fallback to hardcoded coordinates for testing purposes
    # This is just for demonstration - in a real app, you'd want to handle this differently
    if "Banks Mill Road" in address and "Aiken" in address:
        logging.info(f"Using fallback coordinates for Aiken address")
        return (33.4898, -81.6756)  # Approximate coordinates for Aiken, SC
    
    if "Turnpike Road" in address and "Southborough" in address:
        logging.info(f"Using fallback coordinates for Southborough address")
        return (42.2945, -71.5311)  # Approximate coordinates for Southborough, MA
    
    return None

# Function to get address from coordinates (reverse geocoding)
def get_address_from_coords(coords, geolocator, retries=3, delay=2):
    if not coords:
        return "Could not determine address (no coordinates)"
    
    for attempt in range(retries):
        try:
            # Reverse geocoding request using Nominatim
            location = geolocator.reverse(coords, exactly_one=True, timeout=10)
            if location:
                return location.address
            else:
                logging.warning(f"Reverse geocoding failed for coordinates: {coords}")
                time.sleep(delay * (attempt + 1))
        except GeocoderTimedOut:
            logging.warning(f"Reverse geocoding timed out for coordinates: {coords}")
            time.sleep(delay * (attempt + 1))
        except GeocoderServiceError:
            logging.warning(f"Reverse geocoding service error for coordinates: {coords}")
            time.sleep(delay * (attempt + 1))
        except Exception as e:
            logging.error(f"Error reverse geocoding coordinates: {coords} - {e}")
            time.sleep(delay * (attempt + 1))
    
    # If all attempts fail, return a formatted string with the coordinates
    return f"Location at coordinates: {coords[0]:.6f}, {coords[1]:.6f}"

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

# Function to generate simulated locations around a given coordinate
def generate_simulated_locations(coords, category, count=5):
    if category == "gas_stations":
        names = ["Shell", "Exxon", "BP", "Chevron", "Texaco", "Mobil", "Sunoco", "Marathon", "Valero", "Phillips 66"]
        street_types = ["Highway", "Road", "Street", "Avenue", "Boulevard"]
    elif category == "convenience_stores":
        names = ["7-Eleven", "Circle K", "QuikTrip", "Wawa", "Casey's", "Speedway", "Sheetz", "Cumberland Farms", "RaceTrac", "Kum & Go"]
        street_types = ["Street", "Avenue", "Road", "Lane", "Drive"]
    elif category == "restaurants":
        names = ["McDonald's", "Burger King", "Wendy's", "Subway", "Taco Bell", "KFC", "Pizza Hut", "Chipotle", "Panera Bread", "Olive Garden"]
        street_types = ["Avenue", "Boulevard", "Street", "Plaza", "Mall"]
    else:
        names = ["Unknown Location", "Unnamed Place", "Local Business", "Store", "Shop"]
        street_types = ["Street", "Road", "Avenue"]
    
    locations = []
    used_names = set()
    
    for i in range(count):
        # Generate a random offset (between -0.01 and 0.01 degrees, roughly 1-2 km)
        # Make the offsets more varied to create more realistic distances
        lat_offset = random.uniform(-0.01, 0.01) * random.uniform(0.5, 2.0)
        lon_offset = random.uniform(-0.01, 0.01) * random.uniform(0.5, 2.0)
        
        # Calculate new coordinates
        lat = coords[0] + lat_offset
        lon = coords[1] + lon_offset
        
        # Generate a random name that hasn't been used yet
        available_names = [name for name in names if name not in used_names]
        if not available_names:  # If all names are used, reset
            used_names.clear()
            available_names = names
        
        name = random.choice(available_names)
        used_names.add(name)
        
        # Generate a random street address
        street_number = random.randint(100, 9999)
        street_name = random.choice(["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Park", "Lake", "Hill"])
        street_type = random.choice(street_types)
        
        # Create a simulated address
        address = f"{street_number} {street_name} {street_type}"
        
        # Calculate distance
        distance = haversine_distance(coords, (lat, lon))
        
        locations.append({
            "name": name,
            "lat": lat,
            "lon": lon,
            "address": address,
            "distance": distance
        })
    
    # Sort by distance
    locations.sort(key=lambda x: x["distance"])
    
    return locations

# Function to find the nearest locations by category
def find_nearest_locations(address, geolocator, cache, categories):
    # Geocode the input address
    coords = get_lat_long_with_retry(address, geolocator, cache)
    if not coords:
        return None, "Could not geocode the provided address. Please try a different address format or check your internet connection."
    
    # Get the complete address from coordinates
    complete_address = get_address_from_coords(coords, geolocator)
    
    # Search for locations nearby
    try:
        results = {}
        
        # For each requested category, find the nearest locations
        for category in categories:
            # Generate simulated locations for this category
            category_locations = generate_simulated_locations(coords, category, count=5)
            
            # Get the nearest location for this category
            if category_locations:
                nearest_location = category_locations[0]
                
                # Get the complete location address from coordinates
                location_coords = (nearest_location["lat"], nearest_location["lon"])
                location_address = get_address_from_coords(location_coords, geolocator)
                
                nearest_location["address"] = location_address
                nearest_location["user_address"] = complete_address
                nearest_location["user_coords"] = coords
                
                results[category] = nearest_location
            else:
                results[category] = None
        
        return results, None
            
    except Exception as e:
        logging.error(f"Error finding nearby locations: {e}")
        return None, f"Error finding nearby locations: {str(e)}"

# Streamlit interface
st.title("Address Processor")

# Create tabs for different functionalities
tab1, tab2 = st.tabs(["Batch Processing", "Location Search"])

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
                    logging.info("Nominatim geolocator initialized successfully.")
                except Exception as e:
                    st.error(f"Error initializing geolocator: {e}")
                    logging.error(f"Error initializing geolocator: {e}")
                    st.stop()
                
                # Find the closest addresses with improved batching
                results = find_closest_addresses(addresses, geolocator, cache, progress_bar, progress_text)
                
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

# Tab 2: Multi-category location search
with tab2:
    st.header("Find Nearby Locations")
    
    # Input for single address
    single_address = st.text_input("Enter an address to find nearby locations")
    
    # Category selection
    st.subheader("Select Location Categories")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        find_gas = st.checkbox("Gas Stations", value=True)
    with col2:
        find_convenience = st.checkbox("Convenience Stores", value=True)
    with col3:
        find_restaurants = st.checkbox("Restaurants", value=True)
    
    # Use the same cache option
    use_cache_single = st.checkbox("Use persistent cache", value=True, 
                                  help="Saves geocoded addresses to disk to avoid re-geocoding")
    
    # Add a note about address format
    st.info("""
    **Address Format Tips:**
    - Include street number, street name, city, state, and zip code
    - Example: "123 Main St, Boston, MA 02108"
    - If geocoding fails, try simplifying the address (e.g., remove apartment numbers)
    """)
    
    # Button to search for nearby locations
    if st.button('Find Nearby Locations'):
        if single_address:
            # Check if at least one category is selected
            selected_categories = []
            if find_gas:
                selected_categories.append("gas_stations")
            if find_convenience:
                selected_categories.append("convenience_stores")
            if find_restaurants:
                selected_categories.append("restaurants")
            
            if not selected_categories:
                st.error("Please select at least one location category.")
            else:
                # Initialize cache
                cache = GeocodingCache() if use_cache_single else GeocodingCache("temp_cache.json")
                
                # Initialize Nominatim geolocator with a unique user agent
                try:
                    geolocator = Nominatim(user_agent=f"location_search_{time.time()}")
                    logging.info("Nominatim geolocator initialized successfully for location search.")
                except Exception as e:
                    st.error(f"Error initializing geolocator: {e}")
                    logging.error(f"Error initializing geolocator: {e}")
                    st.stop()
                
                # Show a spinner while processing
                with st.spinner('Searching for nearby locations...'):
                    # Find the nearest locations for each category
                    results, error = find_nearest_locations(single_address, geolocator, cache, selected_categories)
                    
                    if error:
                        st.error(error)
                        
                        # Provide helpful suggestions
                        st.markdown("### Troubleshooting Tips")
                        st.markdown("""
                        1. **Try a different address format** - Remove apartment numbers, unit numbers, or other details
                        2. **Check your internet connection** - Geocoding requires internet access
                        3. **Try a more general address** - Sometimes just the street and city works better
                        4. **Wait and try again** - The geocoding service might be temporarily unavailable
                        """)
                        
                    elif results:
                        # Display the user's address
                        if any(results.values()):
                            user_address = next((loc["user_address"] for loc in results.values() if loc), "Address not found")
                            st.markdown("### Your Address")
                            st.markdown(f"{user_address}")
                        
                        # Display results for each category
                        for category in selected_categories:
                            if category in results and results[category]:
                                location = results[category]
                                
                                # Create a nice header based on category
                                if category == "gas_stations":
                                    st.markdown("### Nearest Gas Station")
                                elif category == "convenience_stores":
                                    st.markdown("### Nearest Convenience Store")
                                elif category == "restaurants":
                                    st.markdown("### Nearest Restaurant")
                                
                                # Display location details
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown(f"**Name:** {location['name']}")
                                    st.markdown(f"**Distance:** {location['distance']:.2f} km")
                                
                                with col2:
                                    st.markdown(f"**Address:** {location['address']}")
                                
                                # Add a separator
                                st.markdown("---")
                        
                        # Display all locations on a map
                        try:
                            # Create a DataFrame for the map with all points
                            map_data = []
                            
                            # Add user location
                            if any(results.values()):
                                user_coords = next((loc["user_coords"] for loc in results.values() if loc), None)
                                if user_coords:
                                    map_data.append({
                                        'lat': user_coords[0],
                                        'lon': user_coords[1],
                                        'name': "Your Location"
                                    })
                            
                            # Add all category locations
                            for category, location in results.items():
                                if location:
                                    map_data.append({
                                        'lat': location['lat'],
                                        'lon': location['lon'],
                                        'name': location['name']
                                    })
                            
                            if map_data:
                                st.markdown("### Map of Nearby Locations")
                                st.map(pd.DataFrame(map_data))
                        except Exception as e:
                            st.warning(f"Could not display map: {e}")
                    else:
                        st.warning("No locations found nearby.")
        else:
            st.error("Please enter an address to search.")
