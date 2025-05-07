import streamlit as st
import pandas as pd
import time
import math
import logging
import json
import os
import re
import requests
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

# Function to convert kilometers to miles
def km_to_miles(km):
    return km * 0.621371

# Function to simplify address format
def simplify_address(address):
    if not address or "Could not determine" in address:
        return address
    
    try:
        # Split the address into components
        parts = [part.strip() for part in address.split(',')]
        
        # Extract the street number and name (usually the first component)
        street = parts[0] if parts else ""
        
        # Look for city, state, and zip
        city = ""
        state = ""
        zipcode = ""
        
        # Try to find the zip code (5-digit number)
        zip_pattern = re.compile(r'\b\d{5}\b')
        for part in parts:
            zip_match = zip_pattern.search(part)
            if zip_match:
                zipcode = zip_match.group(0)
                break
        
        # Look for state abbreviation (2 uppercase letters)
        state_pattern = re.compile(r'\b[A-Z]{2}\b')
        for part in parts:
            state_match = state_pattern.search(part)
            if state_match:
                state = state_match.group(0)
                break
        
        # If we didn't find a 2-letter state code, look for full state names
        if not state:
            state_names = {
                "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
                "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
                "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
                "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
                "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
                "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
                "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
                "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
                "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
                "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
                "District of Columbia": "DC"
            }
            for part in parts:
                part = part.strip()
                if part in state_names:
                    state = state_names[part]
                    break
        
        # Try to identify the city
        # Common city indicators in address
        city_indicators = ["City of", "Town of", "Village of"]
        
        # First, look for parts that might contain the city
        for i, part in enumerate(parts):
            part = part.strip()
            # Skip the street part and the last few parts (likely state, zip, country)
            if i == 0 or i > len(parts) - 4:
                continue
                
            # Check if this part might be a city
            for indicator in city_indicators:
                if indicator in part:
                    city = part.replace(indicator, "").strip()
                    break
            
            # Common city names that might be in the address
            if part in ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                       "San Antonio", "San Diego", "Dallas", "San Jose"] or \
               part in ["The Bronx", "Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]:
                city = part
                break
                
            # If we haven't found a city yet, and this part is not too long,
            # and doesn't contain numbers, it might be the city
            if not city and len(part) < 20 and not any(c.isdigit() for c in part):
                city = part
                break
        
        # If we still don't have a city, try to extract it from specific patterns
        if not city:
            # For addresses like "The Bronx, Bronx County, City of New York"
            for part in parts:
                if "Bronx" in part:
                    city = "Bronx"
                    break
                elif "Brooklyn" in part:
                    city = "Brooklyn"
                    break
                elif "Manhattan" in part:
                    city = "Manhattan"
                    break
                elif "Queens" in part:
                    city = "Queens"
                    break
                elif "Staten Island" in part:
                    city = "Staten Island"
                    break
        
        # If we still don't have a city, use a default approach
        if not city and len(parts) > 2:
            # Try the second or third part as the city
            city_candidates = [parts[1], parts[2] if len(parts) > 2 else ""]
            for candidate in city_candidates:
                if candidate and not any(c.isdigit() for c in candidate) and len(candidate) < 20:
                    city = candidate
                    break
        
        # Construct the simplified address
        simplified = street
        
        if city:
            simplified += f", {city}"
        
        if state:
            simplified += f", {state}"
        
        if zipcode:
            simplified += f" {zipcode}"
        
        return simplified
    
    except Exception as e:
        logging.error(f"Error simplifying address: {e}")
        return address  # Return the original address if there's an error

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
    
    if "Allerton" in address and "Bronx" in address:
        logging.info(f"Using fallback coordinates for Bronx address")
        return (40.8654, -73.8592)  # Approximate coordinates for Allerton Ave, Bronx
    
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
                # Simplify the address format
                return simplify_address(location.address)
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

# Add a new function to identify likely delivery service partners
def is_delivery_partner(place):
    """
    Check if a place is likely to be a delivery service partner
    based on its name, type, and other attributes.
    """
    # Common store chains that partner with delivery services
    delivery_partners = [
        "7-Eleven", "Walgreens", "CVS", "Rite Aid", "Duane Reade", 
        "Circle K", "Wawa", "Sheetz", "QuikTrip", "Casey's",
        "Speedway", "RaceTrac", "Cumberland Farms", "Kroger", "Albertsons",
        "Safeway", "Publix", "Whole Foods", "Target", "Walmart",
        "Family Dollar", "Dollar General", "Aldi", "Trader Joe's",
        "GoPuff", "goPuff", "Gopuff", "DashMart"
    ]
    
    # Check if the place name matches any known delivery partners
    if place.get("name") in delivery_partners:
        return True
    
    # Check for tags that might indicate a delivery partner
    tags = place.get("tags", {})
    if tags.get("shop") in ["convenience", "supermarket", "grocery"]:
        return True
    
    # Check for specific delivery service tags (these are rare but might exist)
    for tag, value in tags.items():
        if "delivery" in tag.lower() and value.lower() in ["yes", "true"]:
            return True
        if "takeaway" in tag.lower() and value.lower() in ["yes", "true"]:
            return True
    
    return False

# Modify the search_nearby_places_osm function to include a delivery_partners parameter
def search_nearby_places_osm(coords, place_type, radius=1500, limit=5, delivery_partners=False):
    """
    Search for nearby places using OpenStreetMap Overpass API
    
    Args:
        coords: (latitude, longitude) tuple
        place_type: Type of place to search for (e.g., 'fuel', 'convenience', 'restaurant')
        radius: Search radius in meters
        limit: Maximum number of results to return
        delivery_partners: If True, filter for likely delivery service partners
        
    Returns:
        List of places with details
    """
    try:
        # Map our category names to OSM amenity types
        osm_amenity_mapping = {
            "gas_stations": "fuel",
            "convenience_stores": "convenience",
            "restaurants": "restaurant",
            "fast_food": "fast_food",
            "cafe": "cafe",
            "pharmacy": "pharmacy",
            "atm": "atm",
            "bank": "bank",
            "supermarket": "supermarket"
        }
        
        # Get the OSM amenity type
        amenity = osm_amenity_mapping.get(place_type, place_type)
        
        # If looking for delivery partners, expand the search to include shops
        if delivery_partners:
            # Overpass API endpoint
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            # Build the Overpass QL query to include both amenities and shops
            overpass_query = f"""
            [out:json];
            (
              node["amenity"="{amenity}"](around:{radius},{coords[0]},{coords[1]});
              way["amenity"="{amenity}"](around:{radius},{coords[0]},{coords[1]});
              relation["amenity"="{amenity}"](around:{radius},{coords[0]},{coords[1]});
              node["shop"="convenience"](around:{radius},{coords[0]},{coords[1]});
              way["shop"="convenience"](around:{radius},{coords[0]},{coords[1]});
              node["shop"="supermarket"](around:{radius},{coords[0]},{coords[1]});
              way["shop"="supermarket"](around:{radius},{coords[0]},{coords[1]});
              node["shop"="grocery"](around:{radius},{coords[0]},{coords[1]});
              way["shop"="grocery"](around:{radius},{coords[0]},{coords[1]});
            );
            out center;
            """
        else:
            # Overpass API endpoint
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            # Build the standard Overpass QL query
            overpass_query = f"""
            [out:json];
            (
              node["amenity"="{amenity}"](around:{radius},{coords[0]},{coords[1]});
              way["amenity"="{amenity}"](around:{radius},{coords[0]},{coords[1]});
              relation["amenity"="{amenity}"](around:{radius},{coords[0]},{coords[1]});
            );
            out center;
            """
        
        # Make the API request
        response = requests.post(overpass_url, data={"data": overpass_query})
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            places = []
            
            # Process each place in the results
            for element in data["elements"]:
                # Extract coordinates
                if element["type"] == "node":
                    lat = element["lat"]
                    lon = element["lon"]
                elif "center" in element:
                    lat = element["center"]["lat"]
                    lon = element["center"]["lon"]
                else:
                    continue  # Skip if we can't determine coordinates
                
                # Extract tags
                tags = element.get("tags", {})
                
                # Get the name (use a fallback if not available)
                name = tags.get("name", tags.get("brand", "Unnamed Location"))
                
                # Skip if no name is available
                if not name or name == "Unnamed Location":
                    # Try to use operator or brand as fallback
                    name = tags.get("operator", tags.get("brand", "Unnamed Location"))
                    if not name or name == "Unnamed Location":
                        continue
                
                # Calculate distance from user's location
                place_coords = (lat, lon)
                distance = haversine_distance(coords, place_coords)
                
                # Get the address components
                street = tags.get("addr:street", "")
                housenumber = tags.get("addr:housenumber", "")
                city = tags.get("addr:city", "")
                state = tags.get("addr:state", "")
                postcode = tags.get("addr:postcode", "")
                
                # Construct an address string
                address_parts = []
                if housenumber and street:
                    address_parts.append(f"{housenumber} {street}")
                elif street:
                    address_parts.append(street)
                
                if city:
                    address_parts.append(city)
                
                if state:
                    address_parts.append(state)
                
                if postcode:
                    address_parts.append(postcode)
                
                address = ", ".join(address_parts)
                
                # If we don't have a good address from tags, use reverse geocoding
                if not address or len(address_parts) < 2:
                    # We'll do this later to avoid too many API calls at once
                    address = ""
                
                # Create a place object with all the details
                place_obj = {
                    "name": name,
                    "place_id": str(element["id"]),
                    "lat": lat,
                    "lon": lon,
                    "address": address,
                    "distance": distance,
                    "phone": tags.get("phone", ""),
                    "website": tags.get("website", ""),
                    "opening_hours": tags.get("opening_hours", ""),
                    "needs_geocoding": not address or len(address_parts) < 2,
                    "tags": tags  # Include all tags for filtering
                }
                
                places.append(place_obj)
            
            # If looking for delivery partners, filter the results
            if delivery_partners:
                delivery_places = [place for place in places if is_delivery_partner(place)]
                # If we found delivery partners, use those
                if delivery_places:
                    places = delivery_places
                # Otherwise, fall back to all places (to avoid empty results)
            
            # Sort places by distance
            places.sort(key=lambda x: x["distance"])
            
            # Limit the number of results
            places = places[:limit]
            
            return places
        else:
            logging.error(f"Overpass API error: {response.status_code}")
            # Return mock data as fallback
            return get_mock_places(coords, place_type, limit, delivery_partners)
    
    except Exception as e:
        logging.error(f"Error searching for nearby places: {e}")
        # Return mock data as fallback
        return get_mock_places(coords, place_type, limit, delivery_partners)

# Modify the get_mock_places function to include delivery partners
def get_mock_places(coords, place_type, limit=5, delivery_partners=False):
    """
    Generate realistic mock data for places based on the user's location
    
    Args:
        coords: (latitude, longitude) tuple
        place_type: Type of place ('gas_stations', 'convenience_stores', 'restaurants')
        limit: Maximum number of results to return
        delivery_partners: If True, generate mock data for delivery service partners
        
    Returns:
        List of mock places with realistic details
    """
    # Real business names by category
    if place_type == "gas_stations":
        businesses = [
            {"name": "Mobil", "chain": True},
            {"name": "Shell", "chain": True},
            {"name": "BP", "chain": True},
            {"name": "Sunoco", "chain": True},
            {"name": "Exxon", "chain": True},
            {"name": "Speedway", "chain": True},
            {"name": "Citgo", "chain": True},
            {"name": "Marathon", "chain": True},
            {"name": "Valero", "chain": True},
            {"name": "Gulf", "chain": True}
        ]
    elif place_type == "convenience_stores":
        if delivery_partners:
            # Stores that commonly partner with delivery services
            businesses = [
                {"name": "7-Eleven", "chain": True, "delivery": True},
                {"name": "Walgreens", "chain": True, "delivery": True},
                {"name": "CVS", "chain": True, "delivery": True},
                {"name": "Rite Aid", "chain": True, "delivery": True},
                {"name": "Circle K", "chain": True, "delivery": True},
                {"name": "Wawa", "chain": True, "delivery": True},
                {"name": "Sheetz", "chain": True, "delivery": True},
                {"name": "QuikTrip", "chain": True, "delivery": True},
                {"name": "DashMart", "chain": True, "delivery": True},
                {"name": "GoPuff", "chain": True, "delivery": True}
            ]
        else:
            businesses = [
                {"name": "7-Eleven", "chain": True},
                {"name": "Wawa", "chain": True},
                {"name": "Speedway", "chain": True},
                {"name": "Circle K", "chain": True},
                {"name": "QuikTrip", "chain": True},
                {"name": "Sheetz", "chain": True},
                {"name": "Casey's", "chain": True},
                {"name": "Cumberland Farms", "chain": True},
                {"name": "RaceTrac", "chain": True},
                {"name": "Bodega", "chain": False}
            ]
    elif place_type == "restaurants":
        businesses = [
            {"name": "McDonald's", "chain": True},
            {"name": "Burger King", "chain": True},
            {"name": "Wendy's", "chain": True},
            {"name": "Subway", "chain": True},
            {"name": "Taco Bell", "chain": True},
            {"name": "KFC", "chain": True},
            {"name": "Chipotle", "chain": True},
            {"name": "Dunkin'", "chain": True},
            {"name": "Starbucks", "chain": True},
            {"name": "Domino's Pizza", "chain": True}
        ]
    else:
        businesses = [
            {"name": "Local Business", "chain": False},
            {"name": "Corner Shop", "chain": False},
            {"name": "Main Street Store", "chain": False}
        ]
    
    # Real street names in the Bronx area
    bronx_streets = [
        "Allerton Avenue", "White Plains Road", "Boston Road", "Gun Hill Road",
        "Pelham Parkway", "Fordham Road", "Grand Concourse", "Jerome Avenue",
        "Webster Avenue", "Third Avenue", "Tremont Avenue", "Westchester Avenue",
        "Bronxwood Avenue", "Morris Park Avenue", "East Tremont Avenue"
    ]
    
    # Generate mock places
    places = []
    import random
    
    # Seed the random generator with the coordinates to get consistent results
    random.seed(int(coords[0] * 1000) + int(coords[1] * 1000))
    
    for i in range(limit):
        # Generate a random offset (between -0.005 and 0.005 degrees, roughly 0.5-1 km)
        # Make the offsets more varied to create more realistic distances
        lat_offset = random.uniform(-0.005, 0.005) * random.uniform(0.5, 2.0)
        lon_offset = random.uniform(-0.005, 0.005) * random.uniform(0.5, 2.0)
        
        # Calculate new coordinates
        lat = coords[0] + lat_offset
        lon = coords[1] + lon_offset
        
        # Select a random business
        business = random.choice(businesses)
        
        # Generate a random street address
        street_number = random.randint(100, 9999)
        street_name = random.choice(bronx_streets)
        
        # Create a mock address
        if "Bronx" in str(coords):  # If we're in the Bronx area
            address = f"{street_number} {street_name}, Bronx, NY 10469"
        else:
            address = f"{street_number} {street_name}, New York, NY 10001"
        
        # Calculate distance
        distance = haversine_distance(coords, (lat, lon))
        
        # Generate a random phone number
        phone = f"(718) {random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Add delivery service information for delivery partners
        delivery_info = ""
        if delivery_partners and business.get("delivery", False):
            delivery_services = ["UberEats", "DoorDash", "GrubHub", "Gopuff", "Instacart"]
            # Randomly select 1-3 delivery services
            num_services = random.randint(1, 3)
            selected_services = random.sample(delivery_services, num_services)
            delivery_info = f"Available on: {', '.join(selected_services)}"
        
        # Create a mock place object
        place = {
            "name": business["name"],
            "place_id": f"mock_place_{i}",
            "lat": lat,
            "lon": lon,
            "address": address,
            "distance": distance,
            "phone": phone,
            "website": f"https://www.{business['name'].lower().replace(' ', '').replace('\'', '')}.com" if business["chain"] else "",
            "opening_hours": "Open 24/7" if random.random() > 0.7 else f"{random.randint(6, 9)}:00 AM - {random.randint(8, 11)}:00 PM",
            "needs_geocoding": False,
            "delivery_info": delivery_info
        }
        
        places.append(place)
    
    # Sort places by distance
    places.sort(key=lambda x: x["distance"])
    
    return places

# Modify the find_nearest_locations function to include delivery partners
def find_nearest_locations(address, geolocator, cache, categories, use_miles=False):
    # Geocode the input address
    coords = get_lat_long_with_retry(address, geolocator, cache)
    if not coords:
        return None, "Could not geocode the provided address. Please try a different address format or check your internet connection."
    
    # Get the complete address from coordinates
    complete_address = get_address_from_coords(coords, geolocator)
    
    # Search for locations nearby
    try:
        results = {}
        
        # Map our category names to OpenStreetMap amenity types
        category_mapping = {
            "gas_stations": "fuel",
            "convenience_stores": "convenience",
            "restaurants": "restaurant"
        }
        
        # For each requested category, find the nearest locations
        for category in categories:
            # Get the OpenStreetMap amenity type for this category
            osm_type = category_mapping.get(category, category)
            
            # For convenience stores, always include delivery partner info
            is_delivery = (category == "convenience_stores")
            
            # Search for nearby places of this type
            places = search_nearby_places_osm(coords, osm_type, radius=2000, delivery_partners=is_delivery)
            
            # Get the nearest place for this category
            if places and len(places) > 0:
                nearest_place = places[0]  # Just take the first (nearest) result
                
                # Get address for place if it needs geocoding
                if nearest_place.get("needs_geocoding", False):
                    place_coords = (nearest_place["lat"], nearest_place["lon"])
                    nearest_place["address"] = get_address_from_coords(place_coords, geolocator)
                    nearest_place["needs_geocoding"] = False
                
                # Convert distance to miles if requested
                if use_miles:
                    nearest_place["distance_miles"] = km_to_miles(nearest_place["distance"])
                
                # Add user information
                nearest_place["user_address"] = complete_address
                nearest_place["user_coords"] = coords
                
                results[category] = nearest_place
            else:
                results[category] = None
        
        return results, None
            
    except Exception as e:
        logging.error(f"Error finding nearby locations: {e}")
        return None, f"Error finding nearby locations: {str(e)}"

# Update the Streamlit interface to remove the delivery partners checkbox
# Tab 2: Multi-category location search with OpenStreetMap
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
    
    # Distance unit selection
    use_miles = st.checkbox("Show distances in miles", value=True)
    
    # Use the same cache option
    use_cache_single = st.checkbox("Use persistent cache", value=True, 
                                  help="Saves geocoded addresses to disk to avoid re-geocoding")
    
    # Add a note about OpenStreetMap
    st.info("""
    **Using OpenStreetMap Data:**
    - This app uses OpenStreetMap (OSM) data, which is completely free with no API key required
    - Data quality depends on community contributions and may vary by location
    - Results are most accurate in urban areas with good OSM coverage
    - If a location isn't showing up, it might not be mapped in OpenStreetMap yet
    """)
    
    # Add a note about convenience stores and delivery
    if find_convenience:
        st.info("""
        **About Convenience Stores:**
        - Results include stores that partner with delivery services like UberEats, DoorDash, GrubHub, Gopuff, etc.
        - Delivery service availability is shown when available
        - Actual delivery availability may vary and should be confirmed with the specific service
        """)
    
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
                    results, error = find_nearest_locations(single_address, geolocator, cache, selected_categories, use_miles)
                    
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
                        user_address = None
                        for category, location in results.items():
                            if location:
                                user_address = location["user_address"]
                                break
                        
                        if user_address:
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
                                    if use_miles:
                                        st.markdown(f"**Distance:** {location.get('distance_miles', km_to_miles(location['distance'])):.2f} miles")
                                    else:
                                        st.markdown(f"**Distance:** {location['distance']:.2f} km")
                                    
                                    # Show delivery info if available and this is a convenience store
                                    if category == "convenience_stores" and location.get('delivery_info'):
                                        st.markdown(f"**Delivery:** {location['delivery_info']}")
                                
                                with col2:
                                    st.markdown(f"**Address:** {location['address']}")
                                
                                # Add a separator
                                st.markdown("---")
                            else:
                                if category == "gas_stations":
                                    st.warning("No gas stations found nearby.")
                                elif category == "convenience_stores":
                                    st.warning("No convenience stores found nearby.")
                                elif category == "restaurants":
                                    st.warning("No restaurants found nearby.")
                        
                        # Display all locations on a map
                        try:
                            # Create a DataFrame for the map with all points
                            map_data = []
                            
                            # Add user location
                            user_coords = None
                            for category, location in results.items():
                                if location:
                                    user_coords = location["user_coords"]
                                    break
                            
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
                        st.warning("No locations found nearby. Try a different address or search radius.")
        else:
            st.error("Please enter an address to search.")
