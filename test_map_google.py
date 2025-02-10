import googlemaps
from datetime import datetime

class GoogleMapsScraper:
    def __init__(self, api_key):
        self.gmaps = googlemaps.Client(key=api_key)
    
    def find_company_website(self, name: str, province: str) -> str:
        try:
            query = f"{name} {province} sitio web"
            places_result = self.gmaps.places(
                query=query,
                language='es',
                region='es'
            )
            
            if places_result['results']:
                place_details = self.gmaps.place(
                    places_result['results'][0]['place_id'],
                    fields=['website']
                )
                return place_details.get('result', {}).get('website', '')
        except Exception as e:
            print(f"Error en Google Maps: {str(e)}")
        return ""