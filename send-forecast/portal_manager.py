"""
Portal Manager
Handles portal login and facility mapping between DB names and API IDs
"""

import os
import requests
from dotenv import load_dotenv
from auth_manager import AuthManager
import json
from datetime import datetime


class PortalManager:
    """Manages portal operations and facility mapping"""
    
    def __init__(self, auth_manager=None):
        load_dotenv()
        
        # Use provided auth manager or create new one
        self.auth = auth_manager or AuthManager()
        
        # Environment variables
        self.portal_url = os.getenv('PORTAL_URL')
        self.username = os.getenv('USERNAME')
        
        # Portal data
        self.facilities_map = {}
        self.user_id = None
        self.group_id = None
        
        # Validate
        if not self.portal_url:
            raise ValueError("❌ PORTAL_URL not found in .env file")
    
    def login(self):
        """
        Logs into portal and retrieves facility mapping
        
        Returns:
            dict: Login response with user and facility information
        """
        # Ensure we have valid token
        token = self.auth.refresh_if_needed()
        
        print(f"\n🔐 Logging into portal: {self.portal_url}")
        
        url = f"https://{self.portal_url}/Login/Login"
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "username": self.username
        }
        
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract user information
            self.user_id = data.get('Id')
            
            # Extract group information
            permissions = data.get('Permissions', {})
            groups = permissions.get('groups', [])
            if groups:
                self.group_id = groups[0]['id']
            
            # Build facility mapping
            self._build_facility_map(data)
            
            print(f"✅ Portal login successful")
            print(f"   User ID: {self.user_id}")
            print(f"   Group ID: {self.group_id}")
            print(f"   Total facilities mapped: {len(self.facilities_map)}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Portal login failed: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Response: {e.response.text}")
            raise
    
    def _build_facility_map(self, login_response):
        """
        Builds mapping between customer names and facility IDs
        
        Args:
            login_response: JSON response from login endpoint
        """
        permissions = login_response.get('Permissions', {})
        facilities = permissions.get('facilities', [])
        
        for facility in facilities:
            facility_id = facility.get('id')
            facility_name = facility.get('name', '').strip()
            
            if facility_id and facility_name:
                # Normalize name for matching
                normalized_name = self._normalize_name(facility_name)
                self.facilities_map[normalized_name] = {
                    'id': facility_id,
                    'original_name': facility_name,
                    'company_id': facility.get('companyId')
                }
        
        print(f"\n📋 Facility mapping built:")
        for norm_name, info in sorted(self.facilities_map.items())[:5]:
            print(f"   '{info['original_name']}' → ID: {info['id']}")
        if len(self.facilities_map) > 5:
            print(f"   ... and {len(self.facilities_map) - 5} more")
    
    @staticmethod
    def _normalize_name(name):
        """
        Normalizes facility name for matching
        Handles Turkish characters and case differences
        
        Args:
            name: Facility name to normalize
            
        Returns:
            str: Normalized name
        """
        # Convert to uppercase for case-insensitive matching
        name = name.upper()
        
        # Remove common suffixes and extra spaces
        name = name.replace('  ', ' ').strip()
        
        # Turkish character normalization
        replacements = {
            'İ': 'I',
            'Ğ': 'G',
            'Ü': 'U',
            'Ş': 'S',
            'Ö': 'O',
            'Ç': 'C'
        }
        
        for tr_char, en_char in replacements.items():
            name = name.replace(tr_char, en_char)
        
        return name
    
    def get_facility_id(self, customer_name):
        """
        Gets facility ID for a customer name
        
        Args:
            customer_name: Customer name from database
            
        Returns:
            int: Facility ID, or None if not found
        """
        normalized = self._normalize_name(customer_name)
        facility_info = self.facilities_map.get(normalized)
        
        if facility_info:
            return facility_info['id']
        
        # Try partial matching if exact match fails
        for mapped_name, info in self.facilities_map.items():
            if normalized in mapped_name or mapped_name in normalized:
                print(f"⚠️  Partial match found: '{customer_name}' → '{info['original_name']}'")
                return info['id']
        
        print(f"❌ No facility found for: '{customer_name}'")
        return None
    
    def get_all_facilities(self):
        """Returns all mapped facilities"""
        return self.facilities_map
    
    def save_mapping_to_file(self, filename='facility_mapping.json'):
        """Saves facility mapping to JSON file for reference"""
        mapping_data = {
            'generated_at': datetime.now().isoformat(),
            'user_id': self.user_id,
            'group_id': self.group_id,
            'total_facilities': len(self.facilities_map),
            'facilities': self.facilities_map
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Facility mapping saved to {filename}")


# Example usage and testing
if __name__ == "__main__":
    print("=" * 60)
    print("Portal Manager - Test")
    print("=" * 60)
    
    try:
        # Initialize with shared auth manager
        auth = AuthManager()
        portal = PortalManager(auth_manager=auth)
        
        # Login to portal
        login_response = portal.login()
        
        # Save mapping for reference
        portal.save_mapping_to_file()
        
        # Test some lookups
        print("\n" + "=" * 60)
        print("Testing Facility Lookups")
        print("=" * 60)
        
        test_names = [
            "Ankara Oyak Çimento",
            "BURSA AKÇANSA ÇİMENTO SAN. VE TİC. A.Ş.",
            "Met Tüketim"
        ]
        
        for name in test_names:
            facility_id = portal.get_facility_id(name)
            if facility_id:
                print(f"✅ '{name}' → Facility ID: {facility_id}")
            else:
                print(f"❌ '{name}' → Not found")
        
        print("\n" + "=" * 60)
        print("Test completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)