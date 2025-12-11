"""
SSO Authentication Manager
Handles OAuth2 token acquisition and management
"""

import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
from pathlib import Path

class AuthManager:
    """Manages SSO authentication and token lifecycle"""
    
    def __init__(self, token_file='access_token.txt'):
        load_dotenv()
        
        # Environment variables
        self.hub_url = os.getenv('HUB_URL')
        self.username = os.getenv('USERNAME')
        self.password = os.getenv('PASSWORD')
        self.client_id = os.getenv('CLIENT_ID')
        
        # Token management
        self.token_file = Path(token_file)
        self.access_token = None
        self.token_expiry = None
        
        # Validate environment variables
        self._validate_config()
    
    def _validate_config(self):
        """Validates required environment variables"""
        required_vars = {
            'HUB_URL': self.hub_url,
            'USERNAME': self.username,
            'PASSWORD': self.password,
            'CLIENT_ID': self.client_id
        }
        
        missing = [key for key, value in required_vars.items() if not value]
        
        if missing:
            raise ValueError(
                f"❌ Missing required environment variables: {', '.join(missing)}\n"
                f"Please check your .env file"
            )
        
        print("✅ Environment variables validated")
    
    def _load_token_from_file(self):
        """Loads existing token from file if valid"""
        if not self.token_file.exists():
            return False
        
        try:
            with open(self.token_file, 'r') as f:
                data = json.load(f)
                
            # Check if token is expired
            expiry = datetime.fromisoformat(data['expiry'])
            if expiry > datetime.now():
                self.access_token = data['token']
                self.token_expiry = expiry
                print(f"✅ Loaded valid token from file (expires: {expiry})")
                return True
            else:
                print("⚠️  Token in file is expired")
                return False
                
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"⚠️  Could not load token from file: {e}")
            return False
    
    def _save_token_to_file(self):
        """Saves token to file with expiry information"""
        data = {
            'token': self.access_token,
            'expiry': self.token_expiry.isoformat(),
            'created_at': datetime.now().isoformat()
        }
        
        with open(self.token_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Token saved to {self.token_file}")
    
    def get_access_token(self, force_refresh=False):
        """
        Gets valid access token (from file or by requesting new one)
        
        Args:
            force_refresh: If True, always requests new token
            
        Returns:
            str: Valid access token
        """
        # Try to load from file if not forcing refresh
        if not force_refresh and self._load_token_from_file():
            return self.access_token
        
        # Request new token
        print("🔐 Requesting new access token from SSO server...")
        
        url = f"https://{self.hub_url}/oauth2/token"
        
        payload = {
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
            'redirect_uri': 'myapp://auth',
            'client_id': self.client_id,
            'scope': 'openid'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        try:
            response = requests.post(
                url, 
                data=payload, 
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract token information
            self.access_token = data['access_token']
            expires_in = data.get('expires_in', 3600)  # Default 1 hour
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # Save to file
            self._save_token_to_file()
            
            print(f"✅ Access token acquired successfully")
            print(f"   Expires in: {expires_in} seconds ({expires_in//60} minutes)")
            print(f"   Expiry time: {self.token_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
            
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to get access token: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Response: {e.response.text}")
            raise
    
    def is_token_valid(self):
        """Checks if current token is still valid"""
        if not self.access_token or not self.token_expiry:
            return False
        
        # Add 60 second buffer before expiry
        return datetime.now() < (self.token_expiry - timedelta(seconds=60))
    
    def refresh_if_needed(self):
        """Refreshes token if it's expired or about to expire"""
        if not self.is_token_valid():
            print("🔄 Token expired or missing, refreshing...")
            return self.get_access_token(force_refresh=True)
        return self.access_token


# Example usage
if __name__ == "__main__":
    print("=" * 60)
    print("SSO Authentication Manager - Test")
    print("=" * 60)
    
    try:
        # Initialize auth manager
        auth = AuthManager()
        
        # Get access token (will use cached if valid)
        token = auth.get_access_token()
        
        print("\n" + "=" * 60)
        print("Token acquired successfully!")
        print("=" * 60)
        print(f"Token (first 50 chars): {token[:50]}...")
        print(f"Token valid: {auth.is_token_valid()}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)