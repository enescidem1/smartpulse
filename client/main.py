import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
from config import Config

class SmartPulseClient:
    """SmartPulse API Client - Pipeline işlemleri"""
    
    def __init__(self, config: Config):
        self.config = config
        self.access_token: Optional[str] = None
        self.session = requests.Session()
    
    def _log(self, step: str, message: str):
        """Log helper"""
        print(f"[{step}] {message}")
    
    def get_token(self) -> bool:
        """
        Adım 1: SSO'dan token al
        """
        self._log("STEP 1", "Requesting access token...")
        
        url = f"{self.config.HUB_URL}/oauth2/token"
        
        # Form data olarak gönder (application/x-www-form-urlencoded)
        data = {
            "grant_type": "password",
            "username": self.config.USERNAME,
            "password": self.config.PASSWORD,
            "redirect_uri": "myapp://auth",
            "client_id": self.config.CLIENT_ID,
            "scope": "openid"
        }
        
        try:
            # data parametresi form data olarak gönderir
            response = self.session.post(url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data["access_token"]
            
            self._log("STEP 1", f"✅ Token received: {self.access_token[:20]}...")
            return True
            
        except requests.exceptions.RequestException as e:
            self._log("STEP 1", f"❌ Failed to get token: {e}")
            return False
    
    def login_to_portal(self) -> bool:
        """
        Adım 2: Portal'a login ol
        """
        if not self.access_token:
            self._log("STEP 2", "❌ No access token available")
            return False
        
        self._log("STEP 2", "Logging in to portal...")
        
        url = f"{self.config.PORTAL_URL}/Login/Login"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "username": self.config.USERNAME
        }
        
        try:
            response = self.session.post(url, json=data, headers=headers)
            response.raise_for_status()
            
            login_data = response.json()
            
            if login_data.get("success"):
                self._log("STEP 2", f"✅ Login successful (User ID: {login_data.get('userId')})")
                return True
            else:
                self._log("STEP 2", f"❌ Login failed: {login_data.get('message')}")
                return False
                
        except requests.exceptions.RequestException as e:
            self._log("STEP 2", f"❌ Login request failed: {e}")
            return False
    
    def send_consumption_forecast(self, forecast_data: Dict) -> bool:
        """
        Adım 3: Tüketim tahmini gönder
        """
        if not self.access_token:
            self._log("STEP 3", "❌ No access token available")
            return False
        
        self._log("STEP 3", "Sending consumption forecast...")
        
        url = f"{self.config.PORTAL_URL}/api/consumption-forecast/save-consumption-forecasts-provider"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = self.session.post(url, json=forecast_data, headers=headers)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get("success"):
                self._log("STEP 3", f"✅ Forecast saved successfully ({result.get('savedRecords')} records)")
                return True
            else:
                self._log("STEP 3", f"❌ Failed to save forecast: {result.get('message')}")
                return False
                
        except requests.exceptions.RequestException as e:
            self._log("STEP 3", f"❌ Request failed: {e}")
            return False
    
    def execute_pipeline(self, forecast_data: Dict) -> bool:
        """
        Pipeline: Token al → Login → Veri gönder
        """
        print("\n" + "="*60)
        print("🚀 Starting SmartPulse Pipeline")
        print("="*60 + "\n")
        
        # Adım 1: Token al
        if not self.get_token():
            print("\n❌ Pipeline failed at Step 1")
            return False
        
        print()
        
        # Adım 2: Login
        if not self.login_to_portal():
            print("\n❌ Pipeline failed at Step 2")
            return False
        
        print()
        
        # Adım 3: Veri gönder
        if not self.send_consumption_forecast(forecast_data):
            print("\n❌ Pipeline failed at Step 3")
            return False
        
        print("\n" + "="*60)
        print("✅ Pipeline completed successfully!")
        print("="*60 + "\n")
        
        return True


def generate_mock_forecast_data(forecast_date: str = None) -> Dict:
    """
    Mock tahmin verisi oluştur (24 saatlik)
    """
    if forecast_date is None:
        forecast_date = datetime.now().strftime("%Y-%m-%d")
    
    # 24 saatlik tahmin verisi
    forecasts = []
    base_date = datetime.strptime(forecast_date, "%Y-%m-%d")
    
    for hour in range(24):
        start_time = base_date + timedelta(hours=hour)
        end_time = base_date + timedelta(hours=hour + 1)
        
        forecasts.append({
            "isUpdated": False,
            "deliveryStart": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "deliveryEnd": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "deliveryStartOffset": 180,
            "deliveryEndOffset": 180,
            "order": hour + 1,
            "value": round(50 + (hour * 2) + (hour % 3) * 5, 2)  # Mock değerler
        })
    
    return {
        "groupId": 12,
        "userId": 2952,
        "period": 1,
        "interval": 1,
        "forecastDataList": [
            {
                "unitType": 0,
                "unitNo": 1,
                "providerKey": "testDemo",
                "total": 0,
                "isUpdated": False,
                "forecastDay": forecast_date,
                "forecasts": forecasts
            }
        ]
    }


if __name__ == "__main__":
    # Config yükle
    config = Config()
    
    # Client oluştur
    client = SmartPulseClient(config)
    
    # Mock veri oluştur
    forecast_date = "2024-11-26"
    mock_data = generate_mock_forecast_data(forecast_date)
    
    print(f"📊 Generated mock forecast for: {forecast_date}")
    print(f"   Total hours: {len(mock_data['forecastDataList'][0]['forecasts'])}")
    
    # Pipeline'ı çalıştır
    success = client.execute_pipeline(mock_data)
    
    if success:
        print("🎉 All operations completed successfully!")
    else:
        print("⚠️  Pipeline execution failed. Check logs above.")