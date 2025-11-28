from fastapi import FastAPI, HTTPException, Depends, Header, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import uvicorn
import secrets
import threading
import time

app = FastAPI(title="Mock SmartPulse Server", version="1.0.0")
security = HTTPBearer()

# Mock database - gerçek uygulamada veritabanı kullanılır
mock_tokens = {}
mock_users = {
    "test_user": "test_password"
}

# Token cleanup için lock
token_lock = threading.Lock()


def cleanup_expired_tokens():
    """
    Expired tokenları temizle
    Background thread olarak çalışır
    """
    while True:
        time.sleep(300)  # 5 dakikada bir temizle
        
        with token_lock:
            now = datetime.now()
            expired_tokens = []
            
            for token, data in mock_tokens.items():
                expires_at = datetime.fromisoformat(data["expires_at"])
                if now > expires_at:
                    expired_tokens.append(token)
            
            for token in expired_tokens:
                del mock_tokens[token]
            
            if expired_tokens:
                print(f"🧹 Cleaned up {len(expired_tokens)} expired tokens")
                print(f"📊 Active tokens: {len(mock_tokens)}")


# Cleanup thread'i başlat
cleanup_thread = threading.Thread(target=cleanup_expired_tokens, daemon=True)
cleanup_thread.start()

# ============== REQUEST MODELS ==============

class LoginRequest(BaseModel):
    username: str

class ForecastHour(BaseModel):
    isUpdated: bool = False
    deliveryStart: str
    deliveryEnd: str
    deliveryStartOffset: int = 180
    deliveryEndOffset: int = 180
    order: int
    value: float

class ForecastData(BaseModel):
    unitType: int
    unitNo: int
    providerKey: str
    total: float = 0
    isUpdated: bool = False
    forecastDay: str
    forecasts: List[ForecastHour]

class ConsumptionForecastRequest(BaseModel):
    groupId: int
    userId: int
    period: int
    interval: int
    forecastDataList: List[ForecastData]

# ============== RESPONSE MODELS ==============

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    scope: str

class LoginResponse(BaseModel):
    success: bool
    message: str
    userId: Optional[int] = None

class ForecastResponse(BaseModel):
    success: bool
    message: str
    savedRecords: int = 0

# ============== HELPER FUNCTIONS ==============

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Token doğrulama fonksiyonu"""
    token = credentials.credentials
    if token not in mock_tokens:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return mock_tokens[token]

# ============== ENDPOINTS ==============

@app.get("/")
def root():
    """Health check endpoint"""
    return {
        "service": "Mock SmartPulse Server",
        "status": "running",
        "endpoints": {
            "token": "/oauth2/token",
            "login": "/Login/Login",
            "forecast": "/api/consumption-forecast/save-consumption-forecasts-provider"
        }
    }

@app.post("/oauth2/token", response_model=TokenResponse)
def get_token(
    grant_type: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    redirect_uri: str = Form(...),
    client_id: str = Form(...),
    scope: str = Form(...)
):
    """
    Adım 1: SSO Token alma endpoint'i
    OAuth 2.0 password grant flow
    Form data olarak alır (application/x-www-form-urlencoded)
    """
    # Debug: Ne geldi görelim
    print(f"🔍 Token request received:")
    print(f"   Username: '{username}'")
    print(f"   Password: '{password}'")
    print(f"   Expected users: {list(mock_users.keys())}")
    
    # Grant type kontrolü
    if grant_type != "password":
        raise HTTPException(status_code=400, detail="Unsupported grant_type")
    
    # Kullanıcı doğrulama
    if username not in mock_users or mock_users[username] != password:
        print(f"❌ Authentication failed!")
        print(f"   Username exists: {username in mock_users}")
        if username in mock_users:
            print(f"   Password match: {mock_users[username] == password}")
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    # Token oluştur
    access_token = f"mock_token_{secrets.token_urlsafe(32)}"
    mock_tokens[access_token] = {
        "username": username,
        "client_id": client_id,
        "created_at": datetime.now().isoformat()
    }
    
    print(f"✅ Token created successfully")
    
    return TokenResponse(
        access_token=access_token,
        scope=scope
    )

@app.post("/Login/Login", response_model=LoginResponse)
def login(request: LoginRequest, user_data: dict = Depends(verify_token)):
    """
    Adım 2: Portal login endpoint'i
    Token ile giriş yapma
    """
    if request.username != user_data["username"]:
        raise HTTPException(status_code=403, detail="Username mismatch with token")
    
    return LoginResponse(
        success=True,
        message="Login successful",
        userId=2952  # Mock user ID
    )

@app.post("/api/consumption-forecast/save-consumption-forecasts-provider", 
          response_model=ForecastResponse)
def save_consumption_forecast(
    request: ConsumptionForecastRequest,
    user_data: dict = Depends(verify_token)
):
    """
    Adım 3: Tüketim tahmini kaydetme endpoint'i
    Saatlik tahmin verilerini kaydet
    """
    # Veri validasyonu
    total_records = 0
    for forecast_data in request.forecastDataList:
        # 24 saat kontrolü
        if len(forecast_data.forecasts) != 24:
            raise HTTPException(
                status_code=400, 
                detail=f"Expected 24 hourly forecasts, got {len(forecast_data.forecasts)}"
            )
        
        # Order kontrolü (1-24 arası olmalı)
        orders = [f.order for f in forecast_data.forecasts]
        if sorted(orders) != list(range(1, 25)):
            raise HTTPException(
                status_code=400,
                detail="Forecast orders must be 1-24"
            )
        
        total_records += len(forecast_data.forecasts)
    
    # Başarılı yanıt
    print(f"✅ Received forecast data for {request.groupId}")
    print(f"   User ID: {request.userId}")
    print(f"   Date: {request.forecastDataList[0].forecastDay}")
    print(f"   Total hourly records: {total_records}")
    print(request)
    
    return ForecastResponse(
        success=True,
        message="Consumption forecasts saved successfully",
        savedRecords=total_records
    )

if __name__ == "__main__":
    print("🚀 Starting Mock SmartPulse Server...")
    print("📍 Server will run on: http://localhost:8001")
    print("📝 Default credentials: username='test_user', password='test_password'")
    uvicorn.run(app, host="0.0.0.0", port=8001)