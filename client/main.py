import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable, Any
import json
import time
import logging
from functools import wraps
from client.config import load_config, Config


# Logging setup
def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Logging konfigürasyonu
    
    Args:
        log_level: Log seviyesi (DEBUG, INFO, WARNING, ERROR)
        log_file: Log dosyası (None ise sadece console)
    """
    # Root logger'ı temizle
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    handlers.append(console_handler)
    
    # File handler (opsiyonel)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(file_handler)
    
    # Root logger config
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )


# Logger oluştur
logger = logging.getLogger(__name__)


class APIError(Exception):
    """API çağrısı sırasında oluşan hatalar için özel exception"""
    pass


class JSONParseError(APIError):
    """JSON parse hatası"""
    pass


def safe_json_parse(response: requests.Response) -> Dict[str, Any]:
    """
    Güvenli JSON parsing
    
    Args:
        response: requests Response objesi
        
    Returns:
        Parsed JSON dict
        
    Raises:
        JSONParseError: JSON parse edilemezse
    """
    try:
        return response.json()
    except json.JSONDecodeError as e:
        # Response body'yi logla (ama çok uzunsa kes)
        body_preview = response.text[:500] if response.text else "<empty>"
        
        raise JSONParseError(
            f"Failed to parse JSON response from {response.url}\n"
            f"Status: {response.status_code}\n"
            f"Content-Type: {response.headers.get('Content-Type', 'unknown')}\n"
            f"Body preview: {body_preview}\n"
            f"Parse error: {str(e)}"
        )


def retry_on_failure(max_attempts=3, backoff_factor=2, retry_statuses=(500, 502, 503, 504, 429)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_attempts: Maksimum deneme sayısı
        backoff_factor: Her denemede bekleme süresini katla (1s, 2s, 4s...)
        retry_statuses: Hangi HTTP status kodlarında retry yapılsın (429 rate limit dahil)
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    
                    # Son denemeyse hata fırlat
                    if attempt == max_attempts:
                        break
                    
                    # Retry yapılabilir hata mı kontrol et
                    should_retry = False
                    wait_time = backoff_factor ** (attempt - 1)
                    
                    # HTTP status code kontrolü
                    if hasattr(e, 'response') and e.response is not None:
                        status_code = e.response.status_code
                        
                        if status_code in retry_statuses:
                            should_retry = True
                            
                            # 429 Rate Limit için özel bekleme
                            if status_code == 429:
                                # Retry-After header'ını kontrol et
                                retry_after = e.response.headers.get('Retry-After')
                                if retry_after:
                                    try:
                                        wait_time = int(retry_after)
                                        logger.warning(f"Rate limited (429). Server says retry after {wait_time}s")
                                    except ValueError:
                                        # Retry-After bir tarih olabilir, default kullan
                                        wait_time = backoff_factor ** attempt
                                        logger.warning(f"Rate limited (429). Using exponential backoff: {wait_time}s")
                                else:
                                    # Rate limit ama Retry-After yok, daha uzun bekle
                                    wait_time = backoff_factor ** attempt * 2
                                    logger.warning(f"Rate limited (429). No Retry-After header. Waiting {wait_time}s")
                    
                    # Connection error, timeout vb.
                    elif isinstance(e, (
                        requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout
                    )):
                        should_retry = True
                    
                    if should_retry:
                        logger.warning(f"Attempt {attempt}/{max_attempts} failed. Retrying in {wait_time}s... Error: {e}")
                        time.sleep(wait_time)
                    else:
                        # Retry yapılamaz hata (401, 404 vb.)
                        logger.error(f"Non-retryable error: {e}")
                        break
            
            # Tüm denemeler başarısız
            raise last_exception
        
        return wrapper
    return decorator

class SmartPulseClient:
    """SmartPulse API Client - Pipeline işlemleri"""
    
    # Timeout ayarları (saniye)
    CONNECT_TIMEOUT = 10  # Bağlantı kurma timeout
    READ_TIMEOUT = 30     # Yanıt okuma timeout
    
    def __init__(self, config: Config):
        self.config = config
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        self.token_expires_in: int = 3600  # Default 1 saat
        self.session = requests.Session()
        
        # Timeout tuple (connect, read)
        self.timeout = (self.CONNECT_TIMEOUT, self.READ_TIMEOUT)
    
    def _is_token_valid(self) -> bool:
        """Token hala geçerli mi kontrol et"""
        if not self.access_token or not self.token_expires_at:
            return False
        
        # 5 dakika güvenlik marjı bırak
        safety_margin = timedelta(minutes=5)
        return datetime.now() < (self.token_expires_at - safety_margin)
    
    def _ensure_valid_token(self) -> bool:
        """Token geçerli değilse yenile"""
        if self._is_token_valid():
            logger.debug("Token is still valid")
            return True
        
        logger.info("Token expired or missing, requesting new token...")
        return self.get_token()
    
    @retry_on_failure(max_attempts=3, backoff_factor=2)
    def get_token(self) -> bool:
        """
        Adım 1: SSO'dan token al
        """
        logger.info("Requesting access token...")
        
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
            response = self.session.post(url, data=data, timeout=self.timeout)
            response.raise_for_status()
            
            # Güvenli JSON parsing
            token_data = safe_json_parse(response)
            self.access_token = token_data["access_token"]
            
            # Token expiry hesapla
            self.token_expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=self.token_expires_in)
            
            expires_str = self.token_expires_at.strftime("%H:%M:%S")
            logger.info(f"Token received successfully (expires at {expires_str})")
            logger.debug(f"Token preview: {self.access_token[:20]}...")
            return True
            
        except JSONParseError as e:
            logger.error(f"Failed to parse token response: {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get token: {e}")
            return False
        except KeyError as e:
            logger.error(f"Missing field in token response: {e}")
            return False
    
    @retry_on_failure(max_attempts=3, backoff_factor=2)
    def login_to_portal(self) -> bool:
        """
        Adım 2: Portal'a login ol
        """
        # Token kontrolü ve gerekirse yenileme
        if not self._ensure_valid_token():
            logger.error("No valid access token available for login")
            return False
        
        logger.info("Logging in to portal...")
        
        url = f"{self.config.PORTAL_URL}/Login/Login"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "username": self.config.USERNAME
        }
        
        try:
            response = self.session.post(url, json=data, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            
            login_data = safe_json_parse(response)
            
            if login_data.get("success"):
                user_id = login_data.get('userId')
                logger.info(f"Login successful (User ID: {user_id})")
                return True
            else:
                logger.error(f"Login failed: {login_data.get('message')}")
                return False
        
        except JSONParseError as e:
            logger.error(f"Failed to parse login response: {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Login request failed: {e}")
            return False
    
    @retry_on_failure(max_attempts=3, backoff_factor=2)
    def send_consumption_forecast(self, forecast_data: Dict) -> bool:
        """
        Adım 3: Tüketim tahmini gönder
        """
        # Token kontrolü ve gerekirse yenileme
        if not self._ensure_valid_token():
            logger.error("No valid access token available for forecast submission")
            return False
        
        logger.info("Sending consumption forecast...")
        
        url = f"{self.config.PORTAL_URL}/api/consumption-forecast/save-consumption-forecasts-provider"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = self.session.post(url, json=forecast_data, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            
            result = safe_json_parse(response)
            
            if result.get("success"):
                saved_records = result.get('savedRecords', 0)
                logger.info(f"Forecast saved successfully ({saved_records} records)")
                return True
            else:
                logger.error(f"Failed to save forecast: {result.get('message')}")
                return False
        
        except JSONParseError as e:
            logger.error(f"Failed to parse forecast response: {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Forecast request failed: {e}")
            return False
    
    def execute_pipeline(self, forecast_data: Dict) -> bool:
        """
        Pipeline: Token al → Login → Veri gönder
        """
        logger.info("=" * 60)
        logger.info("Starting SmartPulse Pipeline")
        logger.info("=" * 60)
        
        # Adım 1: Token al
        if not self.get_token():
            logger.error("Pipeline failed at Step 1 (Token)")
            return False
        
        # Adım 2: Login
        if not self.login_to_portal():
            logger.error("Pipeline failed at Step 2 (Login)")
            return False
        
        # Adım 3: Veri gönder
        if not self.send_consumption_forecast(forecast_data):
            logger.error("Pipeline failed at Step 3 (Forecast)")
            return False
        
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)
        
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
    # Logs klasörünü oluştur
    import os
    os.makedirs("logs", exist_ok=True)
    
    # Logging setup
    setup_logging(
        log_level=logging.INFO,  # DEBUG için daha detaylı log
        log_file="logs/smartpulse.log"  # Dosyaya da yaz
    )
    
    logger.info("SmartPulse Integration Client starting...")
    
    # Config yükle
    config = load_config()
    
    # Client oluştur
    client = SmartPulseClient(config)
    
    # Mock veri oluştur
    forecast_date = "2024-11-26"
    mock_data = generate_mock_forecast_data(forecast_date)
    
    logger.info(f"Generated mock forecast for: {forecast_date}")
    logger.info(f"Total hours: {len(mock_data['forecastDataList'][0]['forecasts'])}")
    
    # Pipeline'ı çalıştır
    success = client.execute_pipeline(mock_data)
    
    if success:
        logger.info("All operations completed successfully!")
    else:
        logger.error("Pipeline execution failed. Check logs above.")