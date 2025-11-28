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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from client.config import load_config, Config


# ==================== KONFIGURASYON ====================
"""
OTOMATIK GÜN HESAPLAMA:
Bu script 2 gün için tahmin verisi gönderir:
1. Bugün (T+0): Script'in çalıştırıldığı gün
2. Yarın (T+1): Script'in çalıştırıldığı günün ertesi günü

Örnek:
- 28 Kasım 2025'te çalıştırılırsa → 28 Kasım ve 29 Kasım verilerini gönderir
- 29 Kasım 2025'te çalıştırılırsa → 29 Kasım ve 30 Kasım verilerini gönderir

Bu yaklaşım, karşı sunucunun günlük veri alma gereksinimine uygundur.
Manuel gün belirlemeye gerek yoktur, script otomatik olarak tarihleri hesaplar.
"""

# Database Configuration
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "met_db"
DB_USER = "mlmduser"
DB_PASSWORD = "mlmdpassword"

# Kaç gün ileri tahmin gönderilsin (varsayılan: bugün + yarın = 2 gün)
FORECAST_DAYS_AHEAD = 2

# =======================================================


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


def get_forecast_dates(days_ahead: int = 2) -> List[str]:
    """
    Bugünden başlayarak belirtilen sayıda gün için tarih listesi oluştur
    
    Args:
        days_ahead: Kaç gün ileri tahmin gönderilecek (varsayılan: 2)
        
    Returns:
        YYYY-MM-DD formatında tarih listesi
        
    Örnek:
        Bugün 28-11-2025 ise ve days_ahead=2 ise:
        ['2025-11-28', '2025-11-29']
    """
    today = datetime.now().date()
    dates = []
    
    for i in range(days_ahead):
        forecast_date = today + timedelta(days=i)
        dates.append(forecast_date.strftime("%Y-%m-%d"))
    
    return dates


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


class DatabaseManager:
    """Database işlemleri için yardımcı sınıf"""
    
    def __init__(self, db_url: str):
        """
        Args:
            db_url: SQLAlchemy database URL (postgresql://user:pass@host:port/dbname)
        """
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_session(self) -> Session:
        """Yeni bir database session oluştur"""
        return self.SessionLocal()
    
    def fetch_forecast_data(self, forecast_date: str, customer_name: str = None) -> List[Dict]:
        """
        Belirtilen tarih için tahmin verilerini çek
        
        Args:
            forecast_date: Tahmin tarihi (YYYY-MM-DD)
            customer_name: Müşteri adı (None ise tüm müşteriler)
            
        Returns:
            Liste halinde tahmin kayıtları
        """
        session = self.get_session()
        try:
            from sqlalchemy import text
            
            # SQL sorgusu
            query = """
                SELECT 
                    forecast_date,
                    hour,
                    customer_name,
                    forecast_type,
                    wattica_forecast
                FROM customer_forecast_comparisons
                WHERE forecast_date = :forecast_date
            """
            
            params = {'forecast_date': forecast_date}
            
            if customer_name:
                query += " AND customer_name = :customer_name"
                params['customer_name'] = customer_name
            
            query += " ORDER BY hour, customer_name"
            
            result = session.execute(text(query), params)
            rows = result.fetchall()
            
            # Dict'e çevir
            data = []
            for row in rows:
                data.append({
                    'forecast_date': row[0],
                    'hour': row[1],
                    'customer_name': row[2],
                    'forecast_type': row[3],
                    'wattica_forecast': row[4]
                })
            
            logger.info(f"Fetched {len(data)} records from database for date {forecast_date}")
            return data
            
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise
        finally:
            session.close()


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
    
    def execute_pipeline(self, forecast_data: Dict, forecast_date: str) -> bool:
        """
        Pipeline: Token al → Login → Veri gönder
        
        Args:
            forecast_data: API formatında tahmin verisi
            forecast_date: Tahmin tarihi (loglamak için)
        """
        logger.info("=" * 60)
        logger.info(f"Starting SmartPulse Pipeline for {forecast_date}")
        logger.info("=" * 60)
        
        # Adım 1: Token al (gerekirse, token cache var)
        if not self._ensure_valid_token():
            logger.error("Pipeline failed: No valid token")
            return False
        
        # Adım 2: Login
        if not self.login_to_portal():
            logger.error(f"Pipeline failed at Step 2 (Login) for {forecast_date}")
            return False
        
        # Adım 3: Veri gönder
        if not self.send_consumption_forecast(forecast_data):
            logger.error(f"Pipeline failed at Step 3 (Forecast) for {forecast_date}")
            return False
        
        logger.info("=" * 60)
        logger.info(f"Pipeline completed successfully for {forecast_date}!")
        logger.info("=" * 60)
        
        return True


def transform_db_data_to_api_format(db_records: List[Dict], forecast_date: str) -> Dict:
    """
    Database'den çekilen verileri SmartPulse API formatına dönüştür
    
    Args:
        db_records: Database'den çekilen kayıtlar
        forecast_date: Tahmin tarihi (YYYY-MM-DD)
        
    Returns:
        SmartPulse API formatında dict
    """
    if not db_records:
        logger.warning(f"No data found in database for date {forecast_date}")
        return None
    
    # 24 saatlik forecast listesi oluştur
    forecasts = []
    base_date = datetime.strptime(forecast_date, "%Y-%m-%d")
    
    # Saat bazında veri topla (0-23)
    hour_data = {}
    for record in db_records:
        hour = record['hour']
        if hour not in hour_data:
            hour_data[hour] = []
        hour_data[hour].append(record)
    
    # Her saat için API formatında entry oluştur
    for hour in range(24):
        start_time = base_date + timedelta(hours=hour)
        end_time = base_date + timedelta(hours=hour + 1)
        
        # O saat için değer (varsa)
        value = 0.0
        if hour in hour_data:
            # Wattica tahminlerinin ortalamasını al veya toplamını al (iş mantığına göre)
            values = [r['wattica_forecast'] for r in hour_data[hour] if r['wattica_forecast'] is not None]
            if values:
                value = sum(values)  # Toplam kullanıyoruz (müşteri bazlı ise)
                # value = sum(values) / len(values)  # Ortalama için bu satırı kullan
        
        forecasts.append({
            "isUpdated": False,
            "deliveryStart": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "deliveryEnd": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "deliveryStartOffset": 180,
            "deliveryEndOffset": 180,
            "order": hour + 1,  # 1-24 arası
            "value": round(value, 2)
        })
    
    # Günlük toplam hesapla (tüm saatlerin değerlerinin toplamı)
    daily_total = sum(f["value"] for f in forecasts)
    
    # API formatında döndür
    api_data = {
        "groupId": 12,  # Config'den alınabilir
        "userId": 2952,  # Config'den alınabilir
        "period": 1,
        "interval": 1,
        "forecastDataList": [
            {
                "unitType": 0,
                "unitNo": 1,
                "providerKey": "testDemo",  # Config'den alınabilir
                "total": round(daily_total, 2),  # Günlük toplam tahmin (24 saatin toplamı)
                "isUpdated": False,
                "forecastDay": forecast_date,
                "forecasts": forecasts
            }
        ]
    }
    
    logger.info(f"Transformed {len(db_records)} DB records into 24-hour forecast for {forecast_date}")
    return api_data


def process_forecast_for_date(
    forecast_date: str,
    db_manager: DatabaseManager,
    client: SmartPulseClient
) -> bool:
    """
    Belirli bir tarih için tahmin işlemini gerçekleştir
    
    Args:
        forecast_date: Tahmin tarihi (YYYY-MM-DD)
        db_manager: Database manager instance
        client: SmartPulse client instance
        
    Returns:
        Başarılı ise True, değilse False
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"Processing forecast for date: {forecast_date}")
    logger.info(f"{'='*70}\n")
    
    try:
        # 1. Verileri database'den çek
        logger.info(f"Step 1: Fetching data from database for {forecast_date}...")
        db_records = db_manager.fetch_forecast_data(forecast_date)
        
        if not db_records:
            logger.warning(f"No data found for {forecast_date}, skipping...")
            return False
        
        # 2. API formatına dönüştür
        logger.info(f"Step 2: Transforming data to API format...")
        api_data = transform_db_data_to_api_format(db_records, forecast_date)
        
        if not api_data:
            logger.error(f"Failed to transform data for {forecast_date}")
            return False
        
        # 3. Pipeline'ı çalıştır (API'ye gönder)
        logger.info(f"Step 3: Sending data to SmartPulse API...")
        success = client.execute_pipeline(api_data, forecast_date)
        
        if success:
            logger.info(f"✓ Successfully completed forecast submission for {forecast_date}")
        else:
            logger.error(f"✗ Failed to submit forecast for {forecast_date}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error processing forecast for {forecast_date}: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    # Logs klasörünü oluştur
    os.makedirs("logs", exist_ok=True)
    
    # Logging setup
    log_filename = f"logs/smartpulse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    setup_logging(
        log_level=logging.INFO,
        log_file=log_filename
    )
    
    logger.info("="*70)
    logger.info("SmartPulse Automatic Daily Forecast Submission")
    logger.info("="*70)
    
    # Tarihleri otomatik hesapla
    forecast_dates = get_forecast_dates(days_ahead=FORECAST_DAYS_AHEAD)
    
    logger.info(f"Script execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Forecast dates to process: {', '.join(forecast_dates)}")
    logger.info(f"Total days: {len(forecast_dates)}")
    
    # Database URL oluştur
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    try:
        # Database Manager oluştur
        logger.info("\nInitializing database connection...")
        db_manager = DatabaseManager(db_url)
        
        # Config yükle
        logger.info("Loading configuration...")
        config = load_config()
        
        # SmartPulse Client oluştur
        logger.info("Initializing SmartPulse client...")
        client = SmartPulseClient(config)
        
        # İlk token'ı al (tüm günler için aynı token kullanılacak)
        logger.info("Obtaining initial access token...")
        if not client.get_token():
            logger.error("Failed to obtain initial token. Exiting...")
            sys.exit(1)
        
        # Her gün için işlem yap
        results = {}
        for forecast_date in forecast_dates:
            success = process_forecast_for_date(forecast_date, db_manager, client)
            results[forecast_date] = success
            
            # Günler arası kısa bekleme (rate limit için)
            if forecast_date != forecast_dates[-1]:  # Son gün değilse
                logger.info("\nWaiting 2 seconds before next date...")
                time.sleep(2)
        
        # Özet rapor
        logger.info("\n" + "="*70)
        logger.info("EXECUTION SUMMARY")
        logger.info("="*70)
        
        successful = sum(1 for v in results.values() if v)
        failed = len(results) - successful
        
        logger.info(f"Total dates processed: {len(results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        
        logger.info("\nDetailed results:")
        for date, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED"
            logger.info(f"  {date}: {status}")
        
        logger.info("="*70)
        
        # Exit code
        if failed > 0:
            logger.warning(f"Completed with {failed} failure(s)")
            sys.exit(1)
        else:
            logger.info("All operations completed successfully!")
            sys.exit(0)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)