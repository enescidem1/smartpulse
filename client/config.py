import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

# python-dotenv varsa yükle (opsiyonel)
try:
    from dotenv import load_dotenv
    # .env dosyasını yükle
    env_path = Path(__file__).parent / '../.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ .env dosyası yüklendi: {env_path}")
except ImportError:
    print("⚠️  python-dotenv yüklü değil. Environment variables sistem üzerinden okunacak.")

@dataclass
class Config:
    """
    SmartPulse API Configuration
    
    Credentials environment variables'dan okunur:
    - HUB_URL
    - PORTAL_URL  
    - SP_USERNAME
    - SP_PASSWORD
    - SP_CLIENT_ID
    """
    
    # SSO Server URL
    HUB_URL: str = None
    
    # SmartPulse Portal URL
    PORTAL_URL: str = None
    
    # Credentials
    USERNAME: str = None
    PASSWORD: str = None
    CLIENT_ID: str = None
    
    def __init__(
        self,
        hub_url: Optional[str] = None,
        portal_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None
    ):
        """
        Config initialization
        Öncelik sırası: 
        1. Constructor parametreleri
        2. Environment variables
        3. Default değerler (sadece development için)
        """
        # Environment'tan oku
        self.HUB_URL = hub_url or os.getenv("HUB_URL")
        self.PORTAL_URL = portal_url or os.getenv("PORTAL_URL")
        self.USERNAME = username or os.getenv("SP_USERNAME")
        self.PASSWORD = password or os.getenv("SP_PASSWORD")
        self.CLIENT_ID = client_id or os.getenv("SP_CLIENT_ID")
        
        # Validasyon
        self._validate()
    
    def _validate(self):
        """Config değerlerini kontrol et"""
        missing = []
        
        if not self.HUB_URL:
            missing.append("HUB_URL")
        if not self.PORTAL_URL:
            missing.append("PORTAL_URL")
        if not self.USERNAME:
            missing.append("USERNAME (SP_USERNAME)")
        if not self.PASSWORD:
            missing.append("PASSWORD (SP_PASSWORD)")
        if not self.CLIENT_ID:
            missing.append("CLIENT_ID (SP_CLIENT_ID)")
        
        if missing:
            raise ValueError(
                f"❌ Eksik konfigürasyon değerleri: {', '.join(missing)}\n"
                f"Lütfen .env dosyasını kontrol edin veya environment variables tanımlayın."
            )
    
    def is_production(self) -> bool:
        """Production ortamında mı çalışıyoruz?"""
        return "localhost" not in self.HUB_URL.lower()
    
    def to_dict(self, hide_sensitive=True):
        """Config'i dict olarak döndür (debug için)"""
        return {
            "HUB_URL": self.HUB_URL,
            "PORTAL_URL": self.PORTAL_URL,
            "USERNAME": self.USERNAME,
            "PASSWORD": "***" if hide_sensitive else self.PASSWORD,
            "CLIENT_ID": self.CLIENT_ID[:8] + "***" if hide_sensitive else self.CLIENT_ID,
            "IS_PRODUCTION": self.is_production()
        }


def load_config() -> Config:
    """
    Config yükle (main.py'de kullanmak için)
    """
    try:
        config = Config()
        print(f"✅ Config yüklendi ({('PRODUCTION' if config.is_production() else 'DEVELOPMENT')} mode)")
        return config
    except ValueError as e:
        print(f"\n{e}")
        print("\n💡 Çözüm:")
        print("1. .env dosyası oluşturun:")
        print("   client/.env")
        print("\n2. Aşağıdaki değerleri doldurun:")
        print("   HUB_URL=https://sso.example.com")
        print("   PORTAL_URL=https://portal.staging.smartpulse.io")
        print("   SP_USERNAME=your_username")
        print("   SP_PASSWORD=your_password")
        print("   SP_CLIENT_ID=your_client_id")
        raise