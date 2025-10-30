import os
from dataclasses import dataclass

@dataclass
class Config:
    """
    SmartPulse API Configuration
    
    Gerçek credentials geldiğinde sadece bu dosyayı güncelleyeceğiz
    """
    
    # SSO Server URL
    HUB_URL: str = os.getenv("HUB_URL", "http://localhost:8001")
    
    # SmartPulse Portal URL
    PORTAL_URL: str = os.getenv("PORTAL_URL", "http://localhost:8001")
    
    # Credentials
    USERNAME: str = os.getenv("test_user", "test_user")
    PASSWORD: str = os.getenv("PASSWORD", "test_password")
    CLIENT_ID: str = os.getenv("CLIENT_ID", "test_client")
    
    def __post_init__(self):
        """Validasyon"""
        if not all([self.HUB_URL, self.PORTAL_URL, self.USERNAME, self.PASSWORD, self.CLIENT_ID]):
            raise ValueError("All configuration values must be provided")
    
    def to_dict(self):
        """Config'i dict olarak döndür (debug için)"""
        return {
            "HUB_URL": self.HUB_URL,
            "PORTAL_URL": self.PORTAL_URL,
            "USERNAME": self.USERNAME,
            "PASSWORD": "***",  # Güvenlik için gizle
            "CLIENT_ID": self.CLIENT_ID
        }


# Gerçek ortam için (credentials geldiğinde)
class ProductionConfig(Config):
    """
    Production ortamı için config
    
    Kullanım:
    from config import ProductionConfig
    config = ProductionConfig()
    """
    HUB_URL: str = "https://sso.example.com"
    PORTAL_URL: str = "https://portal.staging.smartpulse.io"
    USERNAME: str = "real_username"
    PASSWORD: str = "real_password"
    CLIENT_ID: str = "real_client_id"