"""
Unit tests for SmartPulse Client

Çalıştırmak için:
    cd client
    pytest test_client.py -v
    
Coverage için:
    pytest test_client.py --cov=main --cov-report=html
"""
#pytestin üst dizinleri görebilmesi için
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import requests
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json

# Import from main
import sys
sys.path.insert(0, '.')
from client.main import (
    SmartPulseClient, 
    safe_json_parse, 
    JSONParseError,
    generate_mock_forecast_data,
    retry_on_failure
)
from client.config import Config


# ============== FIXTURES ==============
@pytest.fixture
def mock_config():
    """Mock Config objesi"""
    return Config(
        hub_url="http://test.example.com",
        portal_url="http://test.example.com",
        username="test_user",
        password="test_pass",
        client_id="test_client"
    )


@pytest.fixture
def client(mock_config):
    """SmartPulseClient instance"""
    return SmartPulseClient(mock_config)


@pytest.fixture
def mock_response():
    """Mock requests.Response objesi"""
    response = Mock(spec=requests.Response)
    response.status_code = 200
    response.headers = {'Content-Type': 'application/json'}
    return response


# ============== safe_json_parse TESTS ==============

def test_safe_json_parse_success(mock_response):
    """Başarılı JSON parse"""
    mock_response.json.return_value = {"status": "ok"}
    result = safe_json_parse(mock_response)
    assert result == {"status": "ok"}


def test_safe_json_parse_invalid_json(mock_response):
    """Bozuk JSON parse hatası"""
    mock_response.json.side_effect = json.JSONDecodeError("test", "doc", 0)
    mock_response.text = "<html>Error</html>"
    mock_response.url = "http://test.com"
    
    with pytest.raises(JSONParseError) as exc_info:
        safe_json_parse(mock_response)
    
    assert "Failed to parse JSON" in str(exc_info.value)
    assert "http://test.com" in str(exc_info.value)


# ============== SmartPulseClient TESTS ==============

def test_client_initialization(mock_config):
    """Client başlatma testi"""
    client = SmartPulseClient(mock_config)
    
    assert client.config == mock_config
    assert client.access_token is None
    assert client.token_expires_at is None
    assert client.timeout == (10, 30)


def test_is_token_valid_no_token(client):
    """Token yokken geçerlilik kontrolü"""
    assert client._is_token_valid() is False


def test_is_token_valid_expired(client):
    """Expire olmuş token kontrolü"""
    client.access_token = "test_token"
    client.token_expires_at = datetime.now() - timedelta(hours=1)
    
    assert client._is_token_valid() is False


def test_is_token_valid_active(client):
    """Geçerli token kontrolü"""
    client.access_token = "test_token"
    client.token_expires_at = datetime.now() + timedelta(hours=1)
    
    assert client._is_token_valid() is True


def test_is_token_valid_near_expiry(client):
    """5 dakikadan az kalan token (güvenlik marjı)"""
    client.access_token = "test_token"
    client.token_expires_at = datetime.now() + timedelta(minutes=3)
    
    # 5 dakika güvenlik marjı var, bu token geçersiz sayılmalı
    assert client._is_token_valid() is False


@patch('client.main.requests.Session.post')
def test_get_token_success(mock_post, client, mock_response):
    """Başarılı token alma"""
    mock_response.json.return_value = {
        "access_token": "test_token_123",
        "expires_in": 3600
    }
    mock_post.return_value = mock_response
    
    result = client.get_token()
    
    assert result is True
    assert client.access_token == "test_token_123"
    assert client.token_expires_in == 3600
    assert client.token_expires_at is not None
    
    # POST çağrısı yapıldı mı?
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert 'data' in call_args.kwargs
    assert call_args.kwargs['data']['username'] == 'test_user'


@patch('client.main.requests.Session.post')
def test_get_token_http_error(mock_post, client):
    """HTTP hatası durumunda token alma"""
    mock_post.side_effect = requests.exceptions.HTTPError("401 Unauthorized")
    
    result = client.get_token()
    
    assert result is False
    assert client.access_token is None


@patch('client.main.requests.Session.post')
def test_get_token_invalid_json(mock_post, client, mock_response):
    """Bozuk JSON yanıtı"""
    mock_response.json.side_effect = json.JSONDecodeError("test", "doc", 0)
    mock_response.text = "Invalid JSON"
    mock_response.url = "http://test.com"
    mock_post.return_value = mock_response
    
    result = client.get_token()
    
    assert result is False


@patch('client.main.requests.Session.post')
def test_login_success(mock_post, client, mock_response):
    """Başarılı login"""
    # Token set et
    client.access_token = "test_token"
    client.token_expires_at = datetime.now() + timedelta(hours=1)
    
    mock_response.json.return_value = {
        "success": True,
        "userId": 2952
    }
    mock_post.return_value = mock_response
    
    result = client.login_to_portal()
    
    assert result is True
    
    # Authorization header kontrolü
    call_args = mock_post.call_args
    assert 'headers' in call_args.kwargs
    assert call_args.kwargs['headers']['Authorization'] == 'Bearer test_token'


def test_login_with_invalid_token_fetch(client):
    """Token alma başarısız olduğunda login de başarısız olmalı"""
    # get_token'ı mock'la, False dönsün
    with patch.object(client, 'get_token', return_value=False):
        result = client.login_to_portal()
    
    assert result is False


@patch('client.main.requests.Session.post')
def test_send_forecast_success(mock_post, client, mock_response):
    """Başarılı forecast gönderimi"""
    # Token set et
    client.access_token = "test_token"
    client.token_expires_at = datetime.now() + timedelta(hours=1)
    
    mock_response.json.return_value = {
        "success": True,
        "savedRecords": 24
    }
    mock_post.return_value = mock_response
    
    forecast_data = {"test": "data"}
    result = client.send_consumption_forecast(forecast_data)
    
    assert result is True
    
    # JSON body kontrolü
    call_args = mock_post.call_args
    assert 'json' in call_args.kwargs
    assert call_args.kwargs['json'] == forecast_data


# ============== generate_mock_forecast_data TESTS ==============

def test_generate_mock_forecast_data():
    """Mock veri üretimi testi"""
    forecast_date = "2024-11-26"
    data = generate_mock_forecast_data(forecast_date)
    
    assert "groupId" in data
    assert "userId" in data
    assert "forecastDataList" in data
    
    forecast_list = data["forecastDataList"][0]
    assert forecast_list["forecastDay"] == forecast_date
    
    forecasts = forecast_list["forecasts"]
    assert len(forecasts) == 24  # 24 saat
    
    # İlk saat kontrolü
    first_hour = forecasts[0]
    assert first_hour["order"] == 1
    assert "deliveryStart" in first_hour
    assert "deliveryEnd" in first_hour
    assert "value" in first_hour
    
    # Son saat kontrolü
    last_hour = forecasts[23]
    assert last_hour["order"] == 24


def test_generate_mock_forecast_data_default_date():
    """Default tarih ile veri üretimi"""
    data = generate_mock_forecast_data()
    
    forecast_date = data["forecastDataList"][0]["forecastDay"]
    # Bugünün tarihi olmalı
    assert forecast_date == datetime.now().strftime("%Y-%m-%d")


# ============== RETRY DECORATOR TESTS ==============

@patch('client.main.time.sleep')  # sleep'i mock'la, testler hızlı olsun
def test_retry_success_first_attempt(mock_sleep):
    """İlk denemede başarılı"""
    mock_func = Mock(return_value="success")
    decorated = retry_on_failure(max_attempts=3)(mock_func)
    
    result = decorated()
    
    assert result == "success"
    assert mock_func.call_count == 1
    mock_sleep.assert_not_called()


@patch('client.main.time.sleep')
def test_retry_success_second_attempt(mock_sleep):
    """İkinci denemede başarılı"""
    mock_func = Mock(side_effect=[
        requests.exceptions.ConnectionError("Connection failed"),
        "success"
    ])
    decorated = retry_on_failure(max_attempts=3)(mock_func)
    
    result = decorated()
    
    assert result == "success"
    assert mock_func.call_count == 2
    mock_sleep.assert_called_once()


@patch('client.main.time.sleep')
def test_retry_all_attempts_fail(mock_sleep):
    """Tüm denemeler başarısız"""
    mock_func = Mock(side_effect=requests.exceptions.ConnectionError("Failed"))
    decorated = retry_on_failure(max_attempts=3)(mock_func)
    
    with pytest.raises(requests.exceptions.ConnectionError):
        decorated()
    
    assert mock_func.call_count == 3
    assert mock_sleep.call_count == 2  # 3 deneme = 2 bekleme


@patch('client.main.time.sleep')
def test_retry_rate_limit_with_retry_after(mock_sleep):
    """429 rate limit + Retry-After header"""
    response = Mock()
    response.status_code = 429
    response.headers = {'Retry-After': '10'}
    
    error = requests.exceptions.HTTPError()
    error.response = response
    
    mock_func = Mock(side_effect=[error, "success"])
    decorated = retry_on_failure(max_attempts=3)(mock_func)
    
    result = decorated()
    
    assert result == "success"
    # Retry-After=10 saniye beklemeli
    mock_sleep.assert_called_once_with(10)


# ============== INTEGRATION TESTS ==============

@patch('client.main.requests.Session.post')
def test_full_pipeline_success(mock_post, client):
    """Tam pipeline testi (E2E)"""
    # 3 endpoint için yanıtları hazırla
    responses = [
        # 1. Token
        Mock(
            status_code=200,
            json=lambda: {"access_token": "token123", "expires_in": 3600}
        ),
        # 2. Login
        Mock(
            status_code=200,
            json=lambda: {"success": True, "userId": 2952}
        ),
        # 3. Forecast
        Mock(
            status_code=200,
            json=lambda: {"success": True, "savedRecords": 24}
        )
    ]
    
    mock_post.side_effect = responses
    
    forecast_data = generate_mock_forecast_data()
    result = client.execute_pipeline(forecast_data)
    
    assert result is True
    assert mock_post.call_count == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])