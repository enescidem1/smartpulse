"""
Forecast Sender
Fetches forecast data from PostgreSQL and sends to SmartPulse API
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
import json
from auth_manager import AuthManager
from portal_manager import PortalManager


class ForecastSender:
    """Manages fetching and sending consumption forecasts"""
    
    def __init__(self, auth_manager=None, portal_manager=None):
        load_dotenv()
        
        # Managers
        self.auth = auth_manager or AuthManager()
        self.portal = portal_manager or PortalManager(self.auth)
        
        # Database connection
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        # API endpoint
        self.api_url = os.getenv('FORECAST_API_URL')
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validates database and API configuration"""
        required = ['host', 'database', 'user', 'password']
        missing = [k for k in required if not self.db_config.get(k)]
        
        if missing:
            raise ValueError(
                f"❌ Missing database config: {', '.join(missing)}\n"
                f"Please check your .env file"
            )
        
        if not self.api_url:
            raise ValueError("❌ FORECAST_API_URL not found in .env file")
        
        print("✅ Database and API configuration validated")
    
    def connect_db(self):
        """Creates database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            print("✅ Connected to PostgreSQL database")
            return conn
        except psycopg2.Error as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def fetch_forecasts(self, customer_name=None, start_date=None, end_date=None, limit=None):
        """
        Fetches forecast data from database
        
        Args:
            customer_name: Filter by specific customer (optional)
            start_date: Start date for forecasts (optional)
            end_date: End date for forecasts (optional)
            limit: Limit number of results (optional)
            
        Returns:
            list: List of forecast records
        """
        conn = self.connect_db()
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Build query
                query = """
                    SELECT 
                        customer_name,
                        prediction_ts,
                        model_pred,
                        customer_pred,
                        created_at
                    FROM customer_forecast_comparisons
                    WHERE 1=1
                """
                params = []
                
                if customer_name:
                    query += " AND customer_name = %s"
                    params.append(customer_name)
                
                if start_date:
                    query += " AND prediction_ts >= %s"
                    params.append(start_date)
                
                if end_date:
                    query += " AND prediction_ts <= %s"
                    params.append(end_date)
                
                query += " ORDER BY customer_name, prediction_ts"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Execute query
                cur.execute(query, params)
                records = cur.fetchall()
                
                print(f"📊 Fetched {len(records)} forecast records from database")
                
                return records
                
        finally:
            conn.close()
    
    def group_forecasts_by_customer_day(self, records):
        """
        Groups forecast records by customer and day
        
        Args:
            records: List of forecast records from database
            
        Returns:
            dict: Grouped forecasts {customer_name: {date: [hourly_records]}}
        """
        grouped = {}
        
        for record in records:
            customer = record['customer_name']
            date = record['prediction_ts'].date()
            
            if customer not in grouped:
                grouped[customer] = {}
            
            if date not in grouped[customer]:
                grouped[customer][date] = []
            
            grouped[customer][date].append(record)
        
        print(f"📦 Grouped into {len(grouped)} customers")
        for customer, dates in grouped.items():
            print(f"   {customer}: {len(dates)} days")
        
        return grouped
    
    def build_api_payload(self, customer_name, date, hourly_records):
        """
        Builds API payload from database records
        
        Args:
            customer_name: Customer name
            date: Forecast date
            hourly_records: List of hourly forecast records
            
        Returns:
            dict: API payload, or None if facility not found
        """
        # Get facility ID from mapping
        facility_id = self.portal.get_facility_id(customer_name)
        
        if not facility_id:
            print(f"⚠️  Skipping {customer_name} - facility not found in mapping")
            return None
        
        # Sort by hour
        hourly_records.sort(key=lambda x: x['prediction_ts'])
        
        # Build hourly forecasts
        forecasts = []
        for idx, record in enumerate(hourly_records, start=1):
            ts = record['prediction_ts']
            
            forecast_entry = {
                "isUpdated": False,
                "deliveryStart": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "deliveryEnd": (ts + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                "deliveryStartOffset": 180,  # UTC+3 for Turkey
                "deliveryEndOffset": 180,
                "order": idx,
                "value": round(record['model_pred'], 2)  # Round to 2 decimals
            }
            forecasts.append(forecast_entry)
        
        # Build full payload
        payload = {
            "groupId": self.portal.group_id,
            "userId": self.portal.user_id,
            "period": 1,  # Daily
            "interval": 1,  # Hourly
            "forecastDataList": [
                {
                    "unitType": 0,
                    "unitNo": facility_id,
                    "providerKey": "testDemo",  # Could be made configurable
                    "total": 0,
                    "isUpdated": False,
                    "forecastDay": date.strftime("%Y-%m-%d"),
                    "forecasts": forecasts
                }
            ]
        }
        
        return payload
    
    def preview_payload(self, payload, show_all_hours=False):
        """
        Displays payload preview before sending
        
        Args:
            payload: API payload dictionary
            show_all_hours: If True, shows all hourly entries
        """
        forecast_data = payload['forecastDataList'][0]
        forecasts = forecast_data['forecasts']
        
        print("\n" + "=" * 80)
        print("📋 PAYLOAD PREVIEW")
        print("=" * 80)
        print(f"Group ID:      {payload['groupId']}")
        print(f"User ID:       {payload['userId']}")
        print(f"Facility ID:   {forecast_data['unitNo']}")
        print(f"Forecast Day:  {forecast_data['forecastDay']}")
        print(f"Total Hours:   {len(forecasts)}")
        print("-" * 80)
        
        # Show sample hours
        if show_all_hours:
            hours_to_show = forecasts
        else:
            hours_to_show = forecasts[:3] + ['...'] + forecasts[-3:]
        
        print("Hourly Forecasts:")
        for entry in hours_to_show:
            if entry == '...':
                print("   ...")
                continue
            print(f"   Hour {entry['order']:2d}: {entry['deliveryStart']} → "
                  f"{entry['deliveryEnd']} | Value: {entry['value']:7.2f} MWh")
        
        print("=" * 80)
    
    def send_forecast(self, payload, preview=True):
        """
        Sends forecast data to API
        
        Args:
            payload: API payload dictionary
            preview: If True, shows preview before sending
            
        Returns:
            dict: API response
        """
        if preview:
            self.preview_payload(payload)
        
        # Ensure valid token
        token = self.auth.refresh_if_needed()
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            print(f"\n🚀 Sending forecast to API...")
            
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            print(f"✅ Forecast sent successfully!")
            print(f"   Status: {response.status_code}")
            
            # Try to parse response
            try:
                return response.json()
            except:
                return {"status": "success", "raw_response": response.text}
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to send forecast: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Response: {e.response.text}")
            raise
    
    def process_and_send(self, customer_name=None, start_date=None, end_date=None, 
                        preview=True, dry_run=False):
        """
        Complete workflow: fetch from DB, build payload, and send
        
        Args:
            customer_name: Filter by customer (optional)
            start_date: Start date filter (optional)
            end_date: End date filter (optional)
            preview: Show payload preview before sending
            dry_run: If True, only preview without sending
            
        Returns:
            dict: Summary of sent forecasts
        """
        print("\n" + "=" * 80)
        print("🚀 FORECAST SENDING WORKFLOW")
        print("=" * 80)
        
        # Step 1: Ensure portal is logged in
        if not self.portal.facilities_map:
            print("\n📍 Step 1: Logging into portal...")
            self.portal.login()
        
        # Step 2: Fetch forecasts from database
        print("\n📍 Step 2: Fetching forecasts from database...")
        records = self.fetch_forecasts(customer_name, start_date, end_date)
        
        if not records:
            print("⚠️  No forecast records found")
            return {"sent": 0, "failed": 0, "skipped": 0}
        
        # Step 3: Group by customer and day
        print("\n📍 Step 3: Grouping forecasts...")
        grouped = self.group_forecasts_by_customer_day(records)
        
        # Step 4: Build and send payloads
        print("\n📍 Step 4: Building and sending payloads...")
        
        results = {"sent": 0, "failed": 0, "skipped": 0}
        
        for customer, dates in grouped.items():
            for date, hourly_records in dates.items():
                print(f"\n{'─' * 80}")
                print(f"Processing: {customer} - {date}")
                print(f"{'─' * 80}")
                
                # Build payload
                payload = self.build_api_payload(customer, date, hourly_records)
                
                if not payload:
                    results["skipped"] += 1
                    continue
                
                # Send (or skip if dry run)
                if dry_run:
                    self.preview_payload(payload)
                    print("\n🔍 DRY RUN - Not sending to API")
                    results["skipped"] += 1
                else:
                    try:
                        response = self.send_forecast(payload, preview=preview)
                        results["sent"] += 1
                    except Exception as e:
                        print(f"❌ Failed: {e}")
                        results["failed"] += 1
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 SENDING SUMMARY")
        print("=" * 80)
        print(f"✅ Successfully sent: {results['sent']}")
        print(f"❌ Failed:           {results['failed']}")
        print(f"⏭️  Skipped:          {results['skipped']}")
        print("=" * 80)
        
        return results


# Example usage
if __name__ == "__main__":
    print("=" * 80)
    print("Forecast Sender - Test")
    print("=" * 80)
    
    try:
        # Initialize (reuses same auth and portal instances)
        sender = ForecastSender()
        
        # Example 1: Process specific customer for today
        print("\n" + "=" * 80)
        print("TEST 1: Dry run for specific customer")
        print("=" * 80)
        
        results = sender.process_and_send(
            customer_name="Ankara Oyak Çimento",
            start_date=datetime(2025, 12, 3),
            end_date=datetime(2025, 12, 3 ,23),
            preview=True,
            dry_run=True  # Set to False to actually send
        )
        
        print("\n✅ Test completed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)