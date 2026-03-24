import requests

# Test data for Case 1: Simple rename
source_data = {
    "user_id": 12345,
    "user_name": "Alice",
    "contact_email": "alice@example.com",
    "phone": "555-0100",
    "registration_date": "2023-01-15T08:00:00Z"
}

target_schema = {
    "customer_uuid": "integer",
    "name": "string",
    "email": "string",
    "mobile": "string",
    "created_at": "string",
    "tier": "string"
}

def run_test():
    url = "http://localhost:8000/api/v1/transform"
    payload = {
        "source": source_data,
        "schema": target_schema
    }
    
    print("Testing transform endpoint...")
    try:
        response = requests.post(url, json=payload)
        print(f"Status: {response.status_code}")
        print(response.json())
    except Exception as e:
        print(f"Error connecting: {e}")

if __name__ == "__main__":
    run_test()
