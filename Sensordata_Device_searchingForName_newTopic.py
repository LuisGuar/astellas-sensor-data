import requests
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import time

class WalteroAPI:
    def __init__(self, base_url, mqtt_broker, mqtt_port=1888):
        self.base_url = base_url
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.access_token = None
        self.machine_id = None
        self.dashboard_id = None
        self.organization_id = None
        self.devices_info = []  # Each: {id, area, external}

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)

    def login(self, username, password):
        url = f"{self.base_url}/users/login"
        headers = {"Content-Type": "application/json"}
        payload = {"username": username, "password": password}
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get("AccessToken")
            self.machine_id = data.get("MachineId")
            if not self.access_token or not self.machine_id:
                print("❌ Error: Missing AccessToken or MachineId.")
                return None
            print("✅ Login Successful!")
            return data
        except requests.exceptions.RequestException as e:
            print(f"❌ Login Error: {e}")
            return None

    def get_dashboards(self):
        url = f"{self.base_url}/dashboards"
        headers = {"Authorization": self.access_token, "MachineId": self.machine_id}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            results = response.json().get("Results", [])
            if results:
                self.dashboard_id = results[0].get("id")
                print(f"📋 Stored dashboardId: {self.dashboard_id}")
            return results
        except requests.exceptions.RequestException as e:
            print(f"❌ Dashboard Error: {e}")
            return None

    def get_organizations(self, org_name=None):
        url = f"{self.base_url}/organizations"
        headers = {"Authorization": self.access_token, "MachineId": self.machine_id}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            for org in response.json().get("Results", []):
                if org_name and org.get("name").strip() == org_name.strip():
                    self.organization_id = org.get("id")
                    print(f"🏢 Stored organizationId for '{org_name}': {self.organization_id}")
                    break
        except requests.exceptions.RequestException as e:
            print(f"❌ Organization Error: {e}")

    def get_devices(self):
        url = f"{self.base_url}/devices"
        headers = {"Authorization": self.access_token, "MachineId": self.machine_id}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            results = response.json().get("Results", [])
            self.devices_info = []

            for device in results:
                name = (device.get("name") or "").strip()
                external = (device.get("externalmeterid") or "").strip()
                if "Astellas" in name:
                    device_id = device.get("id")
                    area_name = name.replace("Astellas", "").strip() or "Unknown"
                    self.devices_info.append({
                        "id": device_id,
                        "area": area_name,
                        "external": external
                    })
                    print(f"✔ Found: ID={device_id}, Name={name}, Area={area_name}, External={external}")

            if not self.devices_info:
                print("⚠ No devices found with 'Astellas' in the name.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Device Fetch Error: {e}")

    def get_sensor_data(self, device_id):
        url = f"{self.base_url}/dataview"
        headers = {
            "Authorization": self.access_token,
            "MachineId": self.machine_id,
            "Content-Type": "application/json"
        }
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=1)

        payload = {
            "ids": [device_id],
            "source": "device",
            "group": "a",
            "startDate": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Sensor Data Error: {e}")
            return None

    def publish_to_mqtt(self, sensor_data, area, external):
        topic = f"Astellas/{area}/{external}"
        for entry in sensor_data:
            payload = {
                "device_id": entry.get("device_id"),
                "timestamp": entry.get("timestamp"),
                "meter_value": entry.get("meter_value"),
                "temperature_1": entry.get("temperature_1"),
                "battery_voltage": entry.get("battery_voltage")
            }
            self.mqtt_client.publish(topic, json.dumps(payload))
            print(f"📡 Published to {topic}")
            print(json.dumps(payload, indent=2))

# --- Main Execution Loop ---
if __name__ == "__main__":
    api = WalteroAPI("https://emea.api.cloud.waltero.io", "localhost")

    if api.login("knaughton@gis.ie", "Gallarustest"):
        api.get_dashboards()
        api.get_organizations("Gallarus")
        api.get_devices()

        while True:
            for device in api.devices_info:
                print(f"\n🔄 Polling device: {device['id']}")
                data = api.get_sensor_data(device["id"])

                # ✅ Show the full response every time
                print("Sensor Data Response:", data)

                if data:
                    api.publish_to_mqtt(data, device["area"], device["external"])

            print("⏱ Waiting 60 seconds before the next poll...\n")
            time.sleep(60)
