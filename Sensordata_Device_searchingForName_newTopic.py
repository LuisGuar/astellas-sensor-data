import json
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests


class WalteroAPI:
    def __init__(self, base_url: str, mqtt_broker: str, mqtt_port: int = 1888):
        self.base_url = base_url
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port

        self.access_token = None
        self.machine_id = None
        self.organization_id = None
        self.devices_info = []  # Each: {id, area, external}

        # Configure MQTT (use Callback API v2 to avoid deprecation warning)
        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
        # Start background network loop for keepalive and retries
        self.mqtt_client.loop_start()

    def login(self, username: str, password: str):
        url = f"{self.base_url}/users/login"
        headers = {"Content-Type": "application/json"}
        payload = {"username": username, "password": password}
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get("AccessToken")
            self.machine_id = data.get("MachineId")
            if not self.access_token or not self.machine_id:
                print("Error: Missing AccessToken or MachineId in login response.")
                return None
            print("Login successful.")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Login error: {e}")
            return None

    

    def get_organizations(self, org_name: str | None = None):
        url = f"{self.base_url}/organizations"
        headers = {"Authorization": self.access_token, "MachineId": self.machine_id}
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            for org in response.json().get("Results", []):
                if org_name and org.get("name", "").strip() == org_name.strip():
                    self.organization_id = org.get("id")
                    print(
                        f"Stored organizationId for '{org_name}': {self.organization_id}"
                    )
                    break
        except requests.exceptions.RequestException as e:
            print(f"Organization error: {e}")

    def get_devices(self, page_size: int = 50, order_by: str = "modifieddate", is_descending: bool = False):
        if not self.organization_id:
            print("Cannot fetch devices: organization_id is not set. Call get_organizations first.")
            return

        base = f"{self.base_url}/organizations/{self.organization_id}/devices"
        common_headers = {"Authorization": self.access_token, "MachineId": self.machine_id}

        current_page = 1
        total_found = 0
        self.devices_info = []

        try:
            while True:
                pagination = {
                    "CurrentPage": current_page,
                    "PageCount": 1,
                    "PageSize": page_size,
                    "RowCount": 0,
                    "OrderBy": order_by,
                    "isDescending": is_descending,
                    "WhereClauses": [],
                    "WhereORClauses": [],
                }

                headers = dict(common_headers)
                headers["Pagination"] = json.dumps(pagination)

                response = requests.get(base, headers=headers, timeout=20)
                response.raise_for_status()
                payload = response.json() or {}

                results = payload.get("Results", [])
                if not isinstance(results, list):
                    print("Unexpected response format for devices; 'Results' is not a list.")
                    break

                if not results:
                    break

                for device in results:
                    name = (device.get("name") or "").strip()
                    external = (device.get("externalmeterid") or "").strip()
                    if "Astellas" in name:
                        device_id = device.get("id")
                        area_name = name.replace("Astellas", "").strip() or "Unknown"
                        self.devices_info.append(
                            {"id": device_id, "area": area_name, "external": external}
                        )
                        print(
                            f"Found device: id={device_id}, name='{name}', area='{area_name}', external='{external}'"
                        )
                        total_found += 1

                # Determine whether there are more pages
                page_count = payload.get("PageCount")
                if isinstance(page_count, int) and current_page >= page_count:
                    break

                # Fallback: if the API doesn't return PageCount, stop when fewer than page_size were returned
                if page_count is None and len(results) < page_size:
                    break

                current_page += 1

            if total_found == 0:
                print("No devices found with 'Astellas' in the name.")
            else:
                print(f"Total devices matched: {total_found}")
        except requests.exceptions.RequestException as e:
            print(f"Device fetch error: {e}")

    def get_sensor_data(self, device_id: str):
        url = f"{self.base_url}/dataview"
        headers = {
            "Authorization": self.access_token,
            "MachineId": self.machine_id,
            "Content-Type": "application/json",
        }
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=1)

        payload = {
            "ids": [device_id],
            "source": "device",
            "group": "a",
            "startDate": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Sensor data error: {e}")
            return None

    def publish_to_mqtt(self, sensor_data, area: str, external: str):
        topic = f"Astellas/{area}/{external or 'unknown'}"

        # Normalize sensor data to a list of entries
        entries = []
        if isinstance(sensor_data, list):
            entries = sensor_data
        elif isinstance(sensor_data, dict):
            for key in ("Results", "results", "data", "Data"):
                if isinstance(sensor_data.get(key), list):
                    entries = sensor_data[key]
                    break

        if not entries:
            print("Warning: No sensor entries to publish.")
            return

        for entry in entries:
            payload = {
                "timestamp_api": entry.get("timestamp"),
                "time_api": entry.get("time"),
                "external_id": entry.get("external_id"),
                "serial_number":entry.get("serial_number"),
                "device_id": entry.get("device_id"),                
                "meter_value": entry.get("meter_value"),
                "connection_mode": entry.get("connection_mode"),
                "battery_voltage": entry.get("battery_voltage"),
            }
            try:
                result = self.mqtt_client.publish(
                    topic, json.dumps(payload), qos=0, retain=False
                )
                status = result.rc if hasattr(result, "rc") else result[0]
                if status == mqtt.MQTT_ERR_SUCCESS:
                    print(f"Published to '{topic}':")
                else:
                    print(f"Publish returned code {status} for topic '{topic}'.")
            except Exception as e:
                print(f"MQTT publish error: {e}")
            print(json.dumps(payload, indent=2))


# --- Main Execution Loop ---
if __name__ == "__main__":
    # NOTE: Avoid hardcoding credentials in code for production use.
    api = WalteroAPI("https://emea.api.cloud.waltero.io", "localhost")

    if api.login("knaughton@gis.ie", "Gallarustest"):
        api.get_organizations("Gallarus")
        api.get_devices()

        while True:
            for device in api.devices_info:
                print(f"\nPolling device: {device['id']}")
                data = api.get_sensor_data(device["id"])

                # Show the full response every time
                print("Sensor Data Response:", data)

                if data:
                    api.publish_to_mqtt(data, device["area"], device["external"])

            print("Waiting 60 seconds before the next poll...\n")
            time.sleep(60)
