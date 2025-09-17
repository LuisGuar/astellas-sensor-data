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

    def get_device_statuses(self, batch_size: int = 50):
        url = f"{self.base_url}/device-statuses"
        headers = {
            "Authorization": self.access_token,
            "MachineId": self.machine_id,
            "Content-Type": "application/json",
        }

        # Collect device IDs from devices_info
        device_ids = [d.get("id") for d in self.devices_info if d.get("id")]
        if not device_ids:
            print("No device IDs available. Did you call get_devices()?")
            return []

        # Helper to chunk IDs
        def chunks(seq, size):
            for i in range(0, len(seq), size):
                yield seq[i : i + size]

        all_statuses = []
        try:
            for group in chunks(device_ids, batch_size):
                body = [{"deviceid": did} for did in group]
                response = requests.post(url, json=body, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json() or []

                # Normalize: API may return list directly or under a key
                if isinstance(data, list):
                    statuses = data
                elif isinstance(data, dict):
                    # Try common keys
                    statuses = data.get("Results") or data.get("results") or data.get("data") or []
                    if not isinstance(statuses, list):
                        statuses = []
                else:
                    statuses = []

                all_statuses.extend(statuses)

            print(f"Fetched statuses for {len(all_statuses)} device entries.")
            return all_statuses
        except requests.exceptions.RequestException as e:
            print(f"Device statuses error: {e}")
            return []

    def publish_statuses_to_mqtt(self, statuses):
        if not statuses:
            print("No statuses to publish.")
            return

        published = 0
        for entry in statuses:
            # Expecting keys like: deviceid, deveui, isconnected, lastValue, usage24hrs, lastTimestamp
            device_id = entry.get("deviceid") or entry.get("device_id") or entry.get("id")
            if not device_id:
                continue

            # Topic per your spec: Astellas/Statuses/{deviceid}
            topic = f"Astellas/Statuses/{device_id}"

            # Build transformed payload with requested field names
            payload = {
                "device_id": device_id,
                "is_connected": entry.get("isconnected"),
                "last_value": entry.get("lastValue"),
                "usage_daily": entry.get("usage24hrs"),
                "last_timestamp": entry.get("lastTimestamp"),
            }

            try:
                result = self.mqtt_client.publish(topic, json.dumps(payload), qos=0, retain=False)
                status_code = result.rc if hasattr(result, "rc") else result[0]
                if status_code != mqtt.MQTT_ERR_SUCCESS:
                    print(f"Publish returned code {status_code} for topic '{topic}'.")
                else:
                    print(f"Published status to '{topic}'.")
                published += 1
            except Exception as e:
                print(f"MQTT publish error: {e}")

        print(f"Total statuses published: {published}")


# --- Main Execution Loop ---
if __name__ == "__main__":
    # NOTE: Avoid hardcoding credentials in code for production use.
    api = WalteroAPI("https://emea.api.cloud.waltero.io", "localhost")

    if api.login("knaughton@gis.ie", "Gallarustest"):
        api.get_organizations("Gallarus")
        api.get_devices()

        while True:
            print("\nRequesting device statuses for all devices...")
            statuses = api.get_device_statuses()

            # Show the response count each cycle
            print(f"Statuses received: {len(statuses)}")

            if statuses:
                api.publish_statuses_to_mqtt(statuses)

            print("Waiting 60 seconds before the next poll...\n")
            time.sleep(60)
