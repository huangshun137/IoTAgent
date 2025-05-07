# 设备常量配置

from getmac import get_mac_address


# def get_mac_address():
#     mac = get_mac_address()
#     print(mac)
#     return mac
print(get_mac_address(interface="WLAN"))

MQTT_BROKER = "localhost"
HTTP_BASE_URL = "http://localhost:5000"
MAX_BACKUP_COUNT = 3
PRODUCT_AGENT_ID = "67f4e06feb17601896126b35"

DEVICE_ID = f"{PRODUCT_AGENT_ID}_{get_mac_address()}_agent"

GET_MSG_UP_TOPIC = lambda device_id: f"/devices/{device_id}/sys/messages/up"
GET_MSG_DOWN_TOPIC = lambda device_id: f"/devices/{device_id}/sys/messages/down"
