# 设备常量配置

from getmac import get_mac_address


MQTT_BROKER = "39.105.185.216"
MQTT_TMS_BROKER = "121.5.162.11"
HTTP_BASE_URL = "http://39.105.185.216:8848"
HTTP_TMS_BASE_URL = "http://121.5.162.11:8081"
MAX_BACKUP_COUNT = 3
PRODUCT_AGENT_ID = "681ac31f6cc0a3de12b5020a"

AGENT_FILE_PATH = "/home/rm/Jett/IoTAgent"
OTA_SELF_FULL_PATH = "/home/rm/Jett/ota_self.py"

DEVICE_ID = f"{PRODUCT_AGENT_ID}_{get_mac_address(interface='eth0')}_agent"

GET_HEARTBEAT_TOPIC = lambda robot_code: f"/robot/{robot_code}/heartbeat"
GET_MSG_UP_TOPIC = lambda device_id: f"/devices/{device_id}/sys/messages/up"
GET_MSG_DOWN_TOPIC = lambda device_id: f"/devices/{device_id}/sys/messages/down"
