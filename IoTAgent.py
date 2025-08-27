import json
import threading
import time
import logging
from pathlib import Path
from getmac import get_mac_address

from services.ota_service import OTAService
from utils.mqtt_manager import MQTTManager
from utils.http import HttpTool
from utils.process_manager import kill_process, find_and_start_app

from config.constant import (
    GET_MSG_UP_TOPIC,
    GET_MSG_DOWN_TOPIC,
    GET_HEARTBEAT_TOPIC,
    DEVICE_ID,
    MQTT_BROKER,
    MQTT_TMS_BROKER,
    HTTP_BASE_URL,
    HTTP_TMS_BASE_URL
)


logger = logging.getLogger(__name__)

# 连接mqtt
mqtt_manager = MQTTManager(MQTT_BROKER, 1883)
mqtt_tms_manager = MQTTManager(MQTT_TMS_BROKER, 1883)
# 创建HTTP工具类
http = HttpTool(retries=3, timeout=5, base_url=HTTP_BASE_URL)
http_tms = HttpTool(retries=3, timeout=5, base_url=HTTP_TMS_BASE_URL)
# OTA服务类
ota_service = OTAService(mqtt_manager)

# 绑定的设备信息（设备id、设备运行目录、OTA升级状态）
device_info = {}

# 是否初始化订阅mqtt主题的标志
init_subscribe_mqtt_flag = False

# 当前机器人code
robot_code = None
# 是否监听心跳
mqtt_heartbeat_flag = False
# 记录所有程序心跳时间
last_heartbeats = {}

def get_robot_code():
    """获取当前机器人信息"""
    global robot_code, mqtt_heartbeat_flag
    try:
        params = {
            "robotMac": get_mac_address(interface='eth0'),
            "pageNum": 1,
            "pageSize": 10
        }
        res = http_tms.get("/robot/list", params=params).json()
        if res and res.get("code") == 200 and res.get("data") and len(res.get("data").get("list", [])) > 0:
            robot_info = res.get("data").get("list")[0]
            robot_code = robot_info.get("robotCode")
            if robot_code:
                if mqtt_tms_manager.check_connection():
                    # 监听机器人运行程序心跳
                    mqtt_subscribe_heartbeat()
                    mqtt_heartbeat_flag = True
                else:
                    mqtt_heartbeat_flag = False
    except Exception as e:
        logger.error(f"获取机器人信息失败: {str(e)}")
        
def mqtt_subscribe_heartbeat():
    """监听程序心跳"""
    global robot_code
    mqtt_tms_manager.client.subscribe(GET_HEARTBEAT_TOPIC(robot_code))

def get_agent_bind_devices():
    """获取agent绑定的设备信息"""
    try:
        global init_subscribe_mqtt_flag
        res = http.get("/api/agentDevices", params={"agentDeviceId": DEVICE_ID}).json()
        if res and res.get("status") == 200 and res.get("data"):
            for item in res.get("data"):
                device_id = None
                if item.get("isCustomDevice"):
                    device_id = item.get("directory") + "/" + item.get("entryName")
                else:
                    device_id = item.get("device", {}).get("deviceId")
                if not device_id:
                    continue
                device_info[device_id] = {
                    "isCustomDevice": item.get("isCustomDevice"),
                    "directory": item.get("directory"),
                    "entryName": item.get("entryName"),
                    "condaEnv": item.get("condaEnv"),
                    "startCommand": item.get("startCommand"),
                    "MSG_UP_TOPIC": (
                        GET_MSG_UP_TOPIC(device_id)
                        if not item.get("isCustomDevice")
                        else GET_MSG_UP_TOPIC(DEVICE_ID)
                    ),
                    "downloading": False,
                    "stop_flag": False,
                    "updating": False,
                }
                if not item.get("isCustomDevice") and mqtt_manager.check_connection():
                    # 订阅设备消息下发主题
                    mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(device_id))
                    init_subscribe_mqtt_flag = True
                print(f"设备信息：{device_id}")
    except Exception as e:
        logger.error(f"获取设备信息失败: {str(e)}")

def check_heartbeats(timeout = 5):
    """检查所有程序的心跳"""
    global last_heartbeats
    while True:
        current_time = time.time()
        programs_to_restart = []
        
        # 检查哪些程序心跳超时
        for program, beat_info in list(last_heartbeats.items()):
            if current_time - beat_info["timestamp"] > timeout:
                logger.warning(f"程序 {program} 心跳超时")
                programs_to_restart.append(beat_info["reload_command"])
                # 移除超时的程序，避免重复重启
                del last_heartbeats[program]
        
        # 重启超时的程序
        for program_restart_command in programs_to_restart:
           find_and_start_app(None, {"startCommand": program_restart_command})
        
        time.sleep(10)  # 每10秒检查一次

def on_tms_message(client, userdata, message):
    global robot_code, last_heartbeats
    msg = message.payload.decode()
    params = json.loads(msg)
    if message.topic == GET_HEARTBEAT_TOPIC(robot_code):
        # 处理心跳
        program_name = params.get("program")
        timestamp = params.get("timestamp")
        if program_name and timestamp:
            last_heartbeats[program_name] = {
                "timestamp": timestamp,
                "reload_command": params.get("reload_command")
            }
            logger.info(f"收到来自 {program_name} 的心跳")

# 消息处理
def on_message(client, userdata, message):
    msg = message.payload.decode()
    params = json.loads(msg)
    # print(f"Received message: {msg}")
    device_id = message.topic.split("/")[2]
    if device_id in device_info or device_id == DEVICE_ID:
        print("Received msg down:", msg)
        # 消息下发逻辑处理
        if params.get("type") == "OTA":
            # OTA升级逻辑
            # 获取对应设备信息
            device_detail = device_info.get(device_id)
            if device_id == DEVICE_ID:
                # 处理本机设备id(自定义设备OTA升级)
                _device_sign = params.get("processPath") + "/" + params.get("entry")
                if _device_sign in device_info:
                    device_detail = device_info.get(_device_sign)
                else:
                    device_detail = {
                        "isCustomDevice": True,
                        "directory": params.get("processPath"),
                        "entryName": params.get("entry"),
                        "condaEnv": params.get("condaEnv"),
                        "startCommand": params.get("startCommand"),
                        "MSG_UP_TOPIC": GET_MSG_UP_TOPIC(DEVICE_ID),
                        "downloading": False,
                        "stop_flag": False,
                        "updating": False,
                    }
                    device_info[_device_sign] = device_detail
            elif not device_detail:
                print("未找到设备信息")
                mqtt_manager.safe_publish(
                    GET_MSG_DOWN_TOPIC(device_id),
                    json.dumps(
                        {
                            "type": "OTA",
                            "status": "update failed",
                            "error": "未找到设备信息",
                        }
                    ),
                )
                return
            if params.get("url"):
                # 下载文件
                ota_service.download_file(
                    params.get("url"), params.get("md5"), device_detail
                )
            elif params.get("stop"):
                # 停止升级
                print("设置停止升级")
                if (
                    device_detail["updating"] == False
                    and device_detail["downloading"] == False
                ):
                    # 直接停止
                    device_detail["stop_flag"] = False
                    mqtt_manager.safe_publish(
                        device_detail["MSG_UP_TOPIC"],
                        json.dumps({"type": "OTA", "status": "update stopped"}),
                    )
                else:
                    device_detail["stop_flag"] = True
            elif params.get("startUpdate"):
                # 开始升级
                target_path = params.get("processPath") or device_detail.get(
                    "directory"
                )
                if not target_path:
                    print("未找到目标路径")
                    mqtt_manager.safe_publish(
                        device_detail["MSG_UP_TOPIC"],
                        json.dumps(
                            {
                                "type": "OTA",
                                "status": "update failed",
                                "error": "未找到目标路径",
                            }
                        ),
                    )
                    return
                if not device_detail["updating"]:
                    device_detail["updating"] = True
                    # 启动独立线程处理更新，否则会阻塞mqtt消息发布
                    threading.Thread(
                        target=ota_service.handle_start_update,
                        args=(
                            params,
                            target_path,
                            device_detail,
                        ),
                        daemon=True,
                    ).start()
        # 绑定设备信息变更操作
        elif "agentDevice" in params.get("type"):
            if not params.get("deviceId"):
                print("消息下发有误，未找到设备id")
                return
            _agent_device = params.get("agentDevice", {})
            device_sign = None
            if _agent_device.get("isCustomDevice"):
                device_sign = (
                    _agent_device.get("directory")
                    + "/"
                    + _agent_device.get("entryName")
                )
            else:
                device_sign = params.get("deviceId")
            if params.get("type") == "agentDeviceAdd":
                # 添加绑定设备信息
                device_info[device_sign] = {
                    "isCustomDevice": _agent_device.get("isCustomDevice"),
                    "directory": _agent_device.get("directory"),
                    "entryName": _agent_device.get("entryName"),
                    "condaEnv": _agent_device.get("condaEnv"),
                    "startCommand": _agent_device.get("startCommand"),
                    "MSG_UP_TOPIC": (
                        GET_MSG_UP_TOPIC(device_sign)
                        if not _agent_device.get("isCustomDevice")
                        else GET_MSG_UP_TOPIC(DEVICE_ID)
                    ),
                    "downloading": False,
                    "stop_flag": False,
                    "updating": False,
                }
                print("新增绑定设备信息:", device_sign)
                if not _agent_device.get("isCustomDevice"):
                    # 订阅设备消息下发主题
                    mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(device_sign))
                    print(
                        "订阅新绑定设备消息下发主题:", GET_MSG_DOWN_TOPIC(device_sign)
                    )
            elif params.get("type") == "agentDeviceUpdate":
                # 更新绑定设备信息
                device_detail = device_info.get(device_sign)
                if not device_detail:
                    print("未找到绑定设备信息")
                else:
                    device_detail["directory"] = _agent_device.get("directory")
                    device_detail["entryName"] = _agent_device.get("entryName")
                    device_detail["condaEnv"] = _agent_device.get("condaEnv")
                    device_detail["startCommand"] = _agent_device.get("startCommand")
                    print("更新绑定设备信息:", device_sign)
            elif params.get("type") == "agentDeviceDelete":
                # 删除绑定设备信息
                device_detail = device_info.get(device_sign)
                if not device_detail:
                    print("未找到绑定设备信息")
                else:
                    if not device_detail.get("isCustomDevice"):
                        # 取消订阅设备消息下发主题
                        mqtt_manager.client.unsubscribe(GET_MSG_DOWN_TOPIC(device_sign))
                        print(
                            "取消订阅绑定设备消息下发主题:",
                            GET_MSG_DOWN_TOPIC(device_sign),
                        )
                    device_info.pop(device_sign)
                    print("删除绑定设备信息:", device_sign)
        elif params.get("type") == "restart":
            # 终止进程
            _detail_info = {
                "isCustomDevice": params.get("isCustomDevice"),
                "directory": params.get("directory"),
                "entryName": params.get("entryName"),
                "condaEnv": params.get("condaEnv"),
                "startCommand": params.get("startCommand"),
                "stop_flag": False,
                "updating": False,
                "downloading": False,
            }
            kill_process(_detail_info["entryName"])
            print("重启设备")
            # 重新启动进程
            find_and_start_app(Path(params.get("directory")), _detail_info)


# 发送mqtt
def mqtt_loop():
    time.sleep(1)
    while True:
        if mqtt_manager.check_connection():
            # 订阅mqtt主题
            mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(DEVICE_ID))
            if not init_subscribe_mqtt_flag:
                for device_id in device_info.keys():
                    if not device_info[device_id].get("isCustomDevice"):
                        mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(device_id))
            mqtt_manager.client.on_message = on_message
            break
        else:
            print("Connection lost, reconnecting...")
            time.sleep(1)

    global mqtt_heartbeat_flag
    while True:
        if mqtt_tms_manager.check_connection():
            if not mqtt_heartbeat_flag:
                mqtt_subscribe_heartbeat()
            mqtt_tms_manager.client.on_message = on_tms_message
            break
        else:
            print("Connection lost, reconnecting...")
            time.sleep(1)

    while True:
        try:
            mqtt_manager.client.publish(
                GET_MSG_UP_TOPIC(DEVICE_ID), json.dumps({"status": "online"})
            )
        except Exception as e:
            print(f"MQTT发送失败: {str(e)}")
        time.sleep(2)


try:
    get_robot_code()
    get_agent_bind_devices()
    mqtt_thread = threading.Thread(target=mqtt_loop)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    monitor_thread = threading.Thread(target=check_heartbeats)
    monitor_thread.daemon = True
    monitor_thread.start()
    # 保持主线程运行
    while True:
        time.sleep(0.5)
except KeyboardInterrupt:
    mqtt_manager.stop()
    mqtt_tms_manager.stop()
    print("程序已安全退出")
