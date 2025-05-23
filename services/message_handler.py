# import json
# from config.constant import DEVICE_ID
# from services.ota_service import OTAService


# class MessageHandler:
#     def __init__(self, mqtt_manager):
#         self.mqtt_manager = mqtt_manager
#         self.ota_service = OTAService(mqtt_manager)

#     def handle_message(self):
#         # Placeholder for message handling logic
#         print(f"Handling message: {self.message}")
#         # Add your message processing logic here
#         return f"Processed: {self.message}"

#     # 消息处理

#     def on_message(client, userdata, message):
#         msg = message.payload.decode()
#         params = json.loads(msg)
#         # print(f"Received message: {msg}")
#         device_id = message.topic.split("/")[2]
#         if device_id in device_info or device_id == DEVICE_ID:
#             print("Received msg down:", msg)
#             # 消息下发逻辑处理
#             if params.get("type") == "OTA":
#                 # OTA升级逻辑
#                 # 获取对应设备信息
#                 device_detail = device_info.get(device_id)
#                 if device_id == DEVICE_ID:
#                     # 处理本机设备id(自定义设备OTA升级)
#                     _device_sign = params.get("processPath") + "/" + params.get("entry")
#                     if _device_sign in device_info:
#                         device_detail = device_info.get(_device_sign)
#                     else:
#                         device_detail = {
#                             "isCustomDevice": True,
#                             "directory": params.get("processPath"),
#                             "entryName": params.get("entry"),
#                             "condaEnv": params.get("condaEnv"),
#                             "MSG_UP_TOPIC": GET_MSG_UP_TOPIC(DEVICE_ID),
#                             "downloading": False,
#                             "stop_flag": False,
#                             "updating": False,
#                         }
#                         device_info[_device_sign] = device_detail
#                 elif not device_detail:
#                     print("未找到设备信息")
#                     mqtt_manager.safe_publish(
#                         GET_MSG_DOWN_TOPIC(device_id),
#                         json.dumps(
#                             {
#                                 "type": "OTA",
#                                 "status": "update failed",
#                                 "error": "未找到设备信息",
#                             }
#                         ),
#                     )
#                     return
#                 if params.get("url"):
#                     # 下载文件
#                     download_file(params.get("url"), params.get("md5"), device_detail)
#                 elif params.get("stop"):
#                     # 停止升级
#                     print("设置停止升级")
#                     if (
#                         device_detail["updating"] == False
#                         and device_detail["downloading"] == False
#                     ):
#                         # 直接停止
#                         device_detail["stop_flag"] = False
#                         mqtt_manager.safe_publish(
#                             device_detail["MSG_UP_TOPIC"],
#                             json.dumps({"type": "OTA", "status": "update stopped"}),
#                         )
#                     else:
#                         device_detail["stop_flag"] = True
#                 elif params.get("startUpdate"):
#                     # 开始升级
#                     target_path = params.get("processPath") or device_detail.get(
#                         "directory"
#                     )
#                     if not target_path:
#                         print("未找到目标路径")
#                         mqtt_manager.safe_publish(
#                             device_detail["MSG_UP_TOPIC"],
#                             json.dumps(
#                                 {
#                                     "type": "OTA",
#                                     "status": "update failed",
#                                     "error": "未找到目标路径",
#                                 }
#                             ),
#                         )
#                         return
#                     if not device_detail["updating"]:
#                         device_detail["updating"] = True
#                         # 启动独立线程处理更新，否则会阻塞mqtt消息发布
#                         threading.Thread(
#                             target=handle_start_update,
#                             args=(
#                                 params,
#                                 target_path,
#                                 device_detail,
#                             ),
#                             daemon=True,
#                         ).start()
#             # 绑定设备信息变更操作
#             elif "agentDevice" in params.get("type"):
#                 if not params.get("deviceId"):
#                     print("消息下发有误，未找到设备id")
#                     return
#                 _agent_device = params.get("agentDevice", {})
#                 device_sign = None
#                 if _agent_device.get("isCustomDevice"):
#                     device_sign = (
#                         _agent_device.get("directory")
#                         + "/"
#                         + _agent_device.get("entryName")
#                     )
#                 else:
#                     device_sign = params.get("deviceId")
#                 if params.get("type") == "agentDeviceAdd":
#                     # 添加绑定设备信息
#                     device_info[device_sign] = {
#                         "isCustomDevice": _agent_device.get("isCustomDevice"),
#                         "directory": _agent_device.get("directory"),
#                         "entryName": _agent_device.get("entryName"),
#                         "condaEnv": _agent_device.get("condaEnv"),
#                         "MSG_UP_TOPIC": (
#                             GET_MSG_UP_TOPIC(device_sign)
#                             if not _agent_device.get("isCustomDevice")
#                             else GET_MSG_UP_TOPIC(DEVICE_ID)
#                         ),
#                         "downloading": False,
#                         "stop_flag": False,
#                         "updating": False,
#                     }
#                     print("新增绑定设备信息:", device_sign)
#                     if not _agent_device.get("isCustomDevice"):
#                         # 订阅设备消息下发主题
#                         mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(device_sign))
#                         print(
#                             "订阅新绑定设备消息下发主题:",
#                             GET_MSG_DOWN_TOPIC(device_sign),
#                         )
#                 elif params.get("type") == "agentDeviceUpdate":
#                     # 更新绑定设备信息
#                     device_detail = device_info.get(device_sign)
#                     if not device_detail:
#                         print("未找到绑定设备信息")
#                     else:
#                         device_detail["directory"] = _agent_device.get("directory")
#                         device_detail["entryName"] = _agent_device.get("entryName")
#                         device_detail["condaEnv"] = _agent_device.get("condaEnv")
#                         print("更新绑定设备信息:", device_sign)
#                 elif params.get("type") == "agentDeviceDelete":
#                     # 删除绑定设备信息
#                     device_detail = device_info.get(device_sign)
#                     if not device_detail:
#                         print("未找到绑定设备信息")
#                     else:
#                         if not device_detail.get("isCustomDevice"):
#                             # 取消订阅设备消息下发主题
#                             mqtt_manager.client.unsubscribe(
#                                 GET_MSG_DOWN_TOPIC(device_sign)
#                             )
#                             print(
#                                 "取消订阅绑定设备消息下发主题:",
#                                 GET_MSG_DOWN_TOPIC(device_sign),
#                             )
#                         device_info.pop(device_sign)
#                         print("删除绑定设备信息:", device_sign)
#             elif params.get("type") == "restart":
#                 # 终止进程
#                 _detail_info = {
#                     "isCustomDevice": params.get("isCustomDevice"),
#                     "directory": params.get("directory"),
#                     "entryName": params.get("entryName"),
#                     "condaEnv": params.get("condaEnv"),
#                     "stop_flag": False,
#                     "updating": False,
#                     "downloading": False,
#                 }
#                 kill_process(_detail_info)
#                 print("重启设备")
#                 # 重新启动进程
#                 find_and_start_app(Path(params.get("directory")), _detail_info)
