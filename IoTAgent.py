import json
import threading
import time
import psutil
import zipfile
import rarfile
import py7zr
import subprocess
import shutil
import logging
from datetime import datetime
from pathlib import Path

from typing import Dict, Set

from utils.mqtt_manager import MQTTManager
from utils.downloader import SecureFileDownloader
from utils.http import HttpTool

import config.constant as constants

DEVICE_ID = constants.DEVICE_ID
GET_MSG_UP_TOPIC = lambda device_id: f"/devices/{device_id}/sys/messages/up"
GET_MSG_DOWN_TOPIC = lambda device_id: f"/devices/{device_id}/sys/messages/down"

# PROCESS_NAME = "main1.py"
# TARGET_DIR = "/代码/python/water"
MAX_BACKUP_COUNT = 3

logger = logging.getLogger(__name__)

# 连接mqtt
mqtt_manager = MQTTManager(constants.MQTT_BROKER, 1883)
# 创建下载器
downloader = SecureFileDownloader()
# 创建HTTP工具类
http = HttpTool(retries=3, timeout=5, base_url=constants.HTTP_BASE_URL)

# 绑定的设备信息（设备id、设备运行目录、OTA升级状态）
device_info = {}

# 是否初始化订阅mqtt主题的标志
init_subscribe_mqtt_flag = False

class ArchiveError(Exception):
    """压缩包处理异常基类"""
    pass

def get_agent_bind_devices():
  """获取agent绑定的设备信息"""
  try:
    global init_subscribe_mqtt_flag
    res = http.get("/api/agentDevices", params={'agentDeviceId': DEVICE_ID}).json()
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
          "MSG_UP_TOPIC": GET_MSG_UP_TOPIC(device_id) if not item.get("isCustomDevice") else GET_MSG_UP_TOPIC(DEVICE_ID),
          "downloading": False,
          "stop_flag": False,
          "updating": False
        }
        if not item.get("isCustomDevice") and mqtt_manager.check_connection():
          # 订阅设备消息下发主题
          mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(device_id))
          init_subscribe_mqtt_flag = True
        print(f"设备信息：{device_id}")
  except Exception as e:
    logger.error(f"获取设备信息失败: {str(e)}")

# =============OTA 升级相关函数 START=================
def download_file(url, expected_md5, device_detail):
  """下载更新压缩包"""
  if not device_detail["downloading"]:
    device_detail["downloading"] = True
    # 通知IOT系统开始下载
    mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
      "type": "OTA",
      "status": "downloading",
      "timestamp": time.time()
    }))
    # if publish:
    #   publish.wait_for_publish()
    # 下载也要开线程，不然会阻止mqtt消息发布
    threading.Thread(target=download_file_thread, args=(url,expected_md5,device_detail,), daemon=True).start()
    # return result.get("path", None)

def download_file_thread(url,expected_md5,device_detail):
  result = downloader.download(url, expected_md5=expected_md5)
  if result["status"] == "success":
    print(f"下载成功：{result['path']}")
    # 通知IOT系统下载成功
    mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
      "type": "OTA",
      "status": "download success",
      "path": result['path'],
      "timestamp": time.time()
    }))
  else:
    print(f"下载失败：{result['message']}")
    time.sleep(1) # 延迟1秒，防止1ms内就下载完成
    errMsg = result['message']
    if ("MD5校验失败" in errMsg):
      print("MD5校验失败")
      errMsg = "MD5校验失败"
    elif ("Internal Server Error" in errMsg):
      print("接口请求失败")
      errMsg = "接口请求失败"
    # 通知IOT系统下载失败
    mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
      "type": "OTA",
      "status": "download failed",
      "error": errMsg
    }))
  device_detail["downloading"] = False

def check_stop_flag(device_detail):
  """检查停止标志位"""
  if device_detail["stop_flag"]:
    print("停止标志位已设置，正在退出...")
    raise Exception("终止升级")
  
# 终止进程
def kill_process(device_detail):
  """终止目标进程"""
  check_stop_flag(device_detail)
  killed = []
  for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
    try:
      # 检查进程命令行是否包含目标脚本
      if any(device_detail["entryName"] in cmd for cmd in proc.cmdline()):
        proc.terminate()
        killed.append(proc.pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
      continue
  print(f"终止进程: {killed}")
  # 等待进程终止，并处理可能的 NoSuchProcess 异常
  processes_to_wait = []
  for pid in killed:
    try:
      processes_to_wait.append(psutil.Process(pid))
    except psutil.NoSuchProcess:
      # 如果进程已经不存在，则跳过
      continue
  gone, alive = psutil.wait_procs(processes_to_wait, timeout=5)
  if alive:
    for p in alive:
      p.kill()
  return len(killed) > 0

def analyze_archive_structure(file_path: Path) -> Dict:
  """
  分析压缩包结构（支持ZIP/RAR/7Z）
  返回结构：
  {
    "format": "zip/rar/7z",
    "is_single_dir": bool,
    "top_dir": str,
    "file_count": int,
    "all_files": list[str]
  }
  """
  try:
    ext = file_path.suffix.lower()
    all_files = []
    top_dirs: Set[str] = set()

    # ZIP格式处理
    if ext == '.zip':
      with zipfile.ZipFile(file_path, 'r') as zf:
          all_files = zf.namelist()
    
    # RAR格式处理
    elif ext == '.rar':
      with rarfile.RarFile(file_path, 'r', charset='gbk') as rf:
          all_files = [f.filename for f in rf.infolist()]
    
    # 7Z格式处理
    elif ext in ('.7z', '.7zip'):
      with py7zr.SevenZipFile(file_path, 'r') as z7:
        all_files = z7.getnames()
    
    else:
      raise ArchiveError(f"不支持的压缩格式: {ext}")

    # 统一分析文件结构
    for name in all_files:
      parts = name.replace('\\', '/').split('/')
      if len(parts) > 1 and parts[0]:
        top_dirs.add(parts[0])
      else:
        # 处理根目录文件
        if '.' in parts[-1]:  # 判断是否为文件
          top_dirs.add("root_files")
        elif parts[0]:
          top_dirs.add(parts[0])

    return {
      "format": ext[1:],
      "is_single_dir": len(top_dirs) == 1 and 'root_files' not in top_dirs,
      "top_dir": next(iter(top_dirs)) if len(top_dirs) == 1 else None,
      "file_count": len(all_files),
      "all_files": all_files
    }

  except Exception as e:
    raise ArchiveError(f"文件结构分析错误: {str(e)}") from e

def backup_directory(target_dir):
  """
  安全备份目录
  返回实际备份目录
  """
  if target_dir.exists():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_dir = target_dir.with_name(f"{target_dir.name}_backup_{timestamp}")
    target_dir.rename(backup_dir)
    # 清理旧备份（保留最多5个）
    try:
      # 获取所有同版本备份目录（按时间倒序）
      backup_dirs = sorted(
        target_dir.parent.glob(f"{target_dir.name}_backup_*"),  # 匹配模式
        key=lambda x: x.stat().st_mtime,  # 按修改时间排序（最新在前）
        reverse=True
      )
      # 删除超过保留数量的旧备份
      if len(backup_dirs) > MAX_BACKUP_COUNT:
        for old_dir in backup_dirs[MAX_BACKUP_COUNT:]:
          try:
            shutil.rmtree(old_dir)
            logger.info(f"删除旧备份: {old_dir}")
          except Exception as e:
            logger.error(f"删除备份失败 {old_dir}: {str(e)}")
            continue
    except Exception as e:
      logger.error(f"备份清理失败: {str(e)}")
      raise f"备份清理失败: {str(e)}"

def extract_archive(src_path: Path, target_dir: Path) -> Path:
  """
  安全解压压缩包（支持ZIP/RAR/7Z）
  返回实际解压目录
  """
  try:
    # 自动清理已存在目录
    if target_dir.exists():
      shutil.rmtree(target_dir)
      logger.warning(f"已清理现有目录: {target_dir}")

    # 分析压缩包结构
    archive_info = analyze_archive_structure(src_path)
    
    # 创建父目录（延迟创建目标目录）
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    # ZIP格式解压
    if archive_info["format"] == 'zip':
      with zipfile.ZipFile(src_path, 'r') as zf:
        if archive_info["is_single_dir"]:
          zf.extractall(target_dir.parent)
          original_dir = target_dir.parent / archive_info["top_dir"]
          original_dir.rename(target_dir)
        else:
          zf.extractall(target_dir)
    
    # RAR格式解压
    elif archive_info["format"] == 'rar':
      with rarfile.RarFile(src_path, 'r', charset='gbk') as rf:
        if archive_info["is_single_dir"]:
          rf.extractall(target_dir.parent)
          original_dir = target_dir.parent / archive_info["top_dir"]
          original_dir.rename(target_dir)
        else:
          rf.extractall(target_dir)
    
    # 7Z格式解压
    elif archive_info["format"] in ('7z', '7zip'):
      with py7zr.SevenZipFile(src_path, 'r') as z7:
        if archive_info["is_single_dir"]:
          z7.extractall(target_dir.parent)
          original_dir = target_dir.parent / archive_info["top_dir"]
          original_dir.rename(target_dir)
        else:
          z7.extractall(target_dir)
    
    logger.info(f"成功解压 {src_path.name} 到 {target_dir}")
    return target_dir

  except Exception as e:
    logger.error(f"解压失败: {str(e)}")
    if target_dir.exists():
      shutil.rmtree(target_dir)
    raise ArchiveError(f"解压操作失败: {str(e)}") from e

def find_and_start_app(target_dir, device_detail):
  """查找并启动应用程序"""
  check_stop_flag(device_detail)
  # 查找入口文件
  _entry_file = target_dir / device_detail["entryName"]
  
  if not _entry_file.exists():
    raise FileNotFoundError("未找到入口文件")
  
  try:
    # 启动应用程序
    order = ["python", str(_entry_file)]
    if device_detail["condaEnv"]:
      order = ["conda", "run", "-n", device_detail["condaEnv"], "python", str(_entry_file)]
    subprocess.Popen(
      order,
      cwd=target_dir,
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL,
      start_new_session=True
    )
    # stdout, stderr = process.communicate()
    print("应用程序已启动")
    # print(stdout)
  except Exception as e:
    logger.error(f"应用程序启动失败: {str(e)}")
    raise f"应用程序启动失败: {str(e)}"

def handle_start_update(params, target_path, device_detail):
  """处理startUpdate的独立线程函数"""
  try:
    zip_path = params.get("path")
    entry_file = device_detail.get("entryName")
    if not zip_path or not Path(zip_path).exists():
      mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
        "type": "OTA",
        "status": "update failed",
        "error": "未找到资源包"
      }))
      return

    # 发送开始更新通知
    mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
      "type": "OTA",
      "status": "start update"
    }))

    # 终止旧进程
    if not kill_process(device_detail):
      print("没有找到运行的进程")
    
    time.sleep(2)  # 等待资源释放

    file_name = params.get("filename") or Path(zip_path).name or params.get('version')
    target_dir = Path(f"{target_path}/{file_name}")
    # 备份资源包
    check_stop_flag(device_detail)
    backup_directory(target_dir)

    check_stop_flag(device_detail)
    extract_archive(Path(zip_path), target_dir)

    # 更新设备代码中的版本号
    version_file = target_dir / "version.txt"
    version_info = params.get("version", "unknown")
    try:
      with open(version_file, "w", encoding="utf-8") as f:
        f.write(version_info)
      logger.info(f"版本文件已生成：{version_file}")
    except Exception as e:
      logger.error(f"版本文件写入失败: {str(e)}")
      raise f"版本文件写入失败: {str(e)}"

    # 启动新程序
    find_and_start_app(target_dir, device_detail)

    # 更新Agent版本号管理
    version_agent_file = Path("./version.json")
    version_data = {}
    try:
      if version_agent_file.exists():
        with open(version_agent_file, "r", encoding="utf-8") as f:
          version_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
      logger.warning(f"版本文件读取失败: {str(e)}，将创建新文件")

    version_data[entry_file] = version_info
    with open(version_agent_file, "w", encoding="utf-8") as f:
      json.dump(version_data, f, indent=2, ensure_ascii=False)
    logger.info("agent版本管理文件已更新")

    # 更新成功通知
    mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
      "type": "OTA",
      "status": "update success",
      "version": version_info
    }))


  except Exception as e:
    error = str(e)
    if error == "终止升级":
      logger.info("终止升级")
      mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
        "type": "OTA",
        "status": "update stopped"
      }))
    else:
      logger.error(f"更新失败: {str(e)}")
      mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
        "type": "OTA",
        "status": "update failed",
        "error": str(e)
      }))

  finally:
    device_detail["stop_flag"] = False
    device_detail["updating"] = False
# =============OTA 升级相关函数 END=================
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
            "MSG_UP_TOPIC": GET_MSG_UP_TOPIC(DEVICE_ID),
            "downloading": False,
            "stop_flag": False,
            "updating": False
          }
          device_info[_device_sign] = device_detail
      elif not device_detail:
        print("未找到设备信息")
        mqtt_manager.safe_publish(GET_MSG_DOWN_TOPIC(device_id), json.dumps({
          "type": "OTA",
          "status": "update failed",
          "error": "未找到设备信息"
        }))
        return
      if params.get("url"):
        # 下载文件
        download_file(params.get("url"), params.get("md5"), device_detail)
      elif params.get("stop"):
        # 停止升级
        print("设置停止升级")
        if device_detail["updating"] == False and device_detail["downloading"] == False:
          # 直接停止
          device_detail["stop_flag"] = False
          mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
            "type": "OTA",
            "status": "update stopped"
          }))
        else:
          device_detail["stop_flag"] = True
      elif params.get("startUpdate"):
        # 开始升级
        target_path = params.get("processPath") or device_detail.get("directory")
        if not target_path:
          print("未找到目标路径")
          mqtt_manager.safe_publish(device_detail["MSG_UP_TOPIC"], json.dumps({
            "type": "OTA",
            "status": "update failed",
            "error": "未找到目标路径"
          }))
          return
        if not device_detail["updating"]:
          device_detail["updating"] = True
          # 启动独立线程处理更新，否则会阻塞mqtt消息发布
          threading.Thread(target=handle_start_update, args=(params,target_path,device_detail,), daemon=True).start()
    # 绑定设备信息变更操作
    elif "agentDevice" in params.get("type"):
      if not params.get("deviceId"):
        print("消息下发有误，未找到设备id")
        return
      _agent_device = params.get("agentDevice", {})
      device_sign = None
      if _agent_device.get("isCustomDevice"):
        device_sign = _agent_device.get("directory") + "/" + _agent_device.get("entryName")
      else:
        device_sign = params.get("deviceId")
      if params.get("type") == "agentDeviceAdd":
        # 添加绑定设备信息
        device_info[device_sign] = {
          "isCustomDevice": _agent_device.get("isCustomDevice"),
          "directory": _agent_device.get("directory"),
          "entryName": _agent_device.get("entryName"),
          "condaEnv": _agent_device.get("condaEnv"),
          "MSG_UP_TOPIC": GET_MSG_UP_TOPIC(device_sign) if not _agent_device.get("isCustomDevice") else GET_MSG_UP_TOPIC(DEVICE_ID),
          "downloading": False,
          "stop_flag": False,
          "updating": False
        }
        print("新增绑定设备信息:", device_sign)
        if not _agent_device.get("isCustomDevice"):
          # 订阅设备消息下发主题
          mqtt_manager.client.subscribe(GET_MSG_DOWN_TOPIC(device_sign))
          print("订阅新绑定设备消息下发主题:", GET_MSG_DOWN_TOPIC(device_sign))
      elif params.get("type") == "agentDeviceUpdate":
        # 更新绑定设备信息
        device_detail = device_info.get(device_sign)
        if not device_detail:
          print("未找到绑定设备信息")
        else:
          device_detail["directory"] = _agent_device.get("directory")
          device_detail["entryName"] = _agent_device.get("entryName")
          device_detail["condaEnv"] = _agent_device.get("condaEnv")
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
            print("取消订阅绑定设备消息下发主题:", GET_MSG_DOWN_TOPIC(device_sign))
          device_info.pop(device_sign)
          print("删除绑定设备信息:", device_sign)
    elif params.get("type") == "restart":
      # 终止进程
      _detail_info = {
        "isCustomDevice": params.get("isCustomDevice"),
        "directory": params.get("directory"),
        "entryName": params.get("entryName"),
        "condaEnv": params.get("condaEnv"),
        "stop_flag": False,
        "updating": False,
        "downloading": False
      }
      kill_process(_detail_info)
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

  while True:
    try:
      mqtt_manager.client.publish(GET_MSG_UP_TOPIC(DEVICE_ID), json.dumps({"status": "online"}))
    except Exception as e:
      print(f"MQTT发送失败: {str(e)}")
    time.sleep(2)

try:
  get_agent_bind_devices()
  mqtt_thread = threading.Thread(target=mqtt_loop)
  mqtt_thread.daemon = True
  mqtt_thread.start()
  # 保持主线程运行
  while True:
    time.sleep(0.5)
except KeyboardInterrupt:
  mqtt_manager.stop()
  print("程序已安全退出")