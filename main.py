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

DEVICE_ID = "67f4e06feb17601896126b35_GONGKONG_MAC"
MSG_UP_TOPIC = f"/devices/{DEVICE_ID}/sys/messages/up"
MSG_DOWN_TOPIC = f"/devices/{DEVICE_ID}/sys/messages/down"

# PROCESS_NAME = "main1.py"
# TARGET_DIR = "/代码/python/water"
MAX_BACKUP_COUNT = 3

logger = logging.getLogger(__name__)

# 连接mqtt
mqtt_manager = MQTTManager("localhost", 1883)
# 创建下载器
downloader = SecureFileDownloader()

# 程序入口文件
entry_file = None
# 绑定的设备信息（设备id、设备运行目录）
device_info = {
  "deviceId1111": "devicePath1111",
  "deviceId2222": "devicePath2222",
  "deviceId3333": "devicePath3333",
}

# 下载文件
downloading = False
# 停止flag
stop_flag = False

class ArchiveError(Exception):
    """压缩包处理异常基类"""
    pass

def download_file(url, expected_md5):
  """下载更新压缩包"""
  global downloading  # 声明使用全局变量
  if not downloading:
    downloading = True
    # 通知IOT系统开始下载
    publish = mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
      "type": "OTA",
      "status": "downloading",
      "timestamp": time.time()
    }))
    # if publish:
    #   publish.wait_for_publish()
    result = downloader.download(url, expected_md5=expected_md5)
    if result["status"] == "success":
      print(f"下载成功：{result['path']}")
      time.sleep(1) # 延迟1秒，防止1ms内就下载完成
      # 通知IOT系统下载成功
      mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
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
      mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
        "type": "OTA",
        "status": "download failed",
        "error": errMsg
      }))
    time.sleep(1)
    downloading = False
    return result.get("path", None)

def check_stop_flag():
  """检查停止标志位"""
  global stop_flag
  if stop_flag:
    print("停止标志位已设置，正在退出...")
    raise Exception("终止升级")
  
# 终止进程
def kill_process():
  """终止目标进程"""
  check_stop_flag()
  global entry_file
  killed = []
  for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
    try:
      # 检查进程命令行是否包含目标脚本
      if any(entry_file in cmd for cmd in proc.cmdline()):
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
  check_stop_flag()
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
  check_stop_flag()
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
  check_stop_flag()
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

def find_and_start_app(target_dir):
  """查找并启动应用程序"""
  check_stop_flag()
  global entry_file
  # 查找入口文件
  _entry_file = target_dir / entry_file
  
  if not _entry_file.exists():
    raise FileNotFoundError("未找到入口文件")
  
  try:
    # 启动应用程序
    subprocess.Popen(
      ["python", str(_entry_file)],
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

updating = False
def handle_start_update(params, target_path):
  """处理startUpdate的独立线程函数"""
  global entry_file
  try:
    zip_path = params.get("path")
    entry_file = params.get("entry")
    if not zip_path or not Path(zip_path).exists():
      mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
        "type": "OTA",
        "status": "update failed",
        "error": "未找到资源包"
      }))
      return

    # 发送开始更新通知
    mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
      "type": "OTA",
      "status": "start update"
    }))

    # 终止旧进程
    if not kill_process():
      print("没有找到运行的进程")
    
    time.sleep(2)  # 等待资源释放

    target_dir = Path(f"{target_path}/{params.get('version')}")
    # 备份资源包
    backup_directory(target_dir)

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
    find_and_start_app(target_dir)

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
    mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
      "type": "OTA",
      "status": "update success",
      "version": version_info
    }))


  except Exception as e:
    error = str(e)
    if error == "终止升级":
      logger.info("终止升级")
      mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
        "type": "OTA",
        "status": "update stopped"
      }))
    else:
      logger.error(f"更新失败: {str(e)}")
      mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
        "type": "OTA",
        "status": "update failed",
        "error": str(e)
      }))

  finally:
    global stop_flag
    global updating
    stop_flag = False
    updating = False
# 消息处理
def on_message(client, userdata, message):
  msg = message.payload.decode()
  params = json.loads(msg)
  # print(f"Received message: {msg}")
  # TODO 添加agent绑定的不同设备id的处理逻辑
  if message.topic == MSG_DOWN_TOPIC:
    print("Received msg down:", msg)
    # 消息下发逻辑处理
    if params.get("type") == "OTA":
      # OTA升级逻辑
      if params.get("url"):
        # 下载文件
        download_file(params.get("url"), params.get("md5"))
      elif params.get("stop"):
        # 停止升级
        print("设置停止升级")
        global stop_flag
        stop_flag = True
      elif params.get("startUpdate"):
        # 开始升级
        target_path = params.get("processPath") or device_info.get(DEVICE_ID)
        if not target_path:
          print("未找到目标路径")
          mqtt_manager.safe_publish(MSG_UP_TOPIC, json.dumps({
            "type": "OTA",
            "status": "update failed",
            "error": "未找到目标路径"
          }))
          return
        global updating  # 声明使用全局变量
        if not updating:
          updating = True
          # 启动独立线程处理更新，否则会阻塞mqtt消息发布
          threading.Thread(target=handle_start_update, args=(params,target_path,), daemon=True).start()

# 发送mqtt
def mqtt_loop():
  time.sleep(1)
  while True:
    if mqtt_manager.check_connection():
      # 订阅mqtt主题
      mqtt_manager.client.subscribe(MSG_DOWN_TOPIC)

      mqtt_manager.client.on_message = on_message
      break
    else:
      print("Connection lost, reconnecting...")
      time.sleep(1)

  while True:
    try:
      mqtt_manager.client.publish(MSG_UP_TOPIC, json.dumps({"status": "online"}))
    except Exception as e:
      print(f"MQTT发送失败: {str(e)}")
    time.sleep(2)

try:
  mqtt_thread = threading.Thread(target=mqtt_loop)
  mqtt_thread.daemon = True
  mqtt_thread.start()
  # 保持主线程运行
  while True:
    time.sleep(0.5)
except KeyboardInterrupt:
  mqtt_manager.stop()
  print("程序已安全退出")