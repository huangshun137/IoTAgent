from datetime import datetime
import shutil
import subprocess
import threading
import time
import json
from pathlib import Path
import logging

from config.constant import AGENT_FILE_PATH, MAX_BACKUP_COUNT, OTA_SELF_FULL_PATH
from utils import downloader, archive_handler
from utils.common import get_conda_executable_path
from utils.process_manager import kill_process, find_and_start_app

logger = logging.getLogger(__name__)


class OTAService:
    def __init__(self, mqtt_manager):
        # self.device_manager = device_manager
        self.mqtt_manager = mqtt_manager
        self.downloader = downloader.SecureFileDownloader()

    def download_file(self, url, expected_md5, device_detail):
        """下载更新压缩包"""
        if not device_detail["downloading"]:
            device_detail["downloading"] = True
            # 通知IOT系统开始下载
            self.mqtt_manager.safe_publish(
                device_detail["MSG_UP_TOPIC"],
                json.dumps(
                    {"type": "OTA", "status": "downloading", "timestamp": time.time()}
                ),
            )
            # if publish:
            #   publish.wait_for_publish()
            # 下载也要开线程，不然会阻止mqtt消息发布
            threading.Thread(
                target=self.download_file_thread,
                args=(
                    url,
                    expected_md5,
                    device_detail,
                ),
                daemon=True,
            ).start()
            # return result.get("path", None)

    def download_file_thread(self, url, expected_md5, device_detail):
        result = self.downloader.download(url, expected_md5=expected_md5)
        if result["status"] == "success":
            print(f"下载成功：{result['path']}")
            # 通知IOT系统下载成功
            self.mqtt_manager.safe_publish(
                device_detail["MSG_UP_TOPIC"],
                json.dumps(
                    {
                        "type": "OTA",
                        "status": "download success",
                        "path": result["path"],
                        "timestamp": time.time(),
                    }
                ),
            )
        else:
            print(f"下载失败：{result['message']}")
            time.sleep(1)  # 延迟1秒，防止1ms内就下载完成
            errMsg = result["message"]
            if "MD5校验失败" in errMsg:
                print("MD5校验失败")
                errMsg = "MD5校验失败"
            elif "Internal Server Error" in errMsg:
                print("接口请求失败")
                errMsg = "接口请求失败"
            # 通知IOT系统下载失败
            self.mqtt_manager.safe_publish(
                device_detail["MSG_UP_TOPIC"],
                json.dumps(
                    {"type": "OTA", "status": "download failed", "error": errMsg}
                ),
            )
        device_detail["downloading"] = False

    def check_stop_flag(self, device_detail):
        """检查停止标志位"""
        if device_detail["stop_flag"]:
            print("停止标志位已设置，正在退出...")
            raise Exception("终止升级")

    def backup_directory(self, target_dir):
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
                    reverse=True,
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

    def handle_start_update(self, params, target_path, device_detail):
        """处理startUpdate的独立线程函数"""
        try:
            zip_path = params.get("path")
            entry_file = device_detail.get("entryName")
            if not zip_path or not Path(zip_path).exists():
                self.mqtt_manager.safe_publish(
                    device_detail["MSG_UP_TOPIC"],
                    json.dumps(
                        {
                            "type": "OTA",
                            "status": "update failed",
                            "error": "未找到资源包",
                        }
                    ),
                )
                return

            file_name = (
                params.get("filename") or Path(zip_path).name or params.get("version")
            )
            print(f"正在更新：{file_name}")
            _target_path = (
                target_path
                if target_path.split("/")[-1] == file_name
                else f"{target_path}/{file_name}"
            )
            target_dir = Path(_target_path)
            print(f"目标目录：{target_dir}")

            # 发送开始更新通知
            self.mqtt_manager.safe_publish(
                device_detail["MSG_UP_TOPIC"],
                json.dumps({"type": "OTA", "status": "start update"}),
            )

            if entry_file == "IoTAgent.py":
                # agent本身进行升级
                if device_detail["condaEnv"]:
                    conda_path = get_conda_executable_path()
                    if not conda_path:
                        self.mqtt_manager.safe_publish(
                            device_detail["MSG_UP_TOPIC"],
                            json.dumps(
                                {
                                    "type": "OTA",
                                    "status": "update failed",
                                    "error": "conda环境未找到",
                                }
                            ),
                        )
                        return
                    subprocess.Popen(
                        [
                            conda_path,
                            "run",
                            "-n",
                            device_detail["condaEnv"],
                            "python",
                            OTA_SELF_FULL_PATH,
                            "--file",  # 参数前缀（可选）
                            f"{AGENT_FILE_PATH}/{zip_path}",  # 实际文件路径
                        ],
                        cwd=target_dir,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )
                else:
                    subprocess.Popen(
                        [
                            "python",
                            OTA_SELF_FULL_PATH,
                            "--file",  # 参数前缀（可选）
                            f"{AGENT_FILE_PATH}/{zip_path}",  # 实际文件路径
                        ],
                        cwd=target_dir,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )
                return

            # 终止旧进程
            self.check_stop_flag(device_detail)
            if not kill_process(device_detail["entryName"]):
                print("没有找到运行的进程")

            time.sleep(2)  # 等待资源释放

            # 备份资源包
            self.check_stop_flag(device_detail)
            self.backup_directory(target_dir)

            self.check_stop_flag(device_detail)
            _archive_handler = archive_handler.ArchiveHandler(
                Path(zip_path), target_dir
            )
            _archive_handler.extract_archive()

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
            self.check_stop_flag(device_detail)
            print(f"正在启动新程序：{target_dir}")
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
            self.mqtt_manager.safe_publish(
                device_detail["MSG_UP_TOPIC"],
                json.dumps(
                    {"type": "OTA", "status": "update success", "version": version_info}
                ),
            )

        except Exception as e:
            error = str(e)
            if error == "终止升级":
                logger.info("终止升级")
                self.mqtt_manager.safe_publish(
                    device_detail["MSG_UP_TOPIC"],
                    json.dumps({"type": "OTA", "status": "update stopped"}),
                )
            else:
                logger.error(f"更新失败: {str(e)}")
                self.mqtt_manager.safe_publish(
                    device_detail["MSG_UP_TOPIC"],
                    json.dumps(
                        {"type": "OTA", "status": "update failed", "error": str(e)}
                    ),
                )

        finally:
            device_detail["stop_flag"] = False
            device_detail["updating"] = False
