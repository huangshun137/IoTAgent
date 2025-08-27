import sys
import argparse
import time
import logging
import shutil
import subprocess
from pathlib import Path
import paho.mqtt.client as mqtt
import json
import zipfile
import rarfile
from getmac import get_mac_address

# 配置常量
PRODUCT_AGENT_ID = "681ac31f6cc0a3de12b5020a"
MAIN_AGENT_NAME = "IoTAgent.py"
CURRENT_AGENT_DIR = "/home/rm/Jett/IoTAgent"  # 当前Agent安装目录
TEMP_DOWNLOAD_DIR = "/home/rm/Jett/tmp/agent_upgrade"  # 临时下载目录
BACKUP_DIR = "/home/rm/Jett/agent_backup"  # 旧版本备份目录

# Supervisor配置
SUPERVISOR_SERVICE_NAME = "IoTAgent"  # supervisor配置中的服务名

# ================== MQTT配置 ==================
MQTT_CONFIG = {
    "host": "39.105.185.216",  # MQTT代理地址
    "port": 1883,  # 端口
    "keepalive": 60,  # 心跳间隔
    "topic": f"/devices/{PRODUCT_AGENT_ID}_{get_mac_address(interface='eth0')}_agent/sys/messages/up",  # 主题模板
    "qos": 1,  # 服务质量等级
    # "retain": False,                  # 是否保留消息
    # "credentials": {                  # 认证信息
    #     "username": "device_user",
    #     "password": "secure_password"
    # },
    # "tls": None,                      # TLS配置（如果需要）
    # "device_id": "DEVICE_123456"      # 设备唯一标识
}


# 初始化日志
logger = logging.getLogger(__name__)


class MQTTNotifier:
    """MQTT通知封装类"""

    def __init__(self, config):
        self.config = config
        self.client = None
        self.connected = False

        # 初始化客户端
        self.client = mqtt.Client()
        # self.client.username_pw_set(
        #     self.config["credentials"]["username"],
        #     self.config["credentials"]["password"],
        # )

        # 配置TLS
        # if self.config["tls"]:
        #     self.client.tls_set(**self.config["tls"])

        # 设置回调
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        """连接回调"""
        if rc == 0:
            self.connected = True
            print("MQTT连接成功")
        else:
            logger.error(f"MQTT连接失败，错误码: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """断开连接回调"""
        self.connected = False
        if rc != 0:
            logger.warning(f"MQTT异常断开，错误码: {rc}")

    def connect(self):
        """建立连接"""
        try:
            self.client.connect(
                self.config["host"],
                port=self.config["port"],
                keepalive=self.config["keepalive"],
            )
            self.client.loop_start()  # 启动后台线程
            # 等待连接建立
            for _ in range(5):  # 最大等待5秒
                if self.connected:
                    return True
                time.sleep(1)
            return False
        except Exception as e:
            logger.error(f"MQTT连接异常: {str(e)}")
            return False

    def publish_status(self, status, error=None):
        """发布状态消息"""
        if not self.connected:
            logger.warning("MQTT未连接，无法发送状态")
            return False

        topic = self.config["topic"]

        payload = {
            "type": "OTA",
            "status": status,
        }
        if error:  # 如果指定了版本，则添加到消息中
            payload["error"] = error

        try:
            result = self.client.publish(
                topic,
                payload=json.dumps(payload),
                qos=self.config["qos"],
            )
            print("发送mqtt消息：", topic, payload)
            # 确保消息发送完成
            result.wait_for_publish(timeout=5)
            return result.is_published()
        except Exception as e:
            logger.error(f"消息发布失败: {str(e)}")
            return False

    def disconnect(self):
        """断开连接"""
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()


class UpgradeFailed(Exception):
    """自定义升级异常"""

    pass


def supervisor_command(cmd: str, timeout=10) -> str:
    """执行supervisorctl命令并返回输出"""
    try:
        result = subprocess.run(
            ["sudo", "supervisorctl", cmd, SUPERVISOR_SERVICE_NAME],
            check=True,
            timeout=timeout,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Supervisor command failed: {e}")
        raise UpgradeFailed(f"Supervisor error: {e}")
    except subprocess.TimeoutExpired:
        raise UpgradeFailed("Supervisor command timed out")


def stop_service():
    """安全停止Supervisor服务"""
    try:
        status_output = supervisor_command("status")
        if "running" in status_output.lower():
            logger.info("Stopping supervisor service...")
            output = supervisor_command("stop")
            if "stopped" not in output.lower():
                raise UpgradeFailed(f"Stop failed: {output}")
        else:
            logger.info("Service is not running, skip stopping")
    except subprocess.CalledProcessError as e:
        if "not running" in str(e).lower():
            logger.warning("Attempted to stop a non-running service")
        else:
            raise


def start_service():
    """启动Supervisor服务"""
    print("Starting supervisor service...")
    output = supervisor_command("start")

    # 验证启动是否成功
    if "started" not in output.lower():
        raise UpgradeFailed(f"Start failed: {output}")


def check_service_status(retries=5, interval=3) -> bool:
    """检查服务是否正常运行"""
    for _ in range(retries):
        try:
            status = supervisor_command("status")
            # 期望输出示例：iot_agent                RUNNING   pid 1234, uptime 0:00:05
            if "running" in status.lower():
                return True
            time.sleep(interval)
        except UpgradeFailed:
            pass  # 可能处于中间状态
    return False


def create_backup():
    """创建备份（独立步骤）"""
    try:
        backup_path = Path(BACKUP_DIR)

        # 清理旧备份
        if backup_path.exists():
            shutil.rmtree(backup_path, ignore_errors=True)

        # 创建新备份
        print(f"Creating backup from {CURRENT_AGENT_DIR} to {BACKUP_DIR}")
        shutil.copytree(
            CURRENT_AGENT_DIR,
            BACKUP_DIR,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("*.tmp"),
        )
        print("Backup created successfully")
    except Exception as e:
        logger.error(f"备份创建失败: {str(e)}")
        raise UpgradeFailed("无法创建备份，终止升级")


def get_real_source_dir() -> Path:
    """自动检测解压后的真实目录"""
    temp_dir = Path(TEMP_DOWNLOAD_DIR)

    # 情况1：直接包含程序文件
    if (temp_dir / MAIN_AGENT_NAME).exists():
        return temp_dir

    # 情况2：包含单一子目录
    subdirs = [d for d in temp_dir.iterdir() if d.is_dir()]
    if len(subdirs) == 1:
        candidate = subdirs[0]
        if (candidate / MAIN_AGENT_NAME).exists():
            return candidate

    raise UpgradeFailed("无法识别解压后的目录结构")


def replace_files():
    """安全替换文件（处理目录层级）"""
    try:
        source_dir = get_real_source_dir()
        target_dir = Path(CURRENT_AGENT_DIR)

        # 清空目标目录
        print(f"清理目标目录: {target_dir}")
        shutil.rmtree(target_dir, ignore_errors=True)
        target_dir.mkdir(parents=True)

        # 复制内容
        print(f"从 {source_dir} 复制到 {target_dir}")
        for item in source_dir.iterdir():
            dest = target_dir / item.name
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)

        print("文件替换成功")
    except Exception as e:
        logger.error(f"文件替换失败: {str(e)}")
        raise UpgradeFailed("文件替换失败")


def perform_rollback() -> None:
    """执行回滚操作"""
    try:
        logger.warning("Starting rollback...")

        backup_path = Path(BACKUP_DIR)
        current_path = Path(CURRENT_AGENT_DIR)

        # 检查备份是否存在
        if not backup_path.exists():
            logger.critical("回滚失败：备份目录不存在")
            logger.critical("请手动从以下位置恢复：")
            logger.critical(f"备份应存在于: {BACKUP_DIR}")
            raise UpgradeFailed("无法回滚：备份丢失")

        # 删除当前损坏的文件
        if current_path.exists():
            logger.info("Removing corrupted files...")
            shutil.rmtree(current_path, ignore_errors=True)

        # 恢复备份
        logger.info("Restoring backup...")
        shutil.copytree(backup_path, current_path, dirs_exist_ok=True)

        print("Rollback completed")
    except Exception as e:
        logger.critical(f"Rollback failed! Manual intervention needed: {str(e)}")
        sys.exit(2)


def main(zipPath):
    notifier = MQTTNotifier(MQTT_CONFIG)
    try:
        # 初始化MQTT连接
        if not notifier.connect():
            logging.error("无法连接到MQTT服务器，但继续升级流程")

        notifier.publish_status("start update")
        # 0. 初始化
        Path(TEMP_DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

        # 1. 停止服务
        print("Stopping main process")
        stop_service()

        # 2. 创建备份（独立步骤）
        create_backup()  # 如果失败会直接终止

        # 3. 解压文件
        print(f"解压文件: {zipPath}")
        try:
            if zipPath.lower().endswith(".zip"):
                # 使用zipfile解压ZIP文件
                with zipfile.ZipFile(zipPath, "r") as zf:
                    zf.extractall(TEMP_DOWNLOAD_DIR)
            else:
                # 使用rarfile解压RAR文件
                with rarfile.RarFile(zipPath, "r") as rf:
                    rf.extractall(TEMP_DOWNLOAD_DIR)
        except Exception as e:
            logger.error(f"解压失败: {str(e)}")
            raise UpgradeFailed(f"文件解压失败: {str(e)}")

        # print(f"解压文件: {zipPath}")
        # if zipPath.lower().endswith(".zip"):
        #     subprocess.run(
        #         ["/usr/bin/unzip", "-o", zipPath, "-d", TEMP_DOWNLOAD_DIR],
        #         check=True,
        #         stdout=subprocess.DEVNULL,  # 隐藏解压输出
        #     )
        # else:
        #     subprocess.run(
        #         ["/usr/bin/unrar", "x", zipPath, TEMP_DOWNLOAD_DIR],
        #         check=True,
        #         stdout=subprocess.DEVNULL,
        #     )

        # 4. 替换文件
        replace_files()

        # 5. 启动服务
        start_service()

        # 步骤6: 验证新进程启动
        print("Verifying new process...")
        if not check_service_status():
            raise UpgradeFailed("Service did not enter running state")

        # 步骤7: 安全关闭子进程
        print("New agent confirmed running. Exiting upgrader.")

        notifier.publish_status("update success")

        print("Upgrade completed successfully")

    except Exception as e:
        logger.error(f"Upgrade failed: {str(e)}")
        perform_rollback()
        try:
            start_service()  # 尝试重新启动旧版本
            notifier.publish_status(
                "update failed", "has restart origin agent." + str(e)
            )
        except Exception as restart_error:
            logger.critical(f"Critical failure: {restart_error}")
            notifier.publish_status("update failed", str(e))
        sys.exit(1)

    finally:
        # 清理临时文件
        shutil.rmtree(TEMP_DOWNLOAD_DIR)
        # 确保子进程退出（新增）
        sys.exit(0)  # 明确退出


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="升级文件路径")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    print(f"子进程收到的文件路径: {args.file}")
    main(args.file)
