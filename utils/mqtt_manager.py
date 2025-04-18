# mqtt_manager.py
import logging
import threading
import time
import paho.mqtt.client as mqtt

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MQTTManager")

# 自定义异常类
class ConnectionFailedError(Exception):
    """MQTT连接异常"""

class MQTTManager:
    _instances = {}
    _lock = threading.Lock()  # 类级线程锁
    _DEFAULT_RETRIES = 3      # 默认重试次数
    _DEFAULT_DELAY = 1        # 默认重试间隔(秒)

    def __new__(cls, host: str, port: int):
        """线程安全的多例模式实现"""
        instance_key = (host, port)
        
        with cls._lock:  # 加锁保证线程安全
            if instance_key not in cls._instances:
                new_instance = super().__new__(cls)
                new_instance._initialized = False
                cls._instances[instance_key] = new_instance
                
        return cls._instances[instance_key]

    def __init__(self, host: str, port: int):
        """初始化连接（带重试机制）"""
        if self._initialized:
            return

        self.host = host
        self.port = port
        self._connect_attempts = 0
        self._reconnect_enabled = True
        self._last_ping = 0

        # 创建客户端
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        
        # 带重试的连接
        self._connect_with_retry(
            retries=self._DEFAULT_RETRIES,
            delay=self._DEFAULT_DELAY
        )
        
        self.client.loop_start()
        self._initialized = True

    def _connect_with_retry(self, retries: int, delay: float):
        """带指数退避的重试连接"""
        for attempt in range(1, retries + 1):
            try:
                self.client.connect(self.host, self.port, keepalive=60)
                # logger.info(f"Connected to {self.host}:{self.port}")
                return
            except (ConnectionRefusedError, OSError, TimeoutError) as e:
                if attempt == retries:
                    logger.error(f"Connection failed after {retries} attempts")
                    raise ConnectionFailedError(f"Failed to connect: {str(e)}") from e
                
                sleep_time = delay * (2 ** (attempt - 1))
                logger.warning(f"Connection attempt {attempt} failed, retrying in {sleep_time}s...")
                time.sleep(sleep_time)

    def _on_connect(self, client, userdata, flags, rc):
        """连接成功回调"""
        if rc == 0:
            logger.info(f"Connected to {self.host}:{self.port}")
        else:
            logger.error(f"Connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """断开连接回调"""
        logger.warning(f"Disconnected from {self.host}:{self.port} (code: {rc})")
        if self._reconnect_enabled:
            self._auto_reconnect()

    def _auto_reconnect(self, max_attempts: int = 5):
        """自动重连机制"""
        logger.info("Attempting automatic reconnect...")
        for attempt in range(1, max_attempts + 1):
            try:
                self.client.reconnect()
                logger.info("Reconnect successful")
                return
            except Exception as e:
                logger.error(f"Reconnect attempt {attempt} failed: {str(e)}")
                time.sleep(2 ** attempt)
        logger.error("Auto reconnect failed after maximum attempts")

    def check_connection(self, timeout: float = 1.0) -> bool:
        """验证连接状态（带主动PING）"""
        # 基础状态检查
        if not self.client.is_connected():
            return False
        
        # 主动发送PING（需要1.6.1+版本）
        try:
            self.client.ping()
            self._last_ping = time.time()
            # 等待PING响应
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.client._state == mqtt.mqtt_cs_connected:
                    return True
                time.sleep(0.1)
            return False
        except AttributeError:
            # 兼容旧版本
            return self.client.is_connected()

    def safe_publish(self, topic: str, payload, **kwargs):
        """带异常处理的发布方法"""
        try:
            if self.check_connection():
                return self.client.publish(topic, payload, **kwargs)
            else:
                logger.error("Cannot publish - connection is down")
                return False
        except Exception as e:
            logger.error(f"Publish failed: {str(e)}")
            raise

    def stop(self):
        """停止客户端"""
        self._reconnect_enabled = False
        print("MQTTManager instance is being deleted")
        self._reconnect_enabled = False
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except:
            pass

# 使用示例 ################################

if __name__ == "__main__":
    # 测试多线程安全
    def create_client():
        m = MQTTManager("localhost", 1883)
        print(f"Client connected: {m.check_connection()}")

    threads = []
    for _ in range(5):
        t = threading.Thread(target=create_client)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()

    # 测试异常处理
    try:
        bad_client = MQTTManager("invalid.host", 1883)
    except ConnectionFailedError as e:
        logger.error(f"Caught expected exception: {e}")