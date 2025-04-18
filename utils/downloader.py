import uuid
import requests
import hashlib
from pathlib import Path

class SecureFileDownloader:
    def __init__(self, base_dir="downloads"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
    def download(self, url, save_name=None, expected_md5=None):
        """安全下载文件"""
        try:
            save_path = None  # 初始化 save_path
            # 发送请求
            response = requests.get(
              url,
              stream=True,
              headers={"User-Agent": "SecureDownloader/1.0"},
              timeout=(3.05, 30)
            )
            response.raise_for_status()

            # 确定保存路径
            save_path = self._get_save_path(response, save_name)

            # 分块下载并计算哈希
            md5_hash = hashlib.md5()
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        md5_hash.update(chunk)

            # 校验哈希值
            actual_md5 = md5_hash.hexdigest()
            if expected_md5 and actual_md5 != expected_md5:
                raise ValueError(f"MD5校验失败: {actual_md5} vs {expected_md5}")

            return {
                "status": "success",
                "path": str(save_path),
                "size": save_path.stat().st_size,
                "md5": actual_md5
            }
        except Exception as e:
            if save_path and save_path.exists():
                save_path.unlink()
            return {"status": "error", "message": str(e)}

    def _get_save_path(self, response, save_name):
        """生成安全的保存路径"""
        if save_name:
            return self.base_dir / save_name
            
        # 从header获取文件名
        content_disp = response.headers.get('Content-Disposition', '')
        if 'filename=' in content_disp:
            filename = content_disp.split('filename=')[-1].strip('"')
            return self.base_dir / filename
            
        # 生成随机文件名
        return self.base_dir / f"file_{uuid.uuid4().hex}"

# 使用示例 
if __name__ == "__main__":
    downloader = SecureFileDownloader()
    result = downloader.download(
        "http://localhost:5000/api/packages/download/67f47d1a833d7dc45ad6e530",
        expected_md5="a2f893ba9c9d1f9f4904c2e851552f6b"
    )
    if result["status"] == "success":
        print(f"下载成功：{result['path']}")
    else:
        errMsg = result['message']
        if ("MD5校验失败" in errMsg):
            print("MD5校验失败")
        elif ("Internal Server Error" in errMsg):
            print("接口请求失败")

        print(f"下载失败：{result['message']}")

