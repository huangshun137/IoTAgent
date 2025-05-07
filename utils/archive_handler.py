import logging
from pathlib import Path
import shutil
from typing import Dict, Set
import zipfile
import py7zr
import rarfile

from exceptions import ArchiveError

logger = logging.getLogger(__name__)


class ArchiveHandler:
    def __init__(self, src_path: Path, target_dir: Path):
        self.src_path = src_path
        self.target_dir = target_dir

    def analyze_archive_structure(self, file_path: Path) -> Dict:
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
            if ext == ".zip":
                with zipfile.ZipFile(file_path, "r") as zf:
                    all_files = zf.namelist()

            # RAR格式处理
            elif ext == ".rar":
                with rarfile.RarFile(file_path, "r", charset="gbk") as rf:
                    all_files = [f.filename for f in rf.infolist()]

            # 7Z格式处理
            elif ext in (".7z", ".7zip"):
                with py7zr.SevenZipFile(file_path, "r") as z7:
                    all_files = z7.getnames()

            else:
                raise ArchiveError(f"不支持的压缩格式: {ext}")

            # 统一分析文件结构
            for name in all_files:
                parts = name.replace("\\", "/").split("/")
                if len(parts) > 1 and parts[0]:
                    top_dirs.add(parts[0])
                else:
                    # 处理根目录文件
                    if "." in parts[-1]:  # 判断是否为文件
                        top_dirs.add("root_files")
                    elif parts[0]:
                        top_dirs.add(parts[0])

            return {
                "format": ext[1:],
                "is_single_dir": len(top_dirs) == 1 and "root_files" not in top_dirs,
                "top_dir": next(iter(top_dirs)) if len(top_dirs) == 1 else None,
                "file_count": len(all_files),
                "all_files": all_files,
            }

        except Exception as e:
            raise ArchiveError(f"文件结构分析错误: {str(e)}") from e

    def extract_archive(self):
        """
        安全解压压缩包（支持ZIP/RAR/7Z）
        """
        try:
            # 自动清理已存在目录
            if self.target_dir.exists():
                shutil.rmtree(self.target_dir)
                logger.warning(f"已清理现有目录: {self.target_dir}")

            # 分析压缩包结构
            archive_info = self.analyze_archive_structure(self.src_path)

            # 创建父目录（延迟创建目标目录）
            self.target_dir.parent.mkdir(parents=True, exist_ok=True)

            # ZIP格式解压
            if archive_info["format"] == "zip":
                with zipfile.ZipFile(self.src_path, "r") as zf:
                    if archive_info["is_single_dir"]:
                        zf.extractall(self.target_dir.parent)
                        original_dir = self.target_dir.parent / archive_info["top_dir"]
                        original_dir.rename(self.target_dir)
                    else:
                        zf.extractall(self.target_dir)

            # RAR格式解压
            elif archive_info["format"] == "rar":
                with rarfile.RarFile(self.src_path, "r", charset="gbk") as rf:
                    if archive_info["is_single_dir"]:
                        rf.extractall(self.target_dir.parent)
                        original_dir = self.target_dir.parent / archive_info["top_dir"]
                        original_dir.rename(self.target_dir)
                    else:
                        rf.extractall(self.target_dir)

            # 7Z格式解压
            elif archive_info["format"] in ("7z", "7zip"):
                with py7zr.SevenZipFile(self.src_path, "r") as z7:
                    if archive_info["is_single_dir"]:
                        z7.extractall(self.target_dir.parent)
                        original_dir = self.target_dir.parent / archive_info["top_dir"]
                        original_dir.rename(self.target_dir)
                    else:
                        z7.extractall(self.target_dir)

            logger.info(f"成功解压 {self.src_path.name} 到 {self.target_dir}")

        except Exception as e:
            logger.error(f"解压失败: {str(e)}")
            if self.target_dir.exists():
                shutil.rmtree(self.target_dir)
            raise ArchiveError(f"解压操作失败: {str(e)}") from e
