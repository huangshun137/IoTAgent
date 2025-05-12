import os
import shutil
from pathlib import Path


def get_conda_executable_path():
    # 检查CONDA_EXE环境变量
    conda_exe = os.environ.get("CONDA_EXE")
    if conda_exe:
        return conda_exe

    # 使用shutil.which查找conda命令路径
    conda_path = shutil.which("conda")
    if conda_path:
        return conda_path

    # 检查常见的conda安装路径
    home_dir = Path.home()
    common_paths = [
        home_dir / "anaconda3" / "condabin" / "conda",
        home_dir / "anaconda3" / "bin" / "conda",
        home_dir / "miniconda3" / "condabin" / "conda",
        home_dir / "miniconda3" / "bin" / "conda",
        # 可根据需要添加其他路径
    ]

    for path in common_paths:
        if path.exists():
            return str(path.resolve())

    return None  # 未找到
