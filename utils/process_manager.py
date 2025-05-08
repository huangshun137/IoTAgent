import logging
import subprocess
import psutil

logger = logging.getLogger(__name__)


# 终止进程
def kill_process(entryName):
    """终止目标进程"""
    killed = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            # 检查进程命令行是否包含目标脚本
            if any(entryName in cmd for cmd in proc.cmdline()):
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


def find_and_start_app(target_dir, device_detail):
    """查找并启动应用程序"""
    if device_detail["startCommand"]:
        # 使用自定义启动命令
        command_list = device_detail["startCommand"].split()
        try:
            subprocess.Popen(
                command_list,
                # cwd=target_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            print(f"应用程序已启动: {device_detail['startCommand']}")
        except Exception as e:
            logger.error(f"应用程序启动失败: {str(e)}")
    else:
        # 使用正常python启动命令
        # 查找入口文件
        _entry_file = target_dir / device_detail["entryName"]

        if not _entry_file.exists():
            raise FileNotFoundError("未找到入口文件")

        try:
            # 启动应用程序
            order = ["python", str(_entry_file)]
            if device_detail["condaEnv"]:
                order = [
                    "conda",
                    "run",
                    "-n",
                    device_detail["condaEnv"],
                    "python",
                    str(_entry_file),
                ]
            subprocess.Popen(
                order,
                cwd=target_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            # stdout, stderr = process.communicate()
            print(f"应用程序已启动: {_entry_file}")
            # print(stdout)
        except Exception as e:
            logger.error(f"应用程序启动失败: {str(e)}")
            raise f"应用程序启动失败: {str(e)}"
