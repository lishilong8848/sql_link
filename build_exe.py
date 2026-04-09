from __future__ import annotations

import subprocess
import sys
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parent


def main() -> None:
    spec_file = ROOT / "AlarmDbWebConsole.spec"
    build_dir = ROOT / "py_build"
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--console",
        "--name",
        "AlarmDbWebConsole",
        "--distpath",
        str(ROOT / "py_release"),
        "--workpath",
        str(ROOT / "py_build"),
        "--specpath",
        str(ROOT),
        "--hidden-import",
        "pkg_resources.py2_warn",
        "--add-data",
        "python_web\\webapp;webapp",
        "python_web\\server.py",
    ]

    subprocess.run(command, cwd=str(ROOT), check=True)

    if spec_file.exists():
        spec_file.unlink()
    if build_dir.exists():
        shutil.rmtree(build_dir)


if __name__ == "__main__":
    main()
