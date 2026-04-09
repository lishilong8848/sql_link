import atexit

from python_web.server import log_info, main


if __name__ == "__main__":
    atexit.register(lambda: log_info("Alarm DB Console 已退出。"))
    main()
