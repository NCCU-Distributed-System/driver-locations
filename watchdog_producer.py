import subprocess
import time
import datetime
import os
import sys
import signal
import driver_app_producer

SCRIPT_NAME = "driver_app_producer.py"
RESTART_DELAY = 2  # seconds
MAX_RESTARTS = 3  # optional limit to avoid infinite loop

def log(msg):
    timestamp = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {msg}")

def main():
    restart_count = 0
    while restart_count < MAX_RESTARTS:
        log(f"Starting {SCRIPT_NAME} (attempt #{restart_count + 1})")
        try:
            # 啟動 producer subprocess
            process = subprocess.Popen(["python", "driver_app_producer.py"])

            # 等待它結束，若正常結束會回傳 0
            exit_code = process.wait()

            if exit_code == 0:
                log(f"{SCRIPT_NAME} exited normally with code {exit_code}. Exiting watchdog.")
                break
            else:
                log(f"{SCRIPT_NAME} crashed with exit code {exit_code}. Restarting in {RESTART_DELAY}s...")
        except Exception as e:
            log(f"Exception occurred: {e}. Restarting in {RESTART_DELAY}s...")

        restart_count += 1
        time.sleep(RESTART_DELAY)

    log("Max restarts reached or exited cleanly. Watchdog stopped.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Watchdog terminated by user.")
