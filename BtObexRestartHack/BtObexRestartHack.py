#!/usr/init/env python3
import subprocess
import sys
import threading
import time
from collections import deque

# Add any number of device MAC address paths here
TARGET_DEVICES = [
    "dev_AA_BB_CC_DD_EE_FF",  # My Car
    "dev_AA_BB_CC_DD_EE_A1", # My another device or car
]

COOLDOWN_SECONDS = 60

# All profiles: everything else first (no pause between them), PBAP Client strictly last
BULK_UUIDS = [
    "0000110b-0000-1000-8000-00805f9b34fb",  # Audio Sink
    "0000110c-0000-1000-8000-00805f9b34fb",  # A/V Remote Control Target
    "0000110d-0000-1000-8000-00805f9b34fb",  # Advanced Audio Distribution
    "0000110e-0000-1000-8000-00805f9b34fb",  # A/V Remote Control
    "0000110f-0000-1000-8000-00805f9b34fb",  # A/V Remote Control Controller
    "0000111e-0000-1000-8000-00805f9b34fb",  # Handsfree
    "0000113b-0000-1000-8000-00805f9b34fb",  # MPS Service
    "00001200-0000-1000-8000-00805f9b34fb",  # PnP Information
    "00001203-0000-1000-8000-00805f9b34fb",  # Generic Audio
    "0000112f-0000-1000-8000-00805f9b34fb",  # Phonebook Access Server
]

LAST_UUID = "0000112e-0000-1000-8000-00805f9b34fb"  # Phonebook Access Client (Always Last)

# Track active loops and thread safety locks
active_threads = {}
threads_lock = threading.Lock()


def active_connect_loop(device_path, stop_event):
    mac_address = device_path.replace("dev_", "").replace("_", ":")
    print(
        f"Starting active connection loop for {mac_address} (6 attempts, bulk sequential, 2s pre-PBAP sleep, 4s loop interval, 10s timeout)...",
        flush=True,
    )

    for attempt in range(1, 7):
        # Check if a newer loop has signaled this one to stop/restart
        if stop_event.is_set():
            print(
                f"Active connection loop for {mac_address} superseded/restarted.",
                flush=True,
            )
            return

        # 1. Connect all other profiles back-to-back without a pause
        for uuid in BULK_UUIDS:
            if stop_event.is_set():
                return
            try:
                subprocess.run(
                    ["bluetoothctl", "connect", mac_address, uuid],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
            except subprocess.TimeoutExpired:
                pass
            except Exception:
                pass

        # 2. Sleep for 2 seconds right before the final Phonebook line
        for _ in range(20):
            if stop_event.is_set():
                return
            time.sleep(0.1)

        # 3. Connect the final Phonebook Access Client profile
        if stop_event.is_set():
            return
        try:
            subprocess.run(
                ["bluetoothctl", "connect", mac_address, LAST_UUID],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            pass

        print(
            f"Connection attempt {attempt}/6 sent for {mac_address} (Profiles + PBAP targeted)",
            flush=True,
        )

        # 4. Sleep 4 seconds between whole loop iterations (in short increments for responsiveness)
        for _ in range(40):
            if stop_event.is_set():
                return
            time.sleep(0.1)

    print(
        f"Finished all 6 connection attempts for {mac_address}.",
        flush=True,
    )


def restart_obex(matched_device):
    print("Restarting Obex...", flush=True)
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", "obex"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        mac_address = matched_device.replace("dev_", "").replace("_", ":")

        with threads_lock:
            # If an active loop is already running for this device, cancel it
            if matched_device in active_threads:
                print(
                    f"Stopping previous active connection loop for {mac_address} to restart from beginning...",
                    flush=True,
                )
                active_threads[matched_device]["stop_event"].set()

            # Create a new stop event and start a fresh loop thread
            stop_event = threading.Event()
            active_threads[matched_device] = {
                "stop_event": stop_event,
                "thread": threading.Thread(
                    target=active_connect_loop,
                    args=(matched_device, stop_event),
                    daemon=True,
                ),
            }
            active_threads[matched_device]["thread"].start()

    except Exception as e:
        print(f"Failed to restart obex: {e}", flush=True)


def main():
    cmd = [
        "dbus-monitor",
        "--system",
        (
            "type='signal',interface='org.freedesktop.DBus.Properties',"
            "member='PropertiesChanged',path_namespace='/org/bluez'"
        ),
    ]

    buffer = deque(maxlen=25)
    last_restart_time = 0

    print(
        "Starting Bluetooth OBEX pro-active watcher for multiple devices...",
        flush=True,
    )

    while True:
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            for line in process.stdout:
                buffer.append(line.strip())
                block = "\n".join(buffer)

                # Find which target device matched
                matched_device = next(
                    (dev for dev in TARGET_DEVICES if dev in block), None
                )

                if (
                    matched_device
                    and "Connected" in block
                    and "boolean true" in block
                ):
                    current_time = time.time()
                    if current_time - last_restart_time > COOLDOWN_SECONDS:
                        restart_obex(matched_device)
                        last_restart_time = time.time()
                    else:
                        print(
                            "Obex restart skipped (cooldown active).",
                            flush=True,
                        )

                    buffer.clear()

            process.wait()
        except Exception as e:
            print(f"Error in monitor loop: {e}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
