#!/usr/bin/env python3
"""Forward incoming oFono SMS messages to Matrix and close notifications on success.

The script monitors:
  * the system bus for org.ofono.MessageManager.IncomingMessage
  * the session bus for org.freedesktop.Notifications.Notify

Each SMS is sent to Matrix up to MATRIX_MAX_ATTEMPTS times. The delay after a
failed attempt is 1, 2, 4, 8, ... seconds. Its Sailfish SMS notification is
closed only after Matrix confirms a successful HTTP response.
"""

import html
import json
import queue
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque, Dict, Iterator, Optional, TextIO, Tuple

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ACCESS_TOKEN = "yyy"
ROOM_ID = "!zzz:foo.bar"
MATRIX_BASE_URL = "https://foo.bar"

MATRIX_MAX_ATTEMPTS = 10
MATRIX_INITIAL_RETRY_DELAY = 1
MATRIX_HTTP_TIMEOUT = 30
CONTACT_LOOKUP_WRAPPER = "/usr/local/sbin/sailfish-contact-lookup-wrapper"
CONTACT_LOOKUP_TIMEOUT = 10

# A notification normally arrives very close to its IncomingMessage signal.
# Keep unmatched SMS notifications this long so either event may arrive first.
NOTIFICATION_MATCH_TIMEOUT = 30
DEBUG = False

SMS_NOTIFICATION_MARKERS = (
    'string "icon-lock-sms"',
    'string "sms,sms_exists"',
    'string "commhistoryd"',
)


@dataclass(frozen=True)
class SmsMessage:
    sequence: int
    sender: str
    body: str
    received_monotonic: float


@dataclass(frozen=True)
class SmsNotification:
    notification_id: int
    detected_monotonic: float


def log(message: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def decode_dbus_string(value: str) -> str:
    """Decode dbus-monitor's quoted-string escapes without corrupting UTF-8."""
    # Decode only escapes dbus-monitor commonly emits. Using unicode_escape on
    # the full UTF-8 string can corrupt non-ASCII SMS text.
    def replace(match: re.Match[str]) -> str:
        escaped = match.group(1)
        replacements = {
            "n": "\n", "r": "\r", "t": "\t",
            '"': '"', "\\": "\\",
        }
        if escaped in replacements:
            return replacements[escaped]
        if escaped.startswith("x") and len(escaped) == 3:
            try:
                return chr(int(escaped[1:], 16))
            except ValueError:
                pass
        return "\\" + escaped

    return re.sub(r"\\(x[0-9A-Fa-f]{2}|.)", replace, value)


def extract_quoted_string(value: str) -> Optional[str]:
    """Extract one complete, possibly multiline dbus-monitor string value."""
    marker = re.search(r'\bstring\s+', value)
    if not marker:
        return None
    opening_quote = value.find('"', marker.end())
    closing_quote = value.rfind('"')
    if opening_quote < 0 or closing_quote <= opening_quote:
        return None
    if value[closing_quote + 1:].strip():
        return None
    return decode_dbus_string(value[opening_quote + 1:closing_quote])


def iter_dbus_records(stream: TextIO) -> Iterator[str]:
    """Join physical output lines that form a multiline D-Bus string."""
    pending: Optional[str] = None
    for raw_line in stream:
        line = raw_line.rstrip("\n")
        if pending is not None:
            pending += "\n" + line
            if line.rstrip().endswith('"'):
                yield pending
                pending = None
            continue

        if re.search(r'\bstring\s+"', line) and not line.rstrip().endswith('"'):
            pending = line
        else:
            yield line

    if pending is not None:
        yield pending


def start_monitor(command: list[str], name: str) -> subprocess.Popen[str]:
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Could not start {name}: {command[0]} was not found") from exc
    if process.stdout is None:
        process.terminate()
        raise RuntimeError(f"Could not read {name} output")
    return process


def is_phone_number(sender: str) -> bool:
    """Return True for a plausible numeric phone number, not a sender ID."""
    sender = sender.strip()
    if not sender or re.search(r"[^0-9+()./\\\s-]", sender):
        return False
    return len(re.sub(r"\D", "", sender)) >= 3


def lookup_contact_name(sender: str) -> Optional[str]:
    """Resolve a number through the setuid-root contact lookup wrapper."""
    if not is_phone_number(sender):
        return None
    try:
        result = subprocess.run(
            [CONTACT_LOOKUP_WRAPPER, sender],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=CONTACT_LOOKUP_TIMEOUT,
            check=False,
        )
    except FileNotFoundError:
        log(f"Contact lookup wrapper was not found: {CONTACT_LOOKUP_WRAPPER}")
        return None
    except subprocess.TimeoutExpired:
        log(f"Contact lookup timed out for {sender!r}")
        return None
    except OSError as exc:
        log(f"Could not execute contact lookup for {sender!r}: {exc}")
        return None

    name = " ".join(result.stdout.split())
    if result.returncode == 0 and name:
        log(f"Resolved SMS sender {sender!r} as {name!r}")
        return name
    if DEBUG:
        details = result.stderr.strip()
        suffix = f": {details}" if details else ""
        log(f"No contact name found for {sender!r} (lookup exit {result.returncode}){suffix}")
    return None


def send_matrix_message(sender: str, sms_text: str) -> bool:
    contact_name = lookup_contact_name(sender)
    display_sender = f"{contact_name} ({sender})" if contact_name else sender
    # Keep the exact plain-text line breaks and also provide an HTML form.
    # Some Matrix clients collapse newlines in formatted rendering unless they
    # are represented explicitly as <br> elements.
    message = f"From: {display_sender}\n\n{sms_text}"
    formatted_message = html.escape(message).replace("\n", "<br>")
    payload = {
        "msgtype": "m.text",
        "body": message,
        "format": "org.matrix.custom.html",
        "formatted_body": formatted_message,
    }
    room_id = urllib.parse.quote(ROOM_ID, safe="")
    access_token = urllib.parse.quote(ACCESS_TOKEN, safe="")
    url = (
        f"{MATRIX_BASE_URL.rstrip('/')}"
        f"/_matrix/client/r0/rooms/{room_id}/send/m.room.message"
        f"?access_token={access_token}"
    )
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=MATRIX_HTTP_TIMEOUT) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            if 200 <= response.status < 300:
                log(f"Matrix message sent successfully (HTTP {response.status})")
                if DEBUG and response_body:
                    log(f"Matrix response: {response_body}")
                return True
            log(f"Matrix send failed (HTTP {response.status}): {response_body}")
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        log(f"Matrix send failed (HTTP {exc.code}): {response_body}")
    except urllib.error.URLError as exc:
        log(f"Matrix send failed: {exc.reason}")
    except Exception as exc:
        log(f"Matrix send failed: {exc}")
    return False


def send_matrix_with_retries(sms: SmsMessage) -> bool:
    delay = MATRIX_INITIAL_RETRY_DELAY
    for attempt in range(1, MATRIX_MAX_ATTEMPTS + 1):
        log(
            f"SMS #{sms.sequence}: Matrix attempt "
            f"{attempt}/{MATRIX_MAX_ATTEMPTS}"
        )
        if send_matrix_message(sms.sender, sms.body):
            return True
        if attempt < MATRIX_MAX_ATTEMPTS:
            log(f"SMS #{sms.sequence}: retrying in {delay} second(s)")
            time.sleep(delay)
            delay *= 2
    return False


def close_notification(notification_id: int) -> bool:
    command = [
        "gdbus", "call", "--session",
        "--dest", "org.freedesktop.Notifications",
        "--object-path", "/org/freedesktop/Notifications",
        "--method", "org.freedesktop.Notifications.CloseNotification",
        str(notification_id),
    ]
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        log(f"Could not close notification {notification_id}: {exc}")
        return False
    output = result.stdout.strip()
    if result.returncode == 0:
        log(f"Closed SMS notification {notification_id}: {output}")
        return True
    log(
        f"Failed to close SMS notification {notification_id} "
        f"(exit {result.returncode}): {output}"
    )
    return False


def iter_ofono_sms(stream: TextIO) -> Iterator[Tuple[str, str]]:
    """Parse oFono SMS signals, including literal multiline message bodies."""
    in_sms = False
    sms_body: Optional[str] = None
    sms_sender: Optional[str] = None
    property_body: Optional[str] = None
    current_key: Optional[str] = None
    array_depth = 0
    body_property_keys = {"text", "message", "body", "content"}

    for line in iter_dbus_records(stream):
        stripped = line.strip()

        if (
            line.startswith("signal ")
            and "interface=org.ofono.MessageManager" in line
            and "member=IncomingMessage" in line
        ):
            in_sms = True
            sms_body = None
            sms_sender = None
            property_body = None
            current_key = None
            array_depth = 0
            if DEBUG:
                log("IncomingMessage signal detected")
            continue

        if not in_sms:
            continue

        if stripped.startswith("array ["):
            array_depth += 1
            continue

        if stripped == "]":
            if array_depth > 0:
                array_depth -= 1
            if array_depth == 0:
                final_body = sms_body if sms_body is not None else property_body
                if final_body is None:
                    log("Ignoring IncomingMessage without a parseable SMS body")
                else:
                    yield sms_sender or "Unknown", final_body
                in_sms = False
                sms_body = None
                sms_sender = None
                property_body = None
                current_key = None
            continue

        if stripped.startswith('string "'):
            value = extract_quoted_string(stripped)
            if value is None:
                continue
            if array_depth == 0 and sms_body is None:
                sms_body = value
            else:
                current_key = value
            continue

        if stripped.startswith("variant") and "string" in stripped:
            value = extract_quoted_string(stripped)
            key = current_key
            current_key = None
            if key is None or value is None:
                continue
            normalized_key = re.sub(r"[^a-z0-9]", "", key.lower())
            if normalized_key == "sender":
                sms_sender = value
                if DEBUG:
                    log(f"Parsed SMS sender: {sms_sender!r}")
            elif normalized_key in body_property_keys and property_body is None:
                property_body = value


def system_bus_watcher(
    sms_queue: "queue.Queue[SmsMessage]",
    stop_event: threading.Event,
    processes: list[subprocess.Popen[str]],
    process_lock: threading.Lock,
) -> None:
    command = [
        "dbus-monitor", "--system",
        "type='signal',interface='org.ofono.MessageManager',member='IncomingMessage'",
    ]
    try:
        process = start_monitor(command, "oFono SMS monitor")
        with process_lock:
            processes.append(process)
        sequence = 0
        assert process.stdout is not None
        for sender, body in iter_ofono_sms(process.stdout):
            if stop_event.is_set():
                break
            sequence += 1
            sms = SmsMessage(sequence, sender, body, time.monotonic())
            log(f"Incoming SMS #{sequence} from {sender!r}: {body!r}")
            sms_queue.put(sms)
    except Exception as exc:
        log(f"System-bus watcher stopped: {exc}")
        stop_event.set()


def session_notification_watcher(
    notification_queue: "queue.Queue[SmsNotification]",
    stop_event: threading.Event,
    processes: list[subprocess.Popen[str]],
    process_lock: threading.Lock,
) -> None:
    # Do not apply a dbus-monitor match rule here. A method_call-only rule
    # would hide the method_return that contains the notification ID.
    command = ["dbus-monitor", "--session"]
    try:
        process = start_monitor(command, "notification monitor")
        with process_lock:
            processes.append(process)
        assert process.stdout is not None
        stream = iter(process.stdout)

        for raw_line in stream:
            if stop_event.is_set():
                break
            line = raw_line.rstrip("\n")
            if not (
                "interface=org.freedesktop.Notifications" in line
                and "member=Notify" in line
            ):
                continue

            serial_match = re.search(r"\bserial=(\d+)\b", line)
            if not serial_match:
                continue
            serial = serial_match.group(1)
            notify_lines = []

            # Notify's final argument is the int32 expiration timeout.
            for argument_line in stream:
                notify_lines.append(argument_line.rstrip("\n"))
                if re.match(r"^\s*int32\s+", argument_line):
                    break

            notify_data = "\n".join(notify_lines)
            is_sms = all(marker in notify_data for marker in SMS_NOTIFICATION_MARKERS)

            # Find the method return carrying the uint32 notification ID.
            notification_id: Optional[int] = None
            for reply_line in stream:
                if f"reply_serial={serial}" not in reply_line:
                    continue
                try:
                    id_line = next(stream)
                except StopIteration:
                    break
                id_match = re.search(r"\buint32\s+(\d+)\b", id_line)
                if id_match:
                    notification_id = int(id_match.group(1))
                break

            if is_sms and notification_id is not None:
                log(f"Detected SMS notification ID {notification_id}")
                notification_queue.put(
                    SmsNotification(notification_id, time.monotonic())
                )
            elif is_sms:
                log("Detected an SMS notification but could not obtain its ID")
    except Exception as exc:
        log(f"Session-bus notification watcher stopped: {exc}")
        stop_event.set()


def choose_notification(
    sms: SmsMessage,
    notifications: Deque[SmsNotification],
) -> Optional[SmsNotification]:
    # FIFO matching correctly pairs bursts of SMS messages with their Notify
    # calls while permitting the notification to arrive shortly before oFono.
    cutoff = sms.received_monotonic - NOTIFICATION_MATCH_TIMEOUT
    while notifications and notifications[0].detected_monotonic < cutoff:
        stale = notifications.popleft()
        log(f"Discarding stale unmatched SMS notification {stale.notification_id}")
    if notifications:
        return notifications.popleft()
    return None


def worker(
    sms_queue: "queue.Queue[SmsMessage]",
    notification_queue: "queue.Queue[SmsNotification]",
    stop_event: threading.Event,
) -> None:
    saved_notifications: Deque[SmsNotification] = deque()

    while not stop_event.is_set():
        try:
            sms = sms_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        try:
            success = send_matrix_with_retries(sms)
            if not success:
                log(
                    f"SMS #{sms.sequence}: all {MATRIX_MAX_ATTEMPTS} Matrix "
                    "attempts failed; leaving its notification untouched"
                )
                continue

            # Drain notifications collected while Matrix retries were running.
            while True:
                try:
                    saved_notifications.append(notification_queue.get_nowait())
                except queue.Empty:
                    break

            notification = choose_notification(sms, saved_notifications)
            deadline = time.monotonic() + NOTIFICATION_MATCH_TIMEOUT
            while notification is None and not stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    saved_notifications.append(
                        notification_queue.get(timeout=min(remaining, 0.5))
                    )
                except queue.Empty:
                    continue
                notification = choose_notification(sms, saved_notifications)

            if notification is None:
                log(
                    f"SMS #{sms.sequence}: Matrix send succeeded, but no matching "
                    "SMS notification ID was observed; nothing was closed"
                )
            else:
                close_notification(notification.notification_id)
        finally:
            sms_queue.task_done()


def validate_configuration() -> None:
    if not ACCESS_TOKEN or ACCESS_TOKEN.startswith("PUT_"):
        raise ValueError("Configure ACCESS_TOKEN at the beginning of the script")
    if not ROOM_ID or ROOM_ID.startswith("PUT_"):
        raise ValueError("Configure ROOM_ID at the beginning of the script")
    if MATRIX_MAX_ATTEMPTS < 1:
        raise ValueError("MATRIX_MAX_ATTEMPTS must be at least 1")
    if MATRIX_INITIAL_RETRY_DELAY < 0:
        raise ValueError("MATRIX_INITIAL_RETRY_DELAY cannot be negative")
    if CONTACT_LOOKUP_TIMEOUT <= 0:
        raise ValueError("CONTACT_LOOKUP_TIMEOUT must be greater than zero")


def main() -> int:
    try:
        validate_configuration()
    except ValueError as exc:
        log(f"Configuration error: {exc}")
        return 1

    log("Starting combined oFono SMS to Matrix and notification watcher")
    sms_queue: "queue.Queue[SmsMessage]" = queue.Queue()
    notification_queue: "queue.Queue[SmsNotification]" = queue.Queue()
    stop_event = threading.Event()
    processes: list[subprocess.Popen[str]] = []
    process_lock = threading.Lock()

    threads = [
        threading.Thread(
            target=system_bus_watcher,
            args=(sms_queue, stop_event, processes, process_lock),
            name="ofono-sms-watcher",
            daemon=True,
        ),
        threading.Thread(
            target=session_notification_watcher,
            args=(notification_queue, stop_event, processes, process_lock),
            name="notification-watcher",
            daemon=True,
        ),
        threading.Thread(
            target=worker,
            args=(sms_queue, notification_queue, stop_event),
            name="matrix-worker",
            daemon=True,
        ),
    ]

    for thread in threads:
        thread.start()

    try:
        while not stop_event.wait(1):
            if not all(thread.is_alive() for thread in threads):
                log("A required worker thread stopped unexpectedly")
                stop_event.set()
    except KeyboardInterrupt:
        log("Stopping on keyboard interrupt")
        stop_event.set()
    finally:
        with process_lock:
            running_processes = list(processes)
        for process in running_processes:
            if process.poll() is None:
                process.terminate()
        for process in running_processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        for thread in threads:
            thread.join(timeout=2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
