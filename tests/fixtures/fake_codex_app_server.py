#!/usr/bin/python3
import hashlib
import json
import os
import signal
import subprocess
import sys
import time


SCENARIO = os.environ.get("HCOM_FAKE_CODEX_SCENARIO", "happy")
REQUEST_METHOD = os.environ.get(
    "HCOM_FAKE_CODEX_REQUEST_METHOD", "item/tool/requestUserInput"
)
REPORT = os.environ.get("HCOM_FAKE_CODEX_REPORT")
DESCENDANT_PID = os.environ.get("HCOM_FAKE_CODEX_DESCENDANT_PID")
SERVER_PID = os.environ.get("HCOM_FAKE_CODEX_SERVER_PID")

threads = []
turn_number = 0
initialized = False

if SERVER_PID:
    with open(SERVER_PID, "w", encoding="ascii") as output:
        output.write(str(os.getpid()))
        output.flush()

if SCENARIO == "environment" and REPORT:
    native = os.environb
    observation = {
        "method": "fixture/environment",
        "unknown": native.get(b"UNKNOWN_PARENT_VALUE") == b"unknown-value",
        "secretShaped": native.get(b"SERVICE_ACCESS_TOKEN")
        == b"environment-secret-sentinel",
        "empty": native.get(b"EMPTY_PARENT_VALUE") == b"",
        "casePair": (
            native.get(b"CASE_PAIR") == b"upper"
            and native.get(b"case_pair") == b"lower"
        ),
        "nonUtf8": native.get(b"RAW_\xff_NAME") == b"value-\xfe",
        "proxyPair": (
            native.get(b"HTTP_PROXY") == b"http://proxy.example"
            and native.get(b"https_proxy") == b"http://lower-proxy.example"
        ),
    }
    with open(REPORT, "a", encoding="utf-8") as output:
        output.write(json.dumps(observation, separators=(",", ":")) + "\n")
        output.flush()


def emit(value, partial=False):
    encoded = json.dumps(value, separators=(",", ":")).encode("utf-8") + b"\n"
    if partial or SCENARIO == "partial":
        cut_one = max(1, len(encoded) // 3)
        cut_two = max(cut_one + 1, (len(encoded) * 2) // 3)
        for part in (encoded[:cut_one], encoded[cut_one:cut_two], encoded[cut_two:]):
            os.write(sys.stdout.fileno(), part)
    else:
        os.write(sys.stdout.fileno(), encoded)


def record(message):
    if not REPORT:
        return
    method = message.get("method")
    params = message.get("params", {})
    selected = {"method": method, "id": message.get("id")}
    if method == "initialize":
        selected["clientInfo"] = params.get("clientInfo")
        selected["capabilities"] = params.get("capabilities")
    elif method == "thread/start":
        selected.update(
            {
                "cwd": params.get("cwd"),
                "model": params.get("model"),
                "approvalPolicy": params.get("approvalPolicy"),
                "sandbox": params.get("sandbox"),
                "ephemeral": params.get("ephemeral"),
                "config": params.get("config"),
                "instructionsSha256": hashlib.sha256(
                    params.get("developerInstructions", "").encode("utf-8")
                ).hexdigest(),
            }
        )
    elif method == "turn/start":
        inputs = params.get("input", [])
        prompt = inputs[0].get("text", "") if inputs else ""
        selected.update(
            {
                "threadId": params.get("threadId"),
                "cwd": params.get("cwd"),
                "model": params.get("model"),
                "effort": params.get("effort"),
                "approvalPolicy": params.get("approvalPolicy"),
                "sandboxPolicy": params.get("sandboxPolicy"),
                "inputTypes": [item.get("type") for item in inputs],
                "promptBytes": len(prompt.encode("utf-8")),
                "outputSchema": params.get("outputSchema"),
            }
        )
    elif method == "turn/interrupt":
        selected["threadId"] = params.get("threadId")
        selected["turnId"] = params.get("turnId")
    with open(REPORT, "a", encoding="utf-8") as output:
        output.write(json.dumps(selected, separators=(",", ":")) + "\n")
        output.flush()


def thread_payload(thread_id, params):
    return {
        "thread": {
            "id": thread_id,
            "cwd": params["cwd"],
            "ephemeral": True,
            "cliVersion": "0.146.0",
            "createdAt": 1,
            "updatedAt": 1,
            "modelProvider": "openai",
            "preview": "",
            "sessionId": "fixture-session",
            "source": "appServer",
            "status": {"type": "idle"},
            "turns": [],
        },
        "approvalPolicy": params["approvalPolicy"],
        "approvalsReviewer": "user",
        "cwd": params["cwd"],
        "model": params["model"],
        "modelProvider": "openai",
        "sandbox": {"type": "dangerFullAccess"},
    }


def completed(thread_id, turn_id, schema, status="completed"):
    if "status" in schema.get("properties", {}):
        text = json.dumps(
            {"status": "ready", "summary": "fixture complete", "questions": []},
            separators=(",", ":"),
        )
    else:
        text = json.dumps(
            {"verdict": "lgtm", "summary": "fixture sound", "findings": []},
            separators=(",", ":"),
        )
    items = [
        {
            "id": "commentary-1",
            "type": "agentMessage",
            "phase": "commentary",
            "text": "ignored commentary",
        },
        {
            "id": "final-1",
            "type": "agentMessage",
            "phase": "final_answer",
            "text": text,
        },
    ]
    if SCENARIO == "missing_final":
        items = items[:1]
    elif SCENARIO == "ambiguous_final":
        items.append(
            {
                "id": "final-2",
                "type": "agentMessage",
                "text": text,
            }
        )
    elif SCENARIO in ("invalid_outcome", "invalid_outcome_wrong_turn"):
        items[-1]["text"] = '{"status":"ready","summary":"bad","questions":["why"]}'
    event_thread = "wrong-thread" if SCENARIO == "wrong_thread" else thread_id
    event_turn = (
        "wrong-turn"
        if SCENARIO in ("wrong_turn", "invalid_outcome_wrong_turn")
        else turn_id
    )
    if SCENARIO != "summary_only":
        for item in items:
            emit(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": event_thread,
                        "turnId": event_turn,
                        "item": item,
                    },
                }
            )
    if SCENARIO == "canonical_only":
        items_view = "notLoaded"
        summary_items = []
    else:
        items_view = "summary"
        summary_items = [dict(item) for item in items]
    if SCENARIO == "summary_mismatch":
        summary_items[-1]["text"] = '{"status":"ready","summary":"mismatch","questions":[]}'
    if SCENARIO == "invalid_items_view":
        items_view = "future"
    elif SCENARIO == "non_string_items_view":
        items_view = 7
    emit(
        {
            "method": "turn/completed",
            "params": {
                "threadId": event_thread,
                "turn": {
                    "id": event_turn,
                    "status": status,
                    "itemsView": items_view,
                    "items": summary_items,
                },
            },
        }
    )


def emit_exact_line(size):
    prefix = b'{"method":"fixture/unknown","params":{}}'
    if len(prefix) > size:
        raise RuntimeError("line size too small")
    os.write(sys.stdout.fileno(), prefix + b" " * (size - len(prefix)) + b"\n")


def handle_turn(message):
    global turn_number
    params = message["params"]
    turn_number += 1
    if SCENARIO == "sandbox_writable":
        target = os.path.join(params["cwd"], "target")
        os.makedirs(target, exist_ok=True)
        with open(
            os.path.join(target, "sandbox-turn-" + str(turn_number)),
            "w",
            encoding="ascii",
        ) as output:
            output.write("writable\n")
        subprocess.run(
            ["/usr/bin/git", "status", "--short"],
            cwd=params["cwd"],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={
                "HOME": os.environ["HOME"],
                "PATH": os.environ["PATH"],
                "LC_ALL": "C",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": "/dev/null",
            },
        )
        protected = os.environ.get("HCOM_FAKE_PROTECTED_PATH")
        if REPORT and protected:
            controlling_tty = True
            try:
                descriptor = os.open("/dev/tty", os.O_RDWR | os.O_NOCTTY)
            except OSError:
                controlling_tty = False
            else:
                os.close(descriptor)
            with open(REPORT, "a", encoding="utf-8") as output:
                output.write(
                    json.dumps(
                        {
                            "method": "fixture/sandbox",
                            "turn": turn_number,
                            "protectedVisible": os.path.exists(protected),
                            "stdioIsTty": any(os.isatty(fd) for fd in (0, 1, 2)),
                            "controllingTty": controlling_tty,
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                output.flush()
    turn_id = "turn-1" if SCENARIO == "turn_duplicate_id" else "turn-" + str(turn_number)
    response = {
        "id": message["id"],
        "result": {
            "turn": {"id": turn_id, "status": "inProgress", "items": []}
        },
    }
    if SCENARIO == "turn_error":
        emit(
            {
                "id": message["id"],
                "error": {"code": -32001, "message": "TURN_ERROR_SECRET_SENTINEL"},
            }
        )
        return
    if SCENARIO == "turn_missing_id":
        del response["result"]["turn"]["id"]
    if SCENARIO == "stale_response":
        response["id"] = message["id"] + 1
    if SCENARIO == "interleaved":
        emit({"method": "fixture/beforeTurnResponse", "params": {}})
    emit(response)
    if SCENARIO == "duplicate_response":
        emit(response)
        return
    if SCENARIO == "malformed":
        os.write(sys.stdout.fileno(), b"not-json\n")
        return
    if SCENARIO == "malformed_envelope":
        emit({})
        return
    if SCENARIO == "eof":
        sys.exit(0)
    if SCENARIO == "exit_nonzero":
        sys.exit(42)
    if SCENARIO == "server_request":
        emit(
            {
                "id": "server-request-1",
                "method": REQUEST_METHOD,
                "params": {"rawSecret": "SERVER_REQUEST_SECRET_SENTINEL"},
            }
        )
        return
    if SCENARIO == "unknown_notifications_exact":
        for _ in range(4096):
            emit({"method": "fixture/unknown", "params": {}})
    elif SCENARIO == "unknown_notifications_over":
        for _ in range(4097):
            emit({"method": "fixture/unknown", "params": {}})
        return
    elif SCENARIO == "stdout_backpressure":
        raw = "RAW_COMMAND_OUTPUT_SECRET_SENTINEL" + "x" * (64 * 1024)
        for index in range(128):
            item = {
                "id": str(index),
                "type": "commandExecution",
                "rawOutput": raw,
            }
            emit(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": params["threadId"],
                        "turnId": turn_id,
                        "item": item,
                    },
                }
            )
    elif SCENARIO == "stderr_saturation":
        os.write(sys.stderr.fileno(), b"STDERR_SECRET_SENTINEL" + b"x" * (2 * 1024 * 1024))
    elif SCENARIO == "line_exact":
        emit_exact_line(16 * 1024 * 1024)
    elif SCENARIO == "line_below":
        emit_exact_line(16 * 1024 * 1024 - 1)
    elif SCENARIO == "line_oversized":
        emit_exact_line(16 * 1024 * 1024 + 1)
        return
    elif SCENARIO == "pending" or SCENARIO == "descendant":
        if SCENARIO == "descendant":
            descendant = subprocess.Popen(
                ["/bin/sh", "-c", 'trap "" TERM; exec /bin/sleep 300']
            )
            if DESCENDANT_PID:
                with open(DESCENDANT_PID, "w", encoding="ascii") as output:
                    output.write(str(descendant.pid))
                    output.flush()
        return

    status = "completed"
    if SCENARIO == "failed":
        status = "failed"
    elif SCENARIO == "interrupted":
        status = "interrupted"
    completed(params["threadId"], turn_id, params["outputSchema"], status)


for raw_line in sys.stdin.buffer:
    try:
        message = json.loads(raw_line)
    except Exception:
        sys.exit(31)
    record(message)
    method = message.get("method")
    if method == "initialize":
        capabilities = message["params"].get("capabilities", {})
        if "experimentalApi" in capabilities:
            sys.exit(32)
        if SCENARIO == "initialize_timeout":
            continue
        if SCENARIO == "initialize_error":
            emit(
                {
                    "id": message["id"],
                    "error": {
                        "code": -32000,
                        "message": "INITIALIZE_ERROR_SECRET_SENTINEL",
                    },
                }
            )
            continue
        if SCENARIO == "initialize_server_request":
            emit(
                {
                    "id": "initialize-server-request",
                    "method": "item/tool/requestUserInput",
                    "params": {"secret": "INITIALIZE_REQUEST_SECRET_SENTINEL"},
                }
            )
            continue
        if SCENARIO == "interleaved":
            emit({"method": "fixture/beforeInitializeResponse", "params": {}})
        emit(
            {
                "id": message["id"],
                "result": {
                    "codexHome": (
                        "/wrong/codex-home"
                        if SCENARIO == "wrong_codex_home"
                        else os.environ.get("CODEX_HOME", "/fixture/.codex")
                    ),
                    "platformFamily": "unix",
                    "platformOs": "linux",
                    "userAgent": "codex-cli/0.146.0",
                },
            }
        )
    elif method == "initialized":
        initialized = True
    elif method == "thread/start":
        if not initialized:
            sys.exit(33)
        if SCENARIO == "thread_error":
            emit(
                {
                    "id": message["id"],
                    "error": {
                        "code": -32002,
                        "message": "THREAD_ERROR_SECRET_SENTINEL",
                    },
                }
            )
            continue
        thread_id = (
            "thread-1"
            if SCENARIO == "thread_duplicate_id"
            else "thread-" + str(len(threads) + 1)
        )
        threads.append(thread_id)
        payload = thread_payload(thread_id, message["params"])
        if SCENARIO == "thread_missing_id":
            del payload["thread"]["id"]
        if SCENARIO == "interleaved":
            emit({"method": "fixture/beforeThreadResponse", "params": {}})
        emit({"id": message["id"], "result": payload})
    elif method == "turn/start":
        handle_turn(message)
    elif method == "turn/interrupt":
        if SCENARIO not in ("pending", "descendant"):
            emit({"id": message["id"], "result": {}})
    else:
        sys.exit(34)
