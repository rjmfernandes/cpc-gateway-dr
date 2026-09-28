#!/usr/bin/env python3

import json
import re
import subprocess

from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PORT = 8080

DASHBOARD_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DASHBOARD_DIR.parent

GATEWAY_CONFIG = PROJECT_ROOT / "gateway-compose.local.yaml"

LINK = "source-to-destination"
TOPIC = "test-topic"


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def run(command):

    try:

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=5,
            cwd=PROJECT_ROOT
        )

        if result.returncode != 0:
            return ""

        return result.stdout.strip()

    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Docker status
# ---------------------------------------------------------------------------

def container_running(name):

    output = run(
        "docker inspect "
        "-f '{{.State.Running}}' "
        f"{name} 2>/dev/null"
    )

    return output == "true"


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------

def get_gateway_target():

    if not GATEWAY_CONFIG.exists():
        return "unknown"

    try:

        config = GATEWAY_CONFIG.read_text(
            encoding="utf-8"
        )

        routes_section = config.split(
            "routes:",
            1
        )

        if len(routes_section) < 2:
            return "unknown"

        route_config = routes_section[1]

        if "name: kafka2-domain" in route_config:
            return "kafka2-domain"

        if "name: kafka1-domain" in route_config:
            return "kafka1-domain"

        return "unknown"

    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Mirror information
# ---------------------------------------------------------------------------

def mirror_description(
    container,
    bootstrap
):

    return run(
        f"docker exec {container} "
        "kafka-mirrors "
        f"--bootstrap-server {bootstrap} "
        "--describe "
        f"--topics {TOPIC} "
        f"--links {LINK} "
        "2>/dev/null"
    )


def parse_mirror_state(output):

    if not output:
        return None

    # Typical kafka-mirrors output contains:
    #
    # Partition: 0  State: ACTIVE ...
    #
    # There may be more than one partition, so collect all
    # partition states.

    states = re.findall(
        r"\bState:\s*'?([A-Za-z_]+)'?",
        output,
        re.IGNORECASE
    )

    if not states:
        return None

    states = [
        state.upper()
        for state in states
    ]

    # If every partition is ACTIVE, the mirror is ACTIVE.

    if all(
        state == "ACTIVE"
        for state in states
    ):
        return "ACTIVE"

    # If every partition reports the same state, return it.

    if len(set(states)) == 1:
        return states[0]

    # Partitions are in different states.

    return "TRANSITIONING"


def parse_mirror_lag(output):

    if not output:
        return None

    matches = re.findall(
        r"\bLag:\s*(\d+)",
        output,
        re.IGNORECASE
    )

    if not matches:
        return None

    # Show total mirror lag across partitions.

    return sum(
        int(value)
        for value in matches
    )


def get_mirror_info():

    # -------------------------------------------------------
    # Normal operation
    #
    # Kafka 1 SOURCE  ----------------->  Kafka 2 MIRROR
    #
    # Therefore the mirror topic lives on Kafka 2.
    # -------------------------------------------------------

    if container_running("kafka-2"):

        output = mirror_description(
            "kafka-2",
            "kafka-2:22222"
        )

        state = parse_mirror_state(output)

        if state:

            return {
                "state": state,
                "lag": parse_mirror_lag(output),
                "location": "kafka2",
                "direction": "kafka1-to-kafka2"
            }


    # -------------------------------------------------------
    # Failover / recovery
    #
    # After Kafka 2 becomes authoritative and Kafka 1 is
    # restored with truncate-and-restore:
    #
    # Kafka 2 SOURCE  ----------------->  Kafka 1 MIRROR
    #
    # The mirror topic therefore lives on Kafka 1.
    # -------------------------------------------------------

    if container_running("kafka-1"):

        output = mirror_description(
            "kafka-1",
            "kafka-1:44444"
        )

        state = parse_mirror_state(output)

        if state:

            return {
                "state": state,
                "lag": parse_mirror_lag(output),
                "location": "kafka1",
                "direction": "kafka2-to-kafka1"
            }


    # -------------------------------------------------------
    # No mirror could be identified.
    # -------------------------------------------------------

    return {
        "state": "UNKNOWN",
        "lag": None,
        "location": "unknown",
        "direction": "unknown"
    }


# ---------------------------------------------------------------------------
# API status
# ---------------------------------------------------------------------------

def get_status():

    mirror = get_mirror_info()

    return {

        "kafka1":
            "UP"
            if container_running("kafka-1")
            else "DOWN",

        "kafka2":
            "UP"
            if container_running("kafka-2")
            else "DOWN",

        "gateway":
            "UP"
            if container_running("gateway")
            else "DOWN",

        "gatewayRoute":
            get_gateway_target(),

        "mirrorState":
            mirror["state"],

        "mirrorLag":
            mirror["lag"],

        "mirrorLocation":
            mirror["location"],

        "mirrorDirection":
            mirror["direction"],

        "clientEndpoint":
            "localhost:19092"
    }


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class DashboardHandler(
    SimpleHTTPRequestHandler
):

    def do_GET(self):

        if self.path == "/api/status":

            data = json.dumps(
                get_status()
            ).encode("utf-8")

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8"
            )

            self.send_header(
                "Content-Length",
                str(len(data))
            )

            self.send_header(
                "Cache-Control",
                "no-store, no-cache, must-revalidate"
            )

            self.end_headers()

            self.wfile.write(data)

            return

        return super().do_GET()


    def translate_path(
        self,
        path
    ):

        path = path.split(
            "?",
            1
        )[0]

        path = path.split(
            "#",
            1
        )[0]

        relative_path = path.lstrip("/")

        if not relative_path:
            relative_path = "index.html"

        requested = (
            DASHBOARD_DIR /
            relative_path
        ).resolve()

        try:

            requested.relative_to(
                DASHBOARD_DIR
            )

        except ValueError:

            return str(
                DASHBOARD_DIR /
                "index.html"
            )

        return str(requested)


    def guess_type(
        self,
        path
    ):

        if path.endswith(".html"):

            return (
                "text/html; "
                "charset=utf-8"
            )

        if path.endswith(".js"):

            return (
                "application/javascript; "
                "charset=utf-8"
            )

        if path.endswith(".css"):

            return (
                "text/css; "
                "charset=utf-8"
            )

        return super().guess_type(
            path
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():

    server = HTTPServer(
        ("0.0.0.0", PORT),
        DashboardHandler
    )

    print()
    print("CPC Gateway DR Dashboard")
    print("----------------------------------------")
    print(
        f"Dashboard : http://localhost:{PORT}"
    )
    print(
        f"Project   : {PROJECT_ROOT}"
    )
    print(
        f"Link      : {LINK}"
    )
    print(
        f"Topic     : {TOPIC}"
    )
    print()
    print("Press Ctrl+C to stop.")
    print()

    try:

        server.serve_forever()

    except KeyboardInterrupt:

        print()
        print("Stopping dashboard...")

        server.server_close()


if __name__ == "__main__":
    main()