#!/usr/bin/env python3
"""
sdn/mininet_demo2_scenarios.py
Helper script that prepares sample payloads for Demo 2 and prints the exact
commands for Multi-Client / Multi-Server, Packet Loss Retransmission, and SDN Failover.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def prepare_files():
    f1 = ROOT / "sample_client1.bin"
    f2 = ROOT / "sample_client2.bin"
    if not f1.exists():
        f1.write_bytes(os.urandom(150 * 1024))
    if not f2.exists():
        f2.write_bytes(os.urandom(150 * 1024))
    print(f"[OK] Prepared test files: {f1.name} (150 KB) and {f2.name} (150 KB)")


def print_guide():
    print("""
================================================================================
   DEMO 2 LIVE MININET COMMAND GUIDE (MULTI-CLIENT, MULTI-SERVER & LOSS)
================================================================================

1. START RYU CONTROLLER (Terminal 1):
   source ~/ryu-venv/bin/activate
   cd ~/udp_reliability_project/sdn
   ryu-manager --ofp-tcp-listen-port 6653 controller.py

2. START 4-HOST DIAMOND TOPOLOGY (Terminal 2):
   cd ~/udp_reliability_project/sdn
   sudo mn -c
   sudo python3 topology.py

3. SCENARIO A — MULTI-CLIENT & MULTI-SERVER CONCURRENT TRANSFER:
   (Host h1 -> Server h2 on port 5005, and Host h3 -> Server h4 on port 5006 simultaneously)
   mininet> pingall
   mininet> h2 python3 -u ../udp_receiver.py --ip 0.0.0.0 --port 5005 --out ../received_h2 --once &
   mininet> h4 python3 -u ../udp_receiver.py --ip 0.0.0.0 --port 5006 --out ../received_h4 --once &
   mininet> h1 python3 -u ../udp_sender.py ../sample_client1.bin --ip 10.0.0.2 --port 5005 --window 5 &
   mininet> h3 python3 -u ../udp_sender.py ../sample_client2.bin --ip 10.0.0.4 --port 5006 --window 5

4. SCENARIO B — MULTIPLE CLIENTS TO A SINGLE SERVER CONCURRENTLY:
   (Both Client h1 and Client h3 send simultaneously to Server h2 on port 5005)
   mininet> h2 python3 -u ../udp_receiver.py --ip 0.0.0.0 --port 5005 --out ../received_h2 --once --expected-clients 2 &
   mininet> h1 python3 -u ../udp_sender.py ../sample_client1.bin --ip 10.0.0.2 --port 5005 --window 5 &
   mininet> h3 python3 -u ../udp_sender.py ../sample_client2.bin --ip 10.0.0.2 --port 5005 --window 5

5. SCENARIO C — PACKET LOSS RETRANSMISSION (STOP-AND-WAIT vs. SLIDING WINDOW):
   (Either restart topology with `sudo python3 topology.py --loss 5.0` OR use `--drop-prob 0.05`)
   mininet> h2 python3 -u ../udp_receiver.py --ip 0.0.0.0 --port 5005 --out ../received_h2 --once &
   mininet> h1 python3 -u ../udp_sender.py ../sample_client1.bin --ip 10.0.0.2 --port 5005 --window 8 --drop-prob 0.05

6. SCENARIO D — MID-TRANSFER LINK FAILURE & AUTOMATIC SDN REROUTING:
   mininet> h2 python3 -u ../udp_receiver.py --ip 0.0.0.0 --port 5005 --out ../received_h2 --once &
   mininet> h1 python3 -u ../udp_sender.py ../sample_client1.bin --ip 10.0.0.2 --port 5005 --window 5 &
   mininet> link s1 s2 down
   mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1
   mininet> link s1 s2 up
================================================================================
""")


if __name__ == "__main__":
    prepare_files()
    print_guide()
