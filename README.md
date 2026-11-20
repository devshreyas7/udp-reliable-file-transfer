# Reliable UDP File Transfer with SDN Failover (Demo 1 & Demo 2)

This project implements a custom transport-layer reliability protocol over raw UDP sockets (`SOCK_DGRAM`) integrated with a 4-host, 4-switch OpenFlow 1.3 Mininet diamond topology and a Ryu SDN controller.

## Project Structure

```text
udp_reliability_project/
├── packet_protocol.py             # 15-byte header (!BIIHI), CRC32 checksum, SHA-256 helper
├── udp_sender.py                  # Stop-and-Wait (W=1) & Sliding Window Go-Back-N (W>1) sender
├── udp_receiver.py                # Multi-threaded receiver, duplicate/out-of-order handling, validation
├── test_transfer.py               # 8 end-to-end integration tests (normal, loss, GBN, multi-client/server)
├── benchmark_eval.py              # Automated performance evaluation & baseline comparison (CSV + plots)
├── results/
│   ├── benchmark_results.csv      # Quantitative metrics table across loss rates and window sizes
│   └── performance_comparison.png # Bar & line charts comparing baseline vs. proposed approach
└── sdn/
    ├── controller.py              # Ryu OpenFlow 1.3 controller (L4 UDP priority, PortStats, Static/Dynamic)
    ├── topology.py                # 4-Host (h1,h2,h3,h4), 4-Switch (s1..s4) Mininet diamond topology
    ├── test_routes.py             # 5 unit tests for primary, failover, static mode, and port mappings
    └── mininet_demo2_scenarios.py # Prepares test files and prints Demo 2 live Mininet commands
```

## Network Topology (Multi-Client & Multi-Server)

```text
                        [ s2 ] (Primary Path: 5ms hop delay)
                       /      \
  h1 (10.0.0.1) --- [s1]      [s4] --- h2 (10.0.0.2, Server 1)
  h3 (10.0.0.3) ---/    \    /    \--- h4 (10.0.0.4, Server 2)
                        [ s3 ] (Backup Path: 10ms hop delay)
```

- **Clients (Senders):** `h1` (`10.0.0.1`, `s1` port 1) and `h3` (`10.0.0.3`, `s1` port 4)
- **Servers (Receivers):** `h2` (`10.0.0.2`, `s4` port 3) and `h4` (`10.0.0.4`, `s4` port 4)
- **Primary Route:** `s1 -> s2 -> s4`
- **Backup Route:** `s1 -> s3 -> s4` (automatically activated on `EventOFPPortStatus` link failure)

## Quick Verification Commands

```bash
# 1. Run all 8 UDP reliability, Go-Back-N, and multi-client/multi-server integration tests
python3 test_transfer.py

# 2. Run all 5 SDN controller routing & static baseline unit tests
python3 -m unittest -v sdn.test_routes

# 3. Run the automated Demo 2 performance evaluation & generate comparison graphs
python3 benchmark_eval.py
```
