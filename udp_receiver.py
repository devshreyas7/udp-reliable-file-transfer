#!/usr/bin/env python3
"""
udp_receiver.py
Concurrent Multi-Client / Multi-Server Reliable UDP File Receiver
Author: Shreyas & Nayak

Features Implemented for Demo 1 & Demo 2:
  1. Multi-client concurrency using per-client worker threads (ClientSessionWorker).
  2. Multi-server support (can bind on multiple hosts h2, h4 or multiple ports 5005, 5006).
  3. Duplicate packet filtering (seq < expected) and Out-of-Order / Missing packet detection (seq > expected).
  4. End-to-end file size and SHA-256 verification before sending Final ACK.
"""

import os
import re
import queue
import socket
import argparse
import threading

from packet_protocol import (
    UDP_PORT, MAX_PACKET_SIZE,
    PKT_SYN, PKT_DATA, PKT_ACK, PKT_FIN,
    pack_packet, unpack_packet, compute_file_sha256,
)

FIN_ACK_GRACE_SECONDS = 12.0


class ClientSessionWorker(threading.Thread):
    """Handle one sender's packets, reconstruction, ACKs, and file validation."""

    def __init__(self, client_addr, sock, output_dir, drop_first_fin_ack=False, prefix_client_port=False, ack_delay_s=0.0):
        super().__init__(daemon=True)
        self.client_addr = client_addr
        self.sock = sock
        self.output_dir = output_dir
        self.pkt_queue = queue.Queue()
        self.prefix_client_port = prefix_client_port
        self.ack_delay_s = ack_delay_s

        self.expected_seq = 0
        self.filename = f"received_{client_addr[0]}_{client_addr[1]}.bin"
        self.expected_size = None
        self.expected_hash = ""
        self.file_handle = None
        self.output_path = ""
        self.duplicates_dropped = 0
        self.out_of_order_dropped = 0
        self.corrupted_dropped = 0
        self.active = True

        self.completed = False
        self.completed_ok = False
        self.completed_fin_seq = None
        self.drop_first_fin_ack = drop_first_fin_ack

    def send_ack(self, ack_num: int):
        if getattr(self, "ack_delay_s", 0.0) > 0:
            import time as _t; _t.sleep(self.ack_delay_s)
        self.sock.sendto(pack_packet(PKT_ACK, seq_num=0, ack_num=ack_num), self.client_addr)

    def _close_file(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

    def _handle_syn(self, seq_num, payload):
        if seq_num != 0:
            return

        if self.expected_seq == 0:
            try:
                metadata = payload.decode("utf-8").split("|", 2)
                if len(metadata) != 3:
                    raise ValueError("expected filename|filesize|sha256")
                raw_filename, raw_size, raw_hash = metadata
                expected_size = int(raw_size)
                if expected_size < 0:
                    raise ValueError("negative file size")
                if not re.fullmatch(r"[0-9a-fA-F]{64}", raw_hash):
                    raise ValueError("invalid SHA-256 digest")
            except (UnicodeDecodeError, ValueError) as exc:
                print(f"[RECEIVER ERROR] Invalid SYN metadata from {self.client_addr}: {exc}")
                return

            safe_filename = os.path.basename(raw_filename.strip()) or "received.bin"
            if self.prefix_client_port:
                self.filename = f"recv_{self.client_addr[0]}_{self.client_addr[1]}_{safe_filename}"
            else:
                self.filename = f"recv_{safe_filename}"
            self.expected_size = expected_size
            self.expected_hash = raw_hash.lower()

            os.makedirs(self.output_dir, exist_ok=True)
            self.output_path = os.path.join(self.output_dir, self.filename)
            self._close_file()
            self.file_handle = open(self.output_path, "wb")
            self.expected_seq = 1
            print(
                f"[RECEIVER] SYN from {self.client_addr}: "
                f"'{self.filename}' ({self.expected_size} bytes)"
            )

        if self.expected_seq > 0:
            self.send_ack(0)

    def _handle_data(self, seq_num, payload):
        if self.expected_seq == 0 or self.file_handle is None:
            print(f"[RECEIVER WARN] DATA before valid SYN from {self.client_addr}; ignored")
            return

        if seq_num == self.expected_seq:
            self.file_handle.write(payload)
            self.send_ack(seq_num)
            self.expected_seq += 1
        elif seq_num < self.expected_seq:
            self.duplicates_dropped += 1
            self.send_ack(seq_num)
            print(f"[RECEIVER] Duplicate DATA {seq_num}; re-ACKed")
        else:
            self.out_of_order_dropped += 1
            print(f"[RECEIVER WARN] Out-of-order DATA seq={seq_num}; expected={self.expected_seq}")
            # Cumulative ACK for the last contiguous in-order packet (triggers Go-Back-N recovery)
            self.send_ack(self.expected_seq - 1)

    def _handle_fin(self, seq_num, payload):
        if self.expected_seq == 0 or self.file_handle is None:
            print(f"[RECEIVER WARN] FIN before valid SYN from {self.client_addr}; ignored")
            return

        if seq_num != self.expected_seq:
            self.out_of_order_dropped += 1
            print(f"[RECEIVER WARN] FIN seq={seq_num}; expected={self.expected_seq}")
            self.send_ack(self.expected_seq - 1)
            return

        self._close_file()
        try:
            actual_size = os.path.getsize(self.output_path)
            actual_hash = compute_file_sha256(self.output_path).lower()
            fin_hash = payload.decode("ascii").lower()
        except (OSError, UnicodeDecodeError):
            actual_size = -1
            actual_hash = ""
            fin_hash = ""

        size_match = actual_size == self.expected_size
        hash_match = (
            bool(self.expected_hash)
            and actual_hash == self.expected_hash
            and fin_hash == self.expected_hash
        )
        self.completed = True
        self.completed_fin_seq = seq_num
        self.completed_ok = size_match and hash_match

        print("============================================================")
        print(f"[UDP RECEIVER] Transfer validation for {self.client_addr}")
        print(f"  - Saved File         : {self.output_path}")
        print(f"  - Expected Size      : {self.expected_size} bytes")
        print(f"  - Actual Size        : {actual_size} bytes")
        print(f"  - Duplicates Dropped : {self.duplicates_dropped}")
        print(f"  - Out-of-Order Seen  : {self.out_of_order_dropped}")
        print(f"  - Corrupted Dropped  : {self.corrupted_dropped}")
        print(f"  - Size Check         : {'PASS' if size_match else 'FAIL'}")
        print(f"  - SHA-256 Check      : {'PASS' if hash_match else 'FAIL'}")

        if not self.completed_ok:
            print("  - Result             : REJECTED; final ACK NOT sent")
            try:
                os.remove(self.output_path)
            except OSError:
                pass
            print("============================================================")
            return

        print("  - Result             : VERIFIED")
        print("============================================================")

        if self.drop_first_fin_ack:
            self.drop_first_fin_ack = False
            print("[RECEIVER TEST] Deliberately dropped first FIN ACK")
        else:
            self.send_ack(seq_num)
            print(f"[RECEIVER] Final ACK {seq_num} sent")

    def run(self):
        print(f"[RECEIVER THREAD] Started session handler for {self.client_addr}")
        try:
            while self.active:
                timeout = FIN_ACK_GRACE_SECONDS if self.completed else 30.0
                try:
                    raw_bytes = self.pkt_queue.get(timeout=timeout)
                except queue.Empty:
                    if self.completed:
                        print(f"[RECEIVER] FIN ACK grace period ended for {self.client_addr}")
                    else:
                        print(f"[RECEIVER THREAD] Session timed out for {self.client_addr}")
                    break

                is_valid, pkt_type, seq_num, _, payload = unpack_packet(raw_bytes)
                if not is_valid:
                    self.corrupted_dropped += 1
                    print(f"[RECEIVER WARN] Dropped corrupted packet from {self.client_addr} (CRC mismatch)")
                    continue

                if self.completed:
                    if self.completed_ok and pkt_type == PKT_FIN and seq_num == self.completed_fin_seq:
                        self.send_ack(seq_num)
                        print(f"[RECEIVER] Duplicate FIN {seq_num}; final ACK resent")
                    elif pkt_type == PKT_SYN and seq_num == 0:
                        print(f"[RECEIVER] New SYN from completed client {self.client_addr}; ending old session")
                        break
                    continue

                if pkt_type == PKT_SYN:
                    self._handle_syn(seq_num, payload)
                elif pkt_type == PKT_DATA:
                    self._handle_data(seq_num, payload)
                elif pkt_type == PKT_FIN:
                    self._handle_fin(seq_num, payload)
        finally:
            self._close_file()
            self.active = False


class ReliableUDPReceiverServer:
    def __init__(
        self,
        bind_ip: str = "0.0.0.0",
        port: int = UDP_PORT,
        output_dir: str = "./received_files",
        drop_first_fin_ack: bool = False,
        expected_clients: int = 1,
        ack_delay_s: float = 0.0,
    ):
        self.bind_addr = (bind_ip, port)
        self.output_dir = output_dir
        self.drop_first_fin_ack = drop_first_fin_ack
        self.expected_clients = max(1, int(expected_clients))
        self.ack_delay_s = float(ack_delay_s)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.bind_addr)
        self.sessions = {}
        self.completed_sessions_count = 0

    def start(self, single_transfer: bool = False):
        print("============================================================")
        print(f"[UDP RECEIVER] Listening on {self.bind_addr[0]}:{self.bind_addr[1]} ...")
        print(f"[UDP RECEIVER] Saving incoming files to: {self.output_dir}")
        print("============================================================", flush=True)
        if single_transfer:
            self.sock.settimeout(1.0)

        try:
            while True:
                try:
                    raw_bytes, client_addr = self.sock.recvfrom(MAX_PACKET_SIZE)
                except socket.timeout:
                    if single_transfer:
                        finished_workers = [
                            w for w in self.sessions.values() if w.completed and not w.active
                        ]
                        if len(finished_workers) >= self.expected_clients:
                            break
                    continue

                is_valid, pkt_type, seq_num, _, _ = unpack_packet(raw_bytes)
                worker = self.sessions.get(client_addr)

                if worker is not None and not worker.active and not single_transfer:
                    del self.sessions[client_addr]
                    worker = None

                if worker is not None and worker.completed and is_valid and pkt_type == PKT_SYN and seq_num == 0:
                    del self.sessions[client_addr]
                    worker = None

                if worker is None:
                    worker = ClientSessionWorker(
                        client_addr,
                        self.sock,
                        self.output_dir,
                        drop_first_fin_ack=self.drop_first_fin_ack,
                        ack_delay_s=self.ack_delay_s,
                    )
                    self.sessions[client_addr] = worker
                    worker.start()

                worker.pkt_queue.put(raw_bytes)
        except KeyboardInterrupt:
            print("\n[UDP RECEIVER] Shutting down server.")
        finally:
            for worker in self.sessions.values():
                worker.active = False
            self.sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reliable UDP File Receiver (Multi-Client / Multi-Server)")
    parser.add_argument("--ip", default="0.0.0.0", help="IP address to bind to")
    parser.add_argument("--port", type=int, default=UDP_PORT, help=f"UDP port (default: {UDP_PORT})")
    parser.add_argument("--out", default="./received_files", help="Output directory for reconstructed files")
    parser.add_argument("--once", action="store_true", help="Exit after expected client transfer(s) finish")
    parser.add_argument("--expected-clients", type=int, default=1, help="Number of concurrent client transfers to wait for in --once mode")
    parser.add_argument("--drop-first-fin-ack", action="store_true", help="TEST ONLY: drop the first final ACK")
    parser.add_argument("--ack-delay-ms", type=float, default=0.0, help="Optional simulated propagation delay in ms")
    args = parser.parse_args()

    server = ReliableUDPReceiverServer(
        args.ip,
        args.port,
        args.out,
        drop_first_fin_ack=args.drop_first_fin_ack,
        expected_clients=args.expected_clients,
        ack_delay_s=args.ack_delay_ms / 1000.0,
    )
    server.start(single_transfer=args.once)
