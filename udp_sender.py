#!/usr/bin/env python3
"""
udp_sender.py
Reliable UDP File Transfer Sender (Stop-and-Wait & Sliding Window Go-Back-N)
Author: Shreyas

Features Implemented for Demo 1 & Demo 2:
  1. Low-level Python UDP Socket (SOCK_DGRAM) file transmission.
  2. Stop-and-Wait (window=1) and Pipelined Go-Back-N Sliding Window (window>1).
  3. Timeout & Retransmission engine + Fast Duplicate-ACK Go-Back-N recovery.
  4. Multi-server targeting (--ip, --port) and JSON telemetry export (--metrics-out).
"""

import os
import sys
import time
import json
import random
import socket
import select
import argparse
from packet_protocol import (
    UDP_PORT, CHUNK_SIZE, MAX_PACKET_SIZE,
    PKT_SYN, PKT_DATA, PKT_ACK, PKT_FIN,
    pack_packet, unpack_packet, compute_file_sha256
)


class ReliableUDPSender:
    def __init__(
        self,
        receiver_ip: str,
        receiver_port: int = UDP_PORT,
        timeout: float = 0.25,
        max_retries: int = 40,
        window_size: int = 1,
        drop_prob: float = 0.0,
        metrics_out: str = None,
    ):
        self.receiver_addr = (receiver_ip, receiver_port)
        self.timeout = timeout
        self.max_retries = max_retries
        self.window_size = max(1, int(window_size))
        self.drop_prob = float(drop_prob)
        self.metrics_out = metrics_out

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(self.timeout)

        # Telemetry counters
        self.total_sent = 0
        self.retransmissions = 0
        self.window_retransmits = 0

    def _sendto_maybe_drop(self, packet_bytes: bytes, is_data: bool = False):
        """Sends a UDP datagram, optionally simulating wire packet loss for testing."""
        self.total_sent += 1
        if is_data and self.drop_prob > 0.0 and random.random() < self.drop_prob:
            print(f"[SIMULATED LOSS] Dropped outgoing DATA packet on wire (drop_prob={self.drop_prob})")
            return
        self.sock.sendto(packet_bytes, self.receiver_addr)

    def send_reliable_packet(self, pkt_type: int, seq_num: int, payload: bytes = b'') -> bool:
        """Sends a single control/data packet (Stop-and-Wait) and waits for a matching ACK."""
        packet_bytes = pack_packet(pkt_type, seq_num, 0, payload)
        retries = 0

        while retries < self.max_retries:
            try:
                self._sendto_maybe_drop(packet_bytes, is_data=(pkt_type == PKT_DATA))

                while True:
                    raw_ack, _ = self.sock.recvfrom(MAX_PACKET_SIZE)
                    is_valid, rx_type, _, rx_ack_num, _ = unpack_packet(raw_ack)

                    if not is_valid:
                        print(f"[SENDER WARN] Corrupted ACK received for seq={seq_num}, ignoring...")
                        continue

                    if rx_type == PKT_ACK and rx_ack_num == seq_num:
                        return True
                    else:
                        continue

            except socket.timeout:
                retries += 1
                self.retransmissions += 1
                print(f"[TIMEOUT / RETRY #{retries}] No ACK for seq={seq_num}. Retransmitting...")

        print(f"[SENDER ERROR] Max retries ({self.max_retries}) exceeded for seq={seq_num}.")
        return False

    def _send_data_sliding_window(self, chunks: list) -> bool:
        """
        Transmits all file chunks using Go-Back-N (GBN) Sliding Window ARQ.
        Sends up to `window_size` unacknowledged packets in flight and slides the window
        immediately as cumulative ACKs arrive. On timeout, retransmits from `base`.
        """
        total_chunks = len(chunks)
        if total_chunks == 0:
            return True

        base = 1
        next_seq = 1
        retries_on_base = 0
        dup_ack_count = 0
        timer_start = time.time()
        last_progress_logged = 0

        while base <= total_chunks:
            # 1. Pipeline packets within the active sliding window [base, base + window_size - 1]
            while next_seq < base + self.window_size and next_seq <= total_chunks:
                pkt = pack_packet(PKT_DATA, seq_num=next_seq, ack_num=0, payload=chunks[next_seq - 1])
                if next_seq == base:
                    timer_start = time.time()
                self._sendto_maybe_drop(pkt, is_data=True)
                next_seq += 1

            # 2. Wait for incoming ACKs up to the remaining timeout for `base`
            elapsed_since_timer = time.time() - timer_start
            remaining_timeout = max(0.005, self.timeout - elapsed_since_timer)

            fast_retransmit_triggered = False
            ready, _, _ = select.select([self.sock], [], [], remaining_timeout)
            if ready:
                # Drain all currently available ACKs from the socket buffer
                while True:
                    r, _, _ = select.select([self.sock], [], [], 0.0)
                    if not r:
                        break
                    raw_ack, _ = self.sock.recvfrom(MAX_PACKET_SIZE)
                    is_valid, rx_type, _, rx_ack_num, _ = unpack_packet(raw_ack)
                    if not is_valid or rx_type != PKT_ACK:
                        continue

                    if rx_ack_num >= base:
                        # Cumulative ACK advances the window base
                        base = rx_ack_num + 1
                        retries_on_base = 0
                        dup_ack_count = 0
                        timer_start = time.time()

                        acked_so_far = base - 1
                        if (acked_so_far - last_progress_logged >= 25) or (acked_so_far == total_chunks):
                            last_progress_logged = acked_so_far
                            progress = (acked_so_far / max(1, total_chunks)) * 100
                            print(f"[UDP SENDER] Progress: {acked_so_far}/{total_chunks} packets ACKed ({progress:.1f}%)")
                    elif rx_ack_num == base - 1:
                        # Duplicate cumulative ACK from receiver (triggered by out-of-order arrival)
                        dup_ack_count += 1
                        if dup_ack_count >= 2:
                            fast_retransmit_triggered = True
                            break

            # 3. Fast Retransmit (on duplicate ACKs) or Timeout Retransmission
            if base <= total_chunks and (fast_retransmit_triggered or (time.time() - timer_start) >= self.timeout):
                retries_on_base += 1
                dup_ack_count = 0
                if retries_on_base > self.max_retries:
                    print(f"[SENDER ERROR] Max retries ({self.max_retries}) exceeded at GBN base seq={base}.")
                    return False

                in_flight = max(1, next_seq - base)
                self.retransmissions += in_flight
                self.window_retransmits += 1
                reason = "FAST-RETRANSMIT (Dup ACKs)" if fast_retransmit_triggered else "TIMEOUT"
                print(
                    f"[{reason} / RETRY #{retries_on_base}] Missing ACK for seq={base} "
                    f"(Go-Back-N rewinding {in_flight} packet(s) from seq={base})..."
                )
                next_seq = base
                timer_start = time.time()
                continue

        return True

    def send_file(self, filepath: str) -> bool:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)
        file_hash = compute_file_sha256(filepath)
        total_chunks = (filesize + CHUNK_SIZE - 1) // CHUNK_SIZE
        mode_name = "Stop-and-Wait (W=1)" if self.window_size == 1 else f"Sliding Window Go-Back-N (W={self.window_size})"

        print("============================================================")
        print(f"[UDP SENDER] Target Server   : {self.receiver_addr[0]}:{self.receiver_addr[1]}")
        print(f"[UDP SENDER] ARQ Mode        : {mode_name}")
        print(f"[UDP SENDER] File            : {filename} ({filesize} bytes, {total_chunks} chunks)")
        print(f"[UDP SENDER] SHA-256 Digest   : {file_hash}")
        print("============================================================")

        start_time = time.time()

        # Step 1: Send SYN packet (seq_num = 0) with metadata: "filename|filesize|sha256"
        metadata = f"{filename}|{filesize}|{file_hash}".encode('utf-8')
        print("[UDP SENDER] Sending SYN (seq=0) with file metadata...")
        if not self.send_reliable_packet(PKT_SYN, seq_num=0, payload=metadata):
            return False

        # Step 2: Send numbered DATA packets (seq_num = 1 .. total_chunks)
        if self.window_size == 1:
            seq_num = 1
            with open(filepath, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    if not self.send_reliable_packet(PKT_DATA, seq_num=seq_num, payload=chunk):
                        return False
                    if seq_num % 25 == 0 or seq_num == total_chunks:
                        progress = (seq_num / max(1, total_chunks)) * 100
                        print(f"[UDP SENDER] Progress: {seq_num}/{total_chunks} packets ACKed ({progress:.1f}%)")
                    seq_num += 1
            fin_seq = seq_num
        else:
            chunks = []
            with open(filepath, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    chunks.append(chunk)
            if not self._send_data_sliding_window(chunks):
                return False
            fin_seq = total_chunks + 1

        # Step 3: Send FIN packet (seq_num = total_chunks + 1) to close session and trigger hash check
        print(f"[UDP SENDER] Sending FIN (seq={fin_seq}) to finalize transfer...")
        if not self.send_reliable_packet(PKT_FIN, seq_num=fin_seq, payload=file_hash.encode('utf-8')):
            return False

        elapsed = max(0.001, time.time() - start_time)
        throughput_kbps = (filesize * 8 / 1000.0) / elapsed

        print("============================================================")
        print("[UDP SENDER] File Transfer Completed Successfully!")
        print(f"  - ARQ Mode          : {mode_name}")
        print(f"  - Elapsed Time      : {elapsed:.3f} s")
        print(f"  - Effective Goodput : {throughput_kbps:.2f} kbps")
        print(f"  - Total Packets Sent: {self.total_sent}")
        print(f"  - Retransmissions   : {self.retransmissions}")
        print("============================================================")

        if self.metrics_out:
            os.makedirs(os.path.dirname(os.path.abspath(self.metrics_out)), exist_ok=True)
            with open(self.metrics_out, 'w') as mf:
                json.dump({
                    "filename": filename,
                    "filesize": filesize,
                    "window_size": self.window_size,
                    "elapsed_s": round(elapsed, 4),
                    "goodput_kbps": round(throughput_kbps, 2),
                    "total_sent": self.total_sent,
                    "retransmissions": self.retransmissions,
                    "window_retransmits": self.window_retransmits,
                    "success": True,
                }, mf, indent=2)

        return True

    def close(self):
        self.sock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Reliable UDP File Sender (Stop-and-Wait & Go-Back-N)")
    parser.add_argument('filepath', help="Path to the file to send")
    parser.add_argument('--ip', default='10.0.0.2', help="Receiver IP address (default: 10.0.0.2 for Mininet h2)")
    parser.add_argument('--port', type=int, default=UDP_PORT, help=f"Receiver UDP port (default: {UDP_PORT})")
    parser.add_argument('--timeout', type=float, default=0.25, help="Retransmission timeout in seconds (default: 0.25)")
    parser.add_argument('--window', type=int, default=1, help="Sliding window size: 1=Stop-and-Wait, >1=Go-Back-N (default: 1)")
    parser.add_argument('--drop-prob', type=float, default=0.0, help="Simulated outgoing DATA packet loss probability (0.0 to 1.0)")
    parser.add_argument('--metrics-out', default=None, help="Optional JSON file path to export transfer metrics")
    args = parser.parse_args()

    sender = ReliableUDPSender(
        args.ip,
        args.port,
        args.timeout,
        window_size=args.window,
        drop_prob=args.drop_prob,
        metrics_out=args.metrics_out,
    )
    try:
        success = sender.send_file(args.filepath)
        sys.exit(0 if success else 1)
    finally:
        sender.close()
