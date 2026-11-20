#!/usr/bin/env python3
"""End-to-end tests for the reliable UDP sender/receiver (Demo 1 & Demo 2).

Run from this directory with: python3 test_transfer.py
"""
import hashlib
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from packet_protocol import (
    PKT_SYN, PKT_DATA, PKT_FIN, PKT_ACK, pack_packet, unpack_packet,
)

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable


def free_udp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ReliableUDPIntegrationTests(unittest.TestCase):
    def run_transfer(self, data: bytes, drop_first_fin_ack=False, window_size=1, drop_prob=0.0):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "source.bin"
            out_dir = tmp_path / "received"
            source.write_bytes(data)
            port = free_udp_port()

            receiver_cmd = [
                PYTHON, "-u", str(ROOT / "udp_receiver.py"),
                "--ip", "127.0.0.1", "--port", str(port), "--out", str(out_dir),
            ]
            if drop_first_fin_ack:
                receiver_cmd.append("--drop-first-fin-ack")

            receiver = subprocess.Popen(
                receiver_cmd, cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            try:
                time.sleep(0.35)
                sender = subprocess.run(
                    [
                        PYTHON, str(ROOT / "udp_sender.py"), str(source),
                        "--ip", "127.0.0.1", "--port", str(port),
                        "--timeout", "0.08", "--window", str(window_size),
                        "--drop-prob", str(drop_prob),
                    ],
                    cwd=ROOT, capture_output=True, text=True, timeout=25,
                )
                self.assertEqual(sender.returncode, 0, msg=sender.stdout + sender.stderr)

                received = out_dir / "recv_source.bin"
                deadline = time.time() + 2
                while not received.exists() and time.time() < deadline:
                    time.sleep(0.05)
                self.assertTrue(received.exists(), "Receiver did not save the reconstructed file")
                self.assertEqual(hashlib.sha256(source.read_bytes()).digest(),
                                 hashlib.sha256(received.read_bytes()).digest())

                if drop_first_fin_ack:
                    self.assertIn("TIMEOUT / RETRY", sender.stdout)
                    self.assertIn("Retransmissions   : 1", sender.stdout)
                return sender.stdout
            finally:
                receiver.terminate()
                try:
                    receiver.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    receiver.kill()
                    receiver.wait(timeout=2)
                if receiver.stdout:
                    receiver.stdout.close()

    def test_1_normal_transfer(self):
        output = self.run_transfer((b"Reliable UDP test data\n" * 400))
        self.assertIn("File Transfer Completed Successfully", output)

    def test_2_lost_final_ack_is_recovered(self):
        output = self.run_transfer(b"Testing final ACK recovery!" * 100,
                                   drop_first_fin_ack=True)
        self.assertIn("File Transfer Completed Successfully", output)

    def test_3_empty_file_transfer(self):
        output = self.run_transfer(b"")
        self.assertIn("File Transfer Completed Successfully", output)

    def test_4_hash_mismatch_is_not_finally_acknowledged(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "received"
            port = free_udp_port()
            receiver = subprocess.Popen(
                [PYTHON, "-u", str(ROOT / "udp_receiver.py"),
                 "--ip", "127.0.0.1", "--port", str(port), "--out", str(out_dir)],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            payload = b"hash mismatch test"
            wrong_hash = "0" * 64
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.settimeout(1.0)
            try:
                time.sleep(0.35)
                syn = pack_packet(PKT_SYN, 0, 0, f"bad.bin|{len(payload)}|{wrong_hash}".encode())
                client.sendto(syn, ("127.0.0.1", port))
                response, _ = client.recvfrom(2048)
                valid, pkt_type, _, ack_num, _ = unpack_packet(response)
                self.assertTrue(valid and pkt_type == PKT_ACK and ack_num == 0)

                client.sendto(pack_packet(PKT_DATA, 1, 0, payload), ("127.0.0.1", port))
                response, _ = client.recvfrom(2048)
                valid, pkt_type, _, ack_num, _ = unpack_packet(response)
                self.assertTrue(valid and pkt_type == PKT_ACK and ack_num == 1)

                client.sendto(pack_packet(PKT_FIN, 2, 0, wrong_hash.encode()), ("127.0.0.1", port))
                client.settimeout(0.3)
                with self.assertRaises(socket.timeout):
                    client.recvfrom(2048)
                self.assertFalse((out_dir / "recv_bad.bin").exists())
            finally:
                client.close()
                receiver.terminate()
                receiver.wait(timeout=2)
                if receiver.stdout:
                    receiver.stdout.close()

    def test_5_size_mismatch_is_not_finally_acknowledged(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "received"
            port = free_udp_port()
            receiver = subprocess.Popen(
                [PYTHON, "-u", str(ROOT / "udp_receiver.py"),
                 "--ip", "127.0.0.1", "--port", str(port), "--out", str(out_dir)],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            payload = b"size mismatch test"
            correct_hash = hashlib.sha256(payload).hexdigest()
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.settimeout(0.3)
            try:
                time.sleep(0.35)
                syn = pack_packet(PKT_SYN, 0, 0, f"bad_size.bin|{len(payload) + 1}|{correct_hash}".encode())
                client.sendto(syn, ("127.0.0.1", port))
                client.recvfrom(2048)

                client.sendto(pack_packet(PKT_DATA, 1, 0, payload), ("127.0.0.1", port))
                client.recvfrom(2048)

                client.sendto(pack_packet(PKT_FIN, 2, 0, correct_hash.encode()), ("127.0.0.1", port))
                with self.assertRaises(socket.timeout):
                    client.recvfrom(2048)
                self.assertFalse((out_dir / "recv_bad_size.bin").exists())
            finally:
                client.close()
                receiver.terminate()
                receiver.wait(timeout=2)
                if receiver.stdout:
                    receiver.stdout.close()

    def test_6_sliding_window_with_packet_loss_retransmission(self):
        """Demo 2: Verifies Go-Back-N Sliding Window (W=8) recovers from 10% packet loss."""
        output = self.run_transfer(
            b"Sliding Window Go-Back-N Packet Loss Recovery Test!\n" * 600,
            window_size=8,
            drop_prob=0.10,
        )
        self.assertIn("File Transfer Completed Successfully", output)
        self.assertIn("Sliding Window Go-Back-N (W=8)", output)

    def test_7_multi_client_multi_server_concurrent_transfers(self):
        """Demo 2: Verifies 2 clients transferring concurrently to 2 servers + 2 clients to 1 server."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            file_a = tmp_path / "clientA.bin"
            file_b = tmp_path / "clientB.bin"
            file_c = tmp_path / "clientC.bin"
            file_a.write_bytes(b"Client A payload " * 300)
            file_b.write_bytes(b"Client B payload " * 300)
            file_c.write_bytes(b"Client C payload " * 300)

            out_srv1 = tmp_path / "srv1_out"
            out_srv2 = tmp_path / "srv2_out"
            port1 = free_udp_port()
            port2 = free_udp_port()

            # Start 2 distinct UDP Receiver Servers simultaneously (Multi-Server)
            srv1 = subprocess.Popen(
                [PYTHON, "-u", str(ROOT / "udp_receiver.py"), "--ip", "127.0.0.1", "--port", str(port1), "--out", str(out_srv1)],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            srv2 = subprocess.Popen(
                [PYTHON, "-u", str(ROOT / "udp_receiver.py"), "--ip", "127.0.0.1", "--port", str(port2), "--out", str(out_srv2)],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            try:
                time.sleep(0.35)
                # Launch 3 concurrent client transfers (Client A & B -> Server 1; Client C -> Server 2)
                c1 = subprocess.Popen([PYTHON, str(ROOT / "udp_sender.py"), str(file_a), "--ip", "127.0.0.1", "--port", str(port1), "--window", "5"])
                c2 = subprocess.Popen([PYTHON, str(ROOT / "udp_sender.py"), str(file_b), "--ip", "127.0.0.1", "--port", str(port1), "--window", "5"])
                c3 = subprocess.Popen([PYTHON, str(ROOT / "udp_sender.py"), str(file_c), "--ip", "127.0.0.1", "--port", str(port2), "--window", "5"])

                self.assertEqual(c1.wait(timeout=15), 0)
                self.assertEqual(c2.wait(timeout=15), 0)
                self.assertEqual(c3.wait(timeout=15), 0)

                self.assertEqual(file_a.read_bytes(), (out_srv1 / "recv_clientA.bin").read_bytes())
                self.assertEqual(file_b.read_bytes(), (out_srv1 / "recv_clientB.bin").read_bytes())
                self.assertEqual(file_c.read_bytes(), (out_srv2 / "recv_clientC.bin").read_bytes())
            finally:
                for proc in (srv1, srv2):
                    proc.terminate()
                    proc.wait(timeout=2)
                    if proc.stdout:
                        proc.stdout.close()

    def test_8_out_of_order_missing_packet_detection(self):
        """Demo 2: Verifies receiver detects missing seq=1 when seq=2 arrives first, and recovers."""
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "received"
            port = free_udp_port()
            receiver = subprocess.Popen(
                [PYTHON, "-u", str(ROOT / "udp_receiver.py"), "--ip", "127.0.0.1", "--port", str(port), "--out", str(out_dir)],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            chunk1 = b"First_Chunk_Data_"
            chunk2 = b"Second_Chunk_Data"
            full_data = chunk1 + chunk2
            expected_hash = hashlib.sha256(full_data).hexdigest()
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.settimeout(0.5)
            try:
                time.sleep(0.35)
                # 1. Send SYN (seq=0)
                client.sendto(pack_packet(PKT_SYN, 0, 0, f"ooo.bin|{len(full_data)}|{expected_hash}".encode()), ("127.0.0.1", port))
                _, _ = client.recvfrom(2048)

                # 2. Send seq=2 BEFORE seq=1 (simulating missing seq=1 in flight)
                client.sendto(pack_packet(PKT_DATA, 2, 0, chunk2), ("127.0.0.1", port))
                resp, _ = client.recvfrom(2048)
                valid, pkt_type, _, ack_num, _ = unpack_packet(resp)
                # Receiver must re-ACK last in-order sequence number (0)
                self.assertTrue(valid and pkt_type == PKT_ACK and ack_num == 0)

                # 3. Retransmit seq=1 and seq=2 in order, then FIN (seq=3)
                client.sendto(pack_packet(PKT_DATA, 1, 0, chunk1), ("127.0.0.1", port))
                _, _ = client.recvfrom(2048)
                client.sendto(pack_packet(PKT_DATA, 2, 0, chunk2), ("127.0.0.1", port))
                _, _ = client.recvfrom(2048)
                client.sendto(pack_packet(PKT_FIN, 3, 0, expected_hash.encode()), ("127.0.0.1", port))
                resp, _ = client.recvfrom(2048)
                valid, pkt_type, _, ack_num, _ = unpack_packet(resp)
                self.assertTrue(valid and pkt_type == PKT_ACK and ack_num == 3)
                self.assertEqual((out_dir / "recv_ooo.bin").read_bytes(), full_data)
            finally:
                client.close()
                receiver.terminate()
                receiver.wait(timeout=2)
                if receiver.stdout:
                    receiver.stdout.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
