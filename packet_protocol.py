#!/usr/bin/env python3
"""
packet_protocol.py
Custom Application-Layer Protocol Header & Integrity Utilities over UDP
Author: Shreyas

Header Format (15 Bytes, Network Big-Endian '!BIIHI'):
  - pkt_type    (1 Byte,  uint8)  : 0 = SYN (Metadata), 1 = DATA, 2 = ACK, 3 = FIN (End of transfer)
  - seq_num     (4 Bytes, uint32) : Packet sequence number (0, 1, 2, ...)
  - ack_num     (4 Bytes, uint32) : Acknowledgment number
  - payload_len (2 Bytes, uint16) : Length of payload in bytes (0 to 1024)
  - checksum    (4 Bytes, uint32) : CRC32 checksum computed over header (with checksum=0) + payload
"""

import struct
import zlib
import hashlib

# Constants
UDP_PORT = 5005
CHUNK_SIZE = 1024
HEADER_FORMAT = '!BIIHI'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 15 bytes
MAX_PACKET_SIZE = HEADER_SIZE + CHUNK_SIZE

# Packet Types
PKT_SYN = 0
PKT_DATA = 1
PKT_ACK = 2
PKT_FIN = 3


def pack_packet(pkt_type: int, seq_num: int, ack_num: int, payload: bytes = b'') -> bytes:
    """Packs the custom UDP header and payload with a CRC32 checksum for error detection."""
    payload_len = len(payload)
    temp_header = struct.pack(HEADER_FORMAT, pkt_type, seq_num, ack_num, payload_len, 0)
    checksum = zlib.crc32(temp_header + payload) & 0xFFFFFFFF
    final_header = struct.pack(HEADER_FORMAT, pkt_type, seq_num, ack_num, payload_len, checksum)
    return final_header + payload


def unpack_packet(raw_bytes: bytes):
    """
    Unpacks a raw UDP datagram and verifies its CRC32 checksum.
    Returns: (is_valid, pkt_type, seq_num, ack_num, payload)
    """
    if len(raw_bytes) < HEADER_SIZE:
        return False, None, None, None, b''

    header = raw_bytes[:HEADER_SIZE]
    payload = raw_bytes[HEADER_SIZE:]

    pkt_type, seq_num, ack_num, payload_len, received_checksum = struct.unpack(HEADER_FORMAT, header)

    if len(payload) != payload_len:
        return False, pkt_type, seq_num, ack_num, b''

    temp_header = struct.pack(HEADER_FORMAT, pkt_type, seq_num, ack_num, payload_len, 0)
    computed_checksum = zlib.crc32(temp_header + payload) & 0xFFFFFFFF

    if received_checksum != computed_checksum:
        return False, pkt_type, seq_num, ack_num, b''

    return True, pkt_type, seq_num, ack_num, payload


def compute_file_sha256(filepath: str) -> str:
    """Computes the full SHA-256 hex digest of a file to verify end-to-end integrity."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(65536):
            sha256.update(chunk)
    return sha256.hexdigest()
