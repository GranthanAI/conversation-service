"""
Shared Helper Utilities.
Includes platform independent UUID generation scripts and converters.
"""

import time
import os
import uuid

def uuidv7() -> uuid.UUID:
    """
    Generates a compliant, chronological RFC 9562 UUIDv7.
    Binds a 48-bit millisecond unix timestamp with random bits.
    """
    # 48-bit timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)
    
    # 10 random bytes for entropy
    rand_bytes = os.urandom(10)
    
    # Pack timestamp_ms to 6 bytes (48 bits)
    ts_bytes = timestamp_ms.to_bytes(6, byteorder='big')
    
    # Set UUID version to 7: bits 4-7 of 7th byte are set to 0111 (0x7)
    v7_byte = (rand_bytes[0] & 0x0F) | 0x70
    
    # Set UUID variant to 2 (RFC 4122): bits 6-7 of 9th byte (index 8) are set to 10 (0x8)
    var_byte = (rand_bytes[1] & 0x3F) | 0x80
    
    uuid_bytes = (
        ts_bytes +
        bytes([v7_byte]) +
        bytes([rand_bytes[2]]) +  # index 7: random
        bytes([var_byte]) +       # index 8: variant
        rand_bytes[3:]            # index 9-15: random
    )
    return uuid.UUID(bytes=uuid_bytes)
