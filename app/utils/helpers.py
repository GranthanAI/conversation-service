"""
Shared Helper Utilities.
Includes platform independent UUID generation scripts and converters.
"""

import time
import os
import uuid

def uuidv7_after(floor: uuid.UUID) -> uuid.UUID:
    """A UUIDv7 strictly greater than `floor`.

    History is clustered by message_id DESC, so an assistant reply must compare
    greater than the user message it answers. Two UUIDv7s minted in the same
    millisecond have random low bits and can compare either way, so retry (and, if
    the clock is behind `floor`, advance past it) until the invariant holds.
    """
    for _ in range(64):
        candidate = uuidv7()
        if candidate.int > floor.int:
            return candidate
        time.sleep(0.001)
    # Clock behind the floor's timestamp: build directly above it.
    return uuid.UUID(int=min(floor.int + 1, (1 << 128) - 1))


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


def uuid7_age_ms(value) -> float | None:
    """Milliseconds since a UUIDv7 was minted (its first 48 bits are a unix-ms timestamp).
    Used to derive time-since-accept for an assistant response id without storing extra state."""
    import time
    import uuid as _uuid

    try:
        u = value if isinstance(value, _uuid.UUID) else _uuid.UUID(str(value))
        if u.version != 7:
            return None
        return max(0.0, round(time.time() * 1000.0 - int(u.hex[:12], 16), 1))
    except (ValueError, AttributeError, TypeError):
        return None


def timeuuid_age_ms(value) -> float | None:
    """Milliseconds since a UUIDv1 (timeuuid) was minted."""
    import time
    import uuid as _uuid

    try:
        u = value if isinstance(value, _uuid.UUID) else _uuid.UUID(str(value))
        if u.version != 1:
            return None
        unix_s = (u.time - 0x01B21DD213814000) / 1e7
        return max(0.0, round((time.time() - unix_s) * 1000.0, 1))
    except (ValueError, AttributeError, TypeError):
        return None
