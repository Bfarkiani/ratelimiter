from dataclasses import dataclass

@dataclass
class Stats:
    """Client reporting statistics."""
    type: str  # 'stat' or 'cong'
    id: int
    sent: int

@dataclass
class ReceivedData:
    """Data received from the controller."""
    total_requests: int
    active_clients: int
    reported_429: int
    token_rate: float
