from dataclasses import dataclass

@dataclass
class DriftParams:
    t0_drift:int = None
    mu_drift:float = None
    delta_drift:int = None


@dataclass
class SensorParams:
    sensor_name: str
    mu: float
    sigma: float
    eps: float
    delay_mode: str  # "fix" or "random"
    delay_value: float
    max_delay: float = None  # Optional for "random" delay mode
    drifts:list = None