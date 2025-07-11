from dataclasses import dataclass

@dataclass
class DriftParams:
    t0_drift:int = None
    mu_drift:float = None
    sigma_drift:float=None
    duration_drift:int = None
    drift_type:str = "mean-sudden"  # "mean-gradual", # "sigma-sudden"


@dataclass
class SensorParams:
    sensor_name: str
    mu: float
    sigma: float
    eps: float
    eps_nan:float = 0.0
    max_delay: float = None  # Optional for "random" delay mode
    drifts:list = None
    eps_nan:float