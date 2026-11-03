"""Configuration shared by Payment's message handler and command line service."""

from dataclasses import dataclass
import math
import os


@dataclass(frozen=True)
class PaymentSettings:
    mode: str = "random"
    success_rate: float = 0.9
    seed: str = "nt132"
    delivery_timeout: float = 15.0

    def __post_init__(self):
        if self.mode not in {"random", "success", "failure"}:
            raise ValueError("PAYMENT_MODE must be random, success or failure")
        if (
            isinstance(self.success_rate, bool)
            or not isinstance(self.success_rate, (float, int))
            or not math.isfinite(self.success_rate)
            or not 0 <= self.success_rate <= 1
        ):
            raise ValueError("PAYMENT_SUCCESS_RATE must be finite and between 0 and 1")
        if not isinstance(self.seed, str) or not self.seed.strip():
            raise ValueError("PAYMENT_SEED must not be empty")
        if (
            isinstance(self.delivery_timeout, bool)
            or not isinstance(self.delivery_timeout, (float, int))
            or not math.isfinite(self.delivery_timeout)
            or self.delivery_timeout <= 0
        ):
            raise ValueError("PAYMENT_DELIVERY_TIMEOUT must be finite and positive")

    @classmethod
    def from_env(cls):
        return cls(
            mode=os.getenv("PAYMENT_MODE", "random").lower(),
            success_rate=float(os.getenv("PAYMENT_SUCCESS_RATE", "0.9")),
            seed=os.getenv("PAYMENT_SEED", "nt132"),
            delivery_timeout=float(os.getenv("PAYMENT_DELIVERY_TIMEOUT", "15")),
        )
