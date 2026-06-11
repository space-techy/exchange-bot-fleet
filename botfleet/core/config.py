"""GeneratorConfig — every parameter that controls order generation."""

from dataclasses import dataclass


@dataclass
class GeneratorConfig:
    """All parameters that control order generation."""
    seed: int = 64

    # Price Model
    start_price: int = 1000
    fair_value: int = 1000
    volatility: float = 2.0                 # how wild price movements are
    mean_reversion: float = 0.05            # how strongly price snaps back to real value

    # Operation mix (sum == 1.0)
    passive_new_orders: float = 0.60        # non-crossing limit orders
    aggressive_new_orders: float = 0.20      # aggressive limit orders (cause matches)
    cancel_orders: float = 0.10             # cancel limit orders
    modify_orders: float = 0.10             # modify limit orders

    # Price offset distribution (How wide or near you want price distribution to be)
    offset_lambda: float = 0.2              # higher = tighter book (ver near prices), lower = wider book (very distributed prices)
    max_price_deviation: int = 50           # for squared-random method
    price_distribution: str = "squared"     # "exponential" or "squared"
    aggressive_overshoot_min: int = 1       # how far PAST the mid-price an aggressive order goes
    aggressive_overshoot_max: int = 100     # how far PAST the mid-price an aggressive order goes

    # Quantity distribution
    qty_min: int = 1                        # minimum quantity of orders
    qty_max: int = 100                      # maximum quantity of orders
    qty_scale: int = 6                      # for multiplied-random method
    qty_distribution: str = "heavy_tail"    # "uniform" or "heavy_tail"

    # Side balance
    buy_probability: int = 0.50             # 0.5 = balanced, 0.7 = more buys compared to sells

    # symbol
    symbol: int = 1                         # the single symbol this bot trades (set per-pod by the runner)

    # limits
    total_orders: int = 10000               # no. of total orders
