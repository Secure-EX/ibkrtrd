"""
config_bt.py — 回测专用配置数据类

与主项目 config.py 分离，避免副作用（主 config 会自动创建目录并加载 .env）。
回测配置通过 BacktestConfig dataclass 传递，所有参数均有合理默认值。
"""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import sys

# 确保能导入主项目 config 中的路径常量
_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))
from config import OHLCV_DIR, FINANCIALS_DIR, OUTPUT_ROOT, RISK_FREE_RATE


@dataclass
class BacktestConfig:
    # ---- 目标标的 ----
    ticker: str                         # 股票代码，如 "0700.HK"
    benchmark: str = "INDEX_HSI"        # 基准指数文件前缀，对应 ohlcv/ 下的 CSV

    # ---- 时间范围 ----
    start_date: str = "2015-01-01"      # 回测起始日期（含热身期）
    end_date: str = None                # 回测结束日期，None = 使用全部可用数据

    # ---- 初始资金 ----
    initial_capital: float = 1_000_000.0  # HKD

    # ---- 仓位管理 ----
    position_sizing: str = "fixed_fraction"   # "fixed_fraction" | "all_in"
    fixed_fraction: float = 0.25              # 该股最大总仓位占初始资金比例
    max_position_pct: float = 1.0             # all_in 模式下最大仓位比例
    max_tranches: int = 3                     # 最多分几批建仓（每批 = fixed_fraction / max_tranches）

    # ---- 交易成本（IBKR HK 实盘参数）----
    commission_rate: float = 0.0008     # 约 0.08%
    stamp_duty: float = 0.0013         # 香港印花税 0.13%（单向，仅买卖均收）
    min_commission: float = 18.0       # IBKR 港股最低佣金 HKD

    # ---- 调仓频率 ----
    rebalance_freq: str = "weekly"     # "daily" | "weekly" | "monthly"
    rebalance_day: int = 4             # 周频下的调仓星期几：0=周一 … 4=周五

    # ---- 信号热身期 ----
    warmup_days: int = 260             # 信号计算所需最小历史数据量（~1年）
                                       # 热身期内不产生交易信号

    # ---- 策略选择 ----
    strategy_name: str = "multifactor_risk"   # 见 strategy.py 中的 STRATEGY_REGISTRY
    strategy_params: dict = field(default_factory=dict)
    # 默认策略参数（若 strategy_params 为空时使用）：
    #   buy_threshold: 0.05   — long_term_risk < 此值时买入（机会区）
    #   sell_threshold: 0.95  — long_term_risk > 此值时卖出（风险区）
    #   stop_loss_pct: -0.30  — 浮亏超过此值时止损
    #   short_term_filter: True  — 买入时过滤短线超买
    #   short_term_buy_max: 0.80 — 短线风险不超过此值才允许买入

    # ---- 无风险利率 ----
    risk_free_rate: float = RISK_FREE_RATE  # 来自主项目 config.py，默认 0.04

    # ---- 路径（来自主项目 config.py）----
    ohlcv_dir: Path = field(default_factory=lambda: OHLCV_DIR)
    financials_dir: Path = field(default_factory=lambda: FINANCIALS_DIR)
    output_dir: Path = field(default_factory=lambda: OUTPUT_ROOT / "backtest")

    def __post_init__(self):
        # 补充默认策略参数
        defaults = {
            "buy_threshold": 0.05,
            "sell_threshold": 0.95,
            "stop_loss_pct": -0.30,
            "short_term_filter": True,
            "short_term_buy_max": 0.80,
        }
        for k, v in defaults.items():
            self.strategy_params.setdefault(k, v)

        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def label(self) -> str:
        """生成标识此次回测的简短字符串，用于输出文件夹命名。"""
        ticker_clean = self.ticker.replace(".", "_")
        return f"bt_{ticker_clean}_{self.strategy_name}"
