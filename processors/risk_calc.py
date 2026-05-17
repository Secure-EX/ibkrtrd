import os
import sys
import glob
import json
import pandas as pd
from pathlib import Path

# 为了确保在终端里直接运行此文件也能找到根目录的 config.py，需要将项目根目录加入 sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from config import PORTFOLIO_DIR, LATEST_DIR

def _get_latest_file(directory: Path, prefix: str) -> Path:
    """
    内部辅助函数：在指定目录中寻找以 prefix 开头的最新文件。
    防止由于某天没跑拉取程序，导致找不到当天文件而报错。
    """
    search_pattern = str(directory / f"{prefix}*.csv")
    files = glob.glob(search_pattern)
    if not files:
        return None
    # 按文件修改时间降序排序，取最新的一个
    latest_file = max(files, key=os.path.getmtime)
    return Path(latest_file)

def _safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def generate_portfolio_risk_report() -> dict:
    """
    读取 IBKR 持仓与账户快照，计算账户风控指标、持仓胜率赔率及压力测试。
    """
    print(f"\n🛡️ 正在进行投资组合级风控与敞口计算 (Risk & Exposure)...")
    
    summary_file = _get_latest_file(PORTFOLIO_DIR, "account_summary_")
    positions_file = _get_latest_file(PORTFOLIO_DIR, "current_positions_")
    
    if not summary_file or not positions_file:
        print("⚠️ 警告: 找不到最新的 IBKR 账户快照或持仓文件，跳过风控计算。")
        return {}

    # ==========================================
    # 1. 账户级全局风控计算 (Account Level)
    # ==========================================
    df_summary = pd.read_csv(summary_file)

    # 提取各币种到基础货币的实时汇率字典
    fx_dict = df_summary.set_index('Currency')['Exchange Rate'].to_dict()

    # 读取 BASE_TOTAL_CALC 行
    base_row = df_summary[df_summary['Currency'] == 'BASE_TOTAL_CALC']

    if not base_row.empty:
        net_liq = _safe_float(base_row['Net Liquidation'].iloc[0]) # 这里是加币
        total_cash = _safe_float(base_row['Total Cash'].iloc[0])   # 这里是加币
    else:
        net_liq = 0.0
        total_cash = 0.0

    if net_liq <= 0:
        print("⚠️ 警告: 账户总净值为 0，跳过风控计算。")
        return {}

    cash_ratio = total_cash / net_liq

    # 注意：当前汇率矩阵表未抓取 MaintMarginReq（维持保证金），暂置 None。
    # 用 None 而不是 0，避免 LLM/前端把它误读为"完全无杠杆"。
    margin_utilization = None
    # margin_utilization = maint_margin / net_liq # 保证金占用率 (越接近 1 越容易爆仓)
    
    # ==========================================
    # 2. 持仓级胜率与赔率计算 (Position Level)
    # ==========================================
    df_pos = pd.read_csv(positions_file)
    
    total_positions = len(df_pos)
    winners = []
    losers = []
    
    top_holdings = []
    total_unrealized_pnl_base = 0.0 # 统一为 Base Currency (加币)
    total_stock_exposure_base = 0.0 # 统一为 Base Currency (加币)
    
    if total_positions > 0:
        # 按持仓市值降序排列，找出重仓股
        df_pos = df_pos.sort_values(by='Market Value', ascending=False)
        
        for _, row in df_pos.iterrows():
            symbol = row['Symbol']
            local_curr = row.get('Currency', 'CAD') # 提取这只股票的计价货币
            fx = _safe_float(fx_dict.get(local_curr, 1.0)) # 拿到对应的汇率

            unrealized_pnl_local = _safe_float(row['Unrealized P&L'])
            market_val_local = _safe_float(row['Market Value'])
            weight = _safe_float(row['Net Liq Ratio'])
            pnl_ratio = _safe_float(row['Unrealized P&L Ratio'])

            # 核心折算：将原生币种的盈亏和市值，乘上汇率转换为加币
            total_unrealized_pnl_base += (unrealized_pnl_local * fx)
            total_stock_exposure_base += (market_val_local * fx)

            # 记录重仓股 (占比超过 15% 即视为主要持仓)
            if weight >= 0.15:
                top_holdings.append({
                    "symbol": symbol,
                    "weight_ratio": weight,
                    "unrealized_pnl_ratio": pnl_ratio
                })

            # 区分赚钱和亏钱的票，收集盈亏百分比用于算赔率
            if unrealized_pnl_local > 0:
                winners.append(pnl_ratio)
            elif unrealized_pnl_local < 0:
                losers.append(pnl_ratio)
                
    # 计算当前胜率与赔率 (基于未结平仓的浮动盈亏)
    win_rate = len(winners) / total_positions if total_positions > 0 else 0.0
    avg_win_ratio = (sum(winners) / len(winners)) if len(winners) > 0 else 0.0
    avg_loss_ratio = (abs(sum(losers)) / len(losers)) if len(losers) > 0 else 0.0
    
    # 赔率 (Profit Factor / 盈亏比) = 平均盈利幅度 / 平均亏损幅度
    # 无亏损样本时返回 None（不再用 99.9 魔法值，避免被误读为"接近 100 倍赔率"）
    if avg_loss_ratio > 0:
        odds = avg_win_ratio / avg_loss_ratio
    elif avg_win_ratio > 0:
        odds = None  # 全员盈利，无亏损分母 → 数据上无法计算赔率
    else:
        odds = 0.0

    # ==========================================
    # 3. 极限压力测试 (Stress Test)
    # ==========================================
    # 假设：所有股票同时遭遇 20% 的断崖式下跌，计算对账户总净值的冲击 (最大回撤预估)
    # 这是一种极度保守的尾部风险评估法 (Value at Risk 简易替代)
    # 分子(股票总暴露) 和 分母(总净值) 现在都是纯粹的 CAD 加币！
    stress_test_drawdown_value = total_stock_exposure_base * 0.20
    max_drawdown_impact_ratio = (stress_test_drawdown_value / net_liq)

    # ==========================================
    # 4. 组装风控 JSON
    # ==========================================
    risk_report = {
        "portfolio_summary": {
            "base_currency": "CAD",
            "net_liquidation": net_liq,
            "total_cash": total_cash,
            "cash_ratio": cash_ratio,
            "margin_utilization_ratio": margin_utilization,
            "total_unrealized_pnl": total_unrealized_pnl_base
        },
        "performance_metrics": {
            "total_positions_count": total_positions,
            "win_rate_ratio": win_rate,
            "average_win_ratio": avg_win_ratio,
            "average_loss_ratio": avg_loss_ratio,
            "current_odds_ratio": odds
        },
        "concentration_and_stress": {
            "top_holdings": top_holdings,
            "stress_test_20pct_drop_impact_ratio": max_drawdown_impact_ratio
        }
    }
    
    # 落盘保存为组合级风控报告
    risk_file_path = LATEST_DIR / "portfolio_risk.json"
    with open(risk_file_path, 'w', encoding='utf-8') as f:
        json.dump(risk_report, f, indent=4, ensure_ascii=False)
        
    margin_str = "N/A" if margin_utilization is None else f"{round(margin_utilization*100, 2)}%"
    print(f"✅ 风控计算完成！(账户净值: {round(net_liq, 2)} CAD, 维持保证金占用: {margin_str})")
    print(f"✅ 账户级风控报告已保存至: {risk_file_path.name}")
    
    return risk_report

# ==========================================
# 测试模块
# ==========================================
if __name__ == "__main__":
    report = generate_portfolio_risk_report()
    if report:
        print("\n最终输出的组合风控 JSON 结构片段:")
        print(json.dumps(report, indent=4, ensure_ascii=False))
