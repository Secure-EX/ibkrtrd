def check_risk_reward(entry_price, stop_loss, target_price):
    """简单的盈亏比计算 (预留)"""
    risk = entry_price - stop_loss
    reward = target_price - entry_price
    if risk <= 0: return 0
    return reward / risk
