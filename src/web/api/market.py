"""市场指数 API - 公共数据，无需认证"""
import logging
from fastapi import APIRouter

from src.core.data_provider_client import get_data_provider

logger = logging.getLogger(__name__)
router = APIRouter()

# 主要市场指数配置
MARKET_INDICES = [
    # A股指数
    {"symbol": "000001", "name": "上证指数", "market": "CN"},
    {"symbol": "399001", "name": "深证成指", "market": "CN"},
    {"symbol": "399006", "name": "创业板指", "market": "CN"},
    # 港股指数
    {"symbol": "HSI", "name": "恒生指数", "market": "HK"},
    # 美股指数
    {"symbol": "IXIC", "name": "纳斯达克", "market": "US"},
    {"symbol": "DJI", "name": "道琼斯", "market": "US"},
]


@router.get("/indices")
async def get_market_indices():
    """获取主要市场指数（公共数据，无需认证）"""
    dp = get_data_provider()
    items = [{"symbol": idx["symbol"], "market": idx["market"]} for idx in MARKET_INDICES]

    try:
        quotes = await dp.batch_quotes(items)
    except Exception as e:
        logger.error(f"获取市场指数失败: {e}")
        return []

    # 构建 symbol -> quote 映射
    quote_map = {q["symbol"]: q for q in quotes}

    result = []
    for idx in MARKET_INDICES:
        quote = quote_map.get(idx["symbol"])

        if quote:
            result.append({
                "symbol": idx["symbol"],
                "name": idx.get("name") or quote.get("name", ""),
                "market": idx["market"],
                "current_price": quote.get("current_price"),
                "change_pct": quote.get("change_pct"),
                "change_amount": quote.get("change_amount"),
                "prev_close": quote.get("prev_close"),
            })
        else:
            result.append({
                "symbol": idx["symbol"],
                "name": idx["name"],
                "market": idx["market"],
                "current_price": None,
                "change_pct": None,
                "change_amount": None,
                "prev_close": None,
            })

    return result
