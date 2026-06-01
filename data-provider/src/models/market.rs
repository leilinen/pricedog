/// Market code enum.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MarketCode {
    CN,
    HK,
    US,
    CRYPTO,
}

impl MarketCode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CN" => Some(Self::CN),
            "HK" => Some(Self::HK),
            "US" => Some(Self::US),
            "CRYPTO" => Some(Self::CRYPTO),
            _ => None,
        }
    }
}

/// Return CN exchange code: SH / SZ / BJ.
pub fn cn_exchange(symbol: &str) -> &'static str {
    let sym = symbol.trim();
    if sym.starts_with("920") || sym.starts_with("83") || sym.starts_with("87") || sym.starts_with("88") {
        "BJ"
    } else if sym.starts_with('5') || sym.starts_with('6') || sym.starts_with("900") {
        "SH"
    } else {
        "SZ"
    }
}

/// Return lowercase market prefix for Tencent API: sh / sz / bj.
pub fn cn_prefix(symbol: &str) -> &'static str {
    match cn_exchange(symbol) {
        "SH" => "sh",
        "BJ" => "bj",
        _ => "sz",
    }
}

/// Whether a CN symbol belongs to Shanghai exchange.
pub fn is_cn_sh(symbol: &str) -> bool {
    cn_exchange(symbol) == "SH"
}

/// Convert symbol + market to Tencent API format.
/// CN: sh600519, bj430047, sz000001
/// HK: hk00700
/// US: usAAPL
pub fn tencent_symbol(symbol: &str, market: &MarketCode) -> String {
    match market {
        MarketCode::HK => format!("hk{}", symbol),
        MarketCode::US => format!("us{}", symbol),
        _ => format!("{}{}", cn_prefix(symbol), symbol),
    }
}

/// Convert symbol + market to EastMoney secid format.
/// CN: 1.600519 (SH) or 0.000001 (SZ)
/// HK: 116.00700
/// US: 105.AAPL
pub fn eastmoney_secid(symbol: &str, market: &MarketCode) -> String {
    match market {
        MarketCode::HK => format!("116.{}", symbol),
        MarketCode::US => format!("105.{}", symbol),
        _ => {
            let prefix = if is_cn_sh(symbol) { "1" } else { "0" };
            format!("{}.{}", prefix, symbol)
        }
    }
}
