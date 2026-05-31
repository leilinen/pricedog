# Signal Bar Model V1 — 信号K线识别模型设计

## Context

现有 Rust 引擎实现了 V2 breakout 模型（65分评分体系），专注于突破检测。新模型与突破无关，核心目标是**逐K线扫描，识别出符合质量标准的信号K线**，为后续入场决策提供基础。

模型通过三层决策：市场背景评估 → 信号K线质量评分 → 入场决策（V1 仅实现前两层 + 特殊形态识别，入场确认后续迭代）。

## 架构

新建独立模型结构 `SignalBarModel`，不实现现有 `BreakoutModel` trait。在 `evaluate` 端点中作为独立信号源注册，signal_type 为 `"pa_signal_bar"`。

```
SignalBarModel::detect(klines) -> Vec<Signal>
  ├── 1. compute_bar_features(kline) -> BarFeatures     # 对每根K线计算基本特征
  ├── 2. evaluate_context(klines, indicators) -> ContextResult  # 市场背景评估
  ├── 3. score_bar_quality(features, direction) -> f64    # 信号K线质量评分
  └── 4. detect_patterns(curr, prev, features_curr, features_prev, context) -> Vec<Signal>
       ├── 常规信号K线（Q >= Q_min 且背景通过）
       ├── 2K反转
       ├── 吞噬线
       └── 惊喜K线
```

### 数据结构

```rust
/// 单根K线的基本特征
struct BarFeatures {
    body: f64,    // B = C - O（正值=阳线，负值=阴线）
    range: f64,   // R = H - L
    p_b: f64,     // 实体占比 = |B| / R（R≠0时，否则0）
    p_c: f64,     // 收盘位置 = (C - L) / R（R≠0时，否则0.5）
    p_u: f64,     // 上影线占比 = (H - max(C,O)) / R
    p_d: f64,     // 下影线占比 = (min(C,O) - L) / R
}

/// 市场背景评估结果
struct ContextResult {
    cr: f64,            // 通道比率 = (H_N - L_N) / ATR_M
    trend_dir: i32,     // 趋势方向 = sign(C - EMA20)，1=多, -1=空, 0=中性
    f_ratio: f64,       // 多空力量对比 = (F_bull - F_bear) / (F_bull + F_bear)
    is_valid: bool,     // CR >= θ（非窄通道）
}

/// 独立的信号K线模型
struct SignalBarModel;
```

### Signal 输出

- `signal_type`: `"pa_signal_bar"`（常规信号K线）或 `"pa_pattern"`（特殊形态）
- `score`: 信号质量分（0.3 / 0.6 / 1.0）
- `entry_price`: H + δ（多）或 L - δ（空），δ 取 `atr14 * 0.01`
- `stop_loss`: L - δ（多）或 H + δ（空）
- `target_price`: 基于 2:1 盈亏比计算
- `evidence`: JSON 记录所有中间计算值

## 核心算法

### 1. K线基本特征计算

```
输入: 单根K线 (O, H, L, C)
输出: BarFeatures

B = C - O
R = H - L
P_b = |B| / R  (R≠0; 否则 0)
P_c = (C - L) / R  (R≠0; 否则 0.5)
P_u = (H - max(C,O)) / R
P_d = (min(C,O) - L) / R
```

### 2. 市场背景评估

```
输入: 最近K线序列, indicators (EMA20, ATR14)
参数: N=10, M=20 (ATR周期), P=5 (力量对比窗口), θ=1.2
输出: ContextResult

通道宽度:
  H_N = max{H_i | i ∈ 最近N根}
  L_N = min{L_i | i ∈ 最近N根}
  ATR_M = 已有的 atr14（M=20，复用 Indicators）
  CR = (H_N - L_N) / ATR_M
  is_valid = CR >= 1.2

趋势方向:
  D = sign(C_current - EMA20)

多空力量:
  F_bull = Σ_{i=1}^{P} max(C_i - O_i, 0)
  F_bear = Σ_{i=1}^{P} max(O_i - C_i, 0)
  F_ratio = (F_bull - F_bear) / (F_bull + F_bear)
  （F_bull + F_bear == 0 时 F_ratio = 0）
```

### 3. 信号K线质量评分

**多头信号质量：**
```
Q_long =
  if C <= O: 0
  else if P_b >= 0.6 ∧ P_c >= 0.85 ∧ P_u <= 0.1: 1.0
  else if P_b >= 0.4 ∧ P_c >= 0.6  ∧ P_u <= 0.25: 0.6
  else if P_b >= 0.3 ∧ P_c >= 0.5: 0.3
  else: 0
```

**空头信号质量：**
```
Q_short =
  if C >= O: 0
  else if P_b >= 0.6 ∧ P_c <= 0.15 ∧ P_d <= 0.1: 1.0
  else if P_b >= 0.4 ∧ P_c <= 0.4  ∧ P_d <= 0.25: 0.6
  else if P_b >= 0.3 ∧ P_c <= 0.5: 0.3
  else: 0
```

### 4. 信号生成规则

#### 4a. 常规信号K线

```
条件（多头）:
  1. CR >= θ（非窄通道）
  2. Q_long >= Q_min（Q_min = 0.6）
  3. F_ratio > 0 或近期由负转正

条件（空头）:
  1. CR >= θ
  2. Q_short >= Q_min
  3. F_ratio < 0 或近期由正转负

入场参数:
  δ = atr14 * 0.01
  多头: entry = H + δ, stop = L - δ, target = entry + 2*(entry - stop)
  空头: entry = L - δ, stop = H + δ, target = entry - 2*(stop - entry)
```

**F_ratio 转正/转正的定义**：当前 F_ratio 的符号与 P 根K线前的 F_ratio 符号不同。实现时用 `sign(f_ratio_curr) != sign(f_ratio_prev)` 判断。

#### 4b. 2K反转（多头）

```
条件:
  Q_short(K_{t-1}) >= 0.6  （前一根是高质量空头信号K）
  ∧ Q_long(K_t) >= 0.6    （当前是高质量多头信号K）

入场参数同上（基于当前K线K_t的H/L）
```

#### 4c. 吞噬线

```
多头吞噬:
  H_t > H_{t-1} ∧ L_t < L_{t-1} ∧ C_t > O_t ∧ P_b_t >= 0.5

空头吞噬:
  H_t > H_{t-1} ∧ L_t < L_{t-1} ∧ C_t < O_t ∧ P_b_t >= 0.5
```

#### 4d. 惊喜K线

```
设 R_max = max{R_i | i ∈ 最近Q根} (Q=20)

惊喜K线 = R_t > 1.5 * R_max ∧ P_b_t >= 0.5
方向由 B 的符号决定
```

### 5. 信号优先级

当多个形态同时触发时，按优先级取最高的一个：
1. 2K反转（最强反转信号）
2. 吞噬线（强反转/持续信号）
3. 惊喜K线（强方向信号）
4. 常规信号K线

每根K线最多生成一个 Signal。

## 集成方式

### evaluate 端点

在 `evaluate_symbol()` 中，除了调用 `model.detect()`（breakout 模型），额外调用 `signal_bar_model.detect()`，将结果合并到 signals 数组中返回。

```rust
// evaluate_symbol() 中新增
let signal_bar_model = SignalBarModel;
let signal_bar_signals = signal_bar_model.detect(&klines);
signals.extend(signal_bar_signals);
```

### 数据库

`pa_signal` 表完全复用，`signal_type` 字段区分：
- `"pa_breakout"` — 现有 breakout 模型信号（保留）
- `"pa_signal_bar"` — 信号K线模型信号（新增）
- `"pa_pattern"` — 特殊形态信号（新增）

### min_klines

SignalBarModel 需要最少 **25 根** K线：ATR20(20) + 力量对比窗口(5) 有重叠，实际需要 20 根历史 + 5 根力量对比。为安全起见设为 **25**。

## 修改文件清单

| 文件 | 修改内容 |
|------|----------|
| `price-action-engine/src/main.rs` | 新增 `BarFeatures`, `ContextResult`, `SignalBarModel` 结构和实现，修改 `evaluate_symbol()` 集成 |

## V1 范围

- [x] K线基本特征计算
- [x] 市场背景评估
- [x] 信号K线质量评分
- [x] 常规信号K线生成
- [x] 特殊形态识别（2K反转、吞噬线、惊喜K线）
- [x] evaluate 端点集成
- [ ] 回测（后续迭代）
- [ ] 入场确认逻辑（后续迭代）

## 验证方式

1. `cargo test` 通过所有现有测试
2. `cargo run` 启动引擎
3. `curl POST /api/v1/evaluate` 传入包含信号K线的K线数据，验证返回的 signals 中包含 `pa_signal_bar` 类型的信号
4. 检查 evidence JSON 中的中间计算值是否正确
