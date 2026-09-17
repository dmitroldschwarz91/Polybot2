# Параметры WFO и план перестройки data architecture

Дата: 2026-09-17

## 1. Итоговые параметры WFO по активам

Основной прогон: `reports/demo_wfo_results.json`
Метод: rolling WFO `144 train / 48 test`, stake 20%, fill = `best_ask + 0.01`, fee = `0.07 * p * (1-p)` на акцию.
Outcome: Gamma для проверенных спорных/краевых markets + token-polarization fallback для оставшихся `cross_disagree`.

| Asset | Рекомендованный статус | `twap_stc_min` | `twap_stc_max` | `twap_min_dev_pct` | `twap_min_barrier_pct` | `twap_min_token_ask` | `twap_max_token_ask` | `full_book` | OOS |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| BTC | **не включать live** | 10 | 35 | 0.015 | 0.050 | 0.50 | 0.90 | false | 6 trades, WR 83.3%, return -7.75%, DD 17.84% |
| ETH | **только demo/watchlist** | 2 | 60 | 0.015 | 0.050 | 0.55 | 0.96 | false | 7 trades, WR 85.7%, return +8.13%, DD 9.58% |
| SOL | **недостаточно сигналов** | 2 | 60 | 0.015 | 0.050 | 0.50 | 0.94 | false | 1 trade, WR 100%, return +1.18%, DD 0% |
| XRP | **отключить** | 2 | 60 | 0.050 | 0.050 | 0.50 | 0.90 | false | 8 trades, WR 50%, return -58.33%, DD 62.04% |

Важно: это не production-параметры. Это best/consensus по короткому и очищенному демо-периоду. Для live нужен период 7–14 дней и хотя бы 30–50 OOS-сделок на актив после всех фильтров.

### Частотность параметров по fold'ам

#### BTC
- `stc_window`: `10-35` выбран 5/8, `2-60` 3/8.
- `min_dev_pct`: `0.015` 6/8, `0.020` 2/8.
- `min_barrier_pct`: `0.050` 8/8.
- `min_token_ask`: `0.50` 5/8, `0.65` 3/8.
- `max_token_ask`: `0.90` 4/8, `0.94` 3/8, `0.96` 1/8.

#### ETH
- `stc_window`: `2-60` 8/8.
- `min_dev_pct`: `0.015` 8/8.
- `min_barrier_pct`: `0.050` 8/8.
- `min_token_ask`: `0.55` 4/8, `0.50` 4/8. Я выбрал `0.55`, потому что это текущий production guard и он лучше защищает от 0.50/0.51 frozen/ambiguous books.
- `max_token_ask`: `0.96` 5/8, `0.90` 2/8, `0.92` 1/8.

#### SOL
- Валидных fold'ов только 6, OOS-сделка только 1.
- `stc_window`: `2-60` 4/6.
- `min_dev_pct`: `0.015` 6/6.
- `min_barrier_pct`: `0.050` 6/6.
- `max_token_ask`: `0.94` 5/6.

#### XRP
- `stc_window`: `2-60` 6/8.
- `min_dev_pct`: `0.050` 4/8 и `0.015` 4/8. Я указал `0.050` как более строгий консенсус, но сам актив по WFO провален.
- `min_barrier_pct`: `0.050` 8/8.
- `min_token_ask`: `0.50` 4/8, `0.65` 3/8, `0.60` 1/8.
- `max_token_ask`: `0.90` 4/8, `0.94` 3/8, `0.96` 1/8.

## 2. Что в текущей архитектуре портило качество входных данных

### Уже найдено и исправлено в коде

1. `sample_io.load_samples()` не читал headerless TSV chunks (`part02`), потому что ожидал JSONL, если нет строки `# header`.
   - Исправлено: TSV chunk без заголовка теперь читается по каноническому `FIELDS`.

2. `AsyncHTTP.get()` не принимал `timeout`, хотя `MarketData.seed_prices()` вызывал `self.http.get(ep, timeout=3.0)`.
   - Эффект: REST seed цен на старте фактически всегда падал в `TypeError` и silently игнорировался.
   - Исправлено: добавлены `timeout` и `headers`, JSON parse с `content_type=None`.

3. `BookPoller` перепутал CLOB side semantics:
   - `BUY` — это цена, которую платим при покупке, то есть **best ask**.
   - `SELL` — это цена продажи, то есть **best bid**.
   - Было наоборот. Исправлено.

4. `BookPoller` раньше тянул только `/price`, без глубины.
   - Исправлено: теперь сначала берёт `/book` и обновляет полный стакан; `/price` остаётся fallback.

5. `last_trade_price` в WS мог перезаписать `lot_prices` как будто это executable ask.
   - Исправлено: `last_trade_price` больше не затирает best ask fallback.

6. Dynamic subscribe на market WS отправлялся без `operation="subscribe"`.
   - Исправлено: для уже открытого сокета добавляется `operation="subscribe"`.

7. У market WS не было текстового `PING`, хотя RTDS его имел.
   - Исправлено: добавлен text PING loop и повышен dead-stream threshold для market channel с 10s до 45s.

8. Стратегия не валидировала масштаб `twap` / `twap_open`.
   - Исправлено: добавлен `twap_sanity_max_rel_diff = 0.02`; если TWAP/open отличается от spot/open больше чем на 2%, стратегия возвращает `twap_sanity_failed` или `twap_open_sanity_failed`.

## 3. Целевая архитектура data quality pipeline

Текущий код смешивает transport, state, feature computation и strategy checks. Для 5-минутных TWAP рынков лучше выделить отдельный слой `MarketDataBus`.

```text
                 ┌───────────────────────────┐
                 │ Feed Supervisors          │
                 │ - Polymarket CLOB WS      │
                 │ - CLOB REST /book batch   │
                 │ - Gamma metadata/results  │
                 │ - Chainlink RTDS TWAP     │
                 │ - Binance WS/REST         │
                 │ - Coinbase/Kraken backup  │
                 └─────────────┬─────────────┘
                               │ RawFeedEvent
                               ▼
                 ┌───────────────────────────┐
                 │ Normalizer + Timestamping │
                 │ monotonic recv_ts, src_ts │
                 │ source, latency, symbol   │
                 └─────────────┬─────────────┘
                               │
                               ▼
                 ┌───────────────────────────┐
                 │ State Stores              │
                 │ OracleStore               │
                 │ TwapStore                 │
                 │ OrderBookStore            │
                 │ MarketCatalogStore        │
                 └─────────────┬─────────────┘
                               │
                               ▼
                 ┌───────────────────────────┐
                 │ DataQualityGate           │
                 │ freshness, coverage,      │
                 │ source agreement, hashes, │
                 │ depth, spread, anomalies  │
                 └─────────────┬─────────────┘
                               │ CleanSnapshot + QualityScore
                               ▼
                 ┌───────────────────────────┐
                 │ Strategy Engine           │
                 │ TWAP Inertia              │
                 └─────────────┬─────────────┘
                               ▼
                 ┌───────────────────────────┐
                 │ PreFlight + Execution     │
                 └───────────────────────────┘
```

## 4. Конкретные компоненты

### 4.1 FeedSupervisor

Для каждого источника отдельный actor/task:

- `ClobMarketWsFeed`: market channel WS, snapshots + `price_change` + `best_bid_ask` + lifecycle events.
- `ClobRestBookFeed`: batch `/books` или parallel `/book` fallback каждые 1–2s в active window, 5–10s вне active window.
- `GammaMarketFeed`: market discovery, `clobTokenIds`, `outcomePrices`, `closed`, `acceptingOrders`, `umaResolutionStatus`.
- `ChainlinkTwapFeed`: RTDS official TWAP.
- `OracleSpotFeed`: Binance primary, Coinbase/Kraken backup.

Каждый feed пишет только raw events в очередь, не принимает торговых решений.

### 4.2 Normalizer

Нормализует всё в единый формат:

```python
@dataclass
class FeedEvent:
    source: Literal["clob_ws", "clob_rest", "gamma", "rtds_twap", "binance", "coinbase", "kraken"]
    kind: Literal["book", "bba", "trade", "twap", "spot", "market", "resolution"]
    asset: str | None
    slug: str | None
    token_id: str | None
    source_ts: float | None
    recv_ts: float
    payload: dict
```

Важно хранить и `source_ts`, и локальный `recv_ts`, чтобы отличать устаревшее сообщение от свежего сообщения со старой биржевой меткой.

### 4.3 State stores

#### OracleStore

- хранит по каждому asset несколько источников spot;
- считает median / best-source;
- отдаёт `price`, `source`, `age`, `source_count`, `agreement_bps`.

#### TwapStore

- хранит official Chainlink TWAP отдельно от reconstructed TWAP;
- не смешивает official и fallback в одном поле;
- для каждой точки хранит `twap_source`, `coverage_pct`, `age`, `sanity_vs_spot`.

#### OrderBookStore

- хранит полный стакан с `hash`, `source`, `last_snapshot_ts`, `last_delta_ts`;
- применяет `price_change` как delta к state, а не только BBA;
- умеет сверять WS book с REST book по top-of-book и hash;
- если WS молчит, REST snapshot повышает confidence, но ставит `source=rest_fallback`.

#### MarketCatalogStore

- заранее подгружает текущий и следующий интервал;
- подписывает токены next interval до начала окна;
- хранит `acceptingOrders`, `closed`, `endDate`, `cryptoMarketConfig.twapLookbackSeconds`.

### 4.4 DataQualityGate

Перед strategy check выдаёт `CleanSnapshot` только если:

- `oracle_age <= 1.5–3.0s`;
- `twap_age <= 3.0s` или reconstructed coverage >= 70–80%;
- `abs(official_twap / spot - 1) <= 2%`;
- `abs(twap_open / spot_open - 1) <= 2%`;
- best ask из WS и REST расходятся не больше 1–2 ticks, либо один источник явно свежее;
- full book не старше 5–10s в entry window;
- side semantics проверены: BUY->ask, SELL->bid;
- рынок `acceptingOrders=true`, `closed=false`, `end_ts` совпадает со slug;
- нет frozen 50/50 и нет one-sided book без достаточной глубины.

Пример результата:

```python
@dataclass
class CleanSnapshot:
    asset: str
    slug: str
    stc: float
    oracle_price: float
    oracle_source: str
    oracle_age: float
    twap: float
    twap_open: float
    twap_source: str
    twap_age: float
    twap_coverage: float
    up_book: OrderBook
    down_book: OrderBook
    quality_score: float
    reject_reasons: list[str]
```

Стратегия должна работать только с `CleanSnapshot`, а не напрямую с разрозненными stores.

## 5. Резервные источники данных

### Polymarket / CLOB

- Primary: CLOB market websocket.
- Fallback: `/book` или batch `/books` для полного стакана.
- Fallback top-of-book: `/price`, `/midpoint`, `/spread`.
- История: `/prices-history` для post-mortem и backfill.

### Polymarket / Gamma

- Market discovery: `/markets?slug=...` или `/markets/slug/{slug}`.
- Resolution: `outcomePrices`, `closed`, `umaResolutionStatus`, `eventMetadata.finalPrice/priceToBeat`.

### Oracle/spot

- Primary execution-fast feed: Binance aggTrade/miniTicker.
- Backup: Binance data-stream, Binance REST klines/ticker.
- Additional backups: Coinbase Advanced Trade WS ticker, Kraken WS ticker, CryptoCompare/CoinGecko only as slow sanity fallback.
- For settlement logic: official Chainlink TWAP must remain authoritative when sane; spot exchanges only sanity-check and reconstruct fallback.

## 6. Connection resilience rules

1. Не делать тяжёлые расчёты в WS receive loop: parse → update state → enqueue metrics; strategy runs separately.
2. Text `PING` every 5–10s + protocol ping.
3. Exponential backoff with jitter, but with circuit breaker per host.
4. Dynamic subscribe/unsubscribe without reconnect.
5. TTL subscriptions by market end time, not fixed 15 minutes only.
6. On reconnect:
   - resubscribe active tokens;
   - immediately fetch REST `/book` snapshots;
   - compare first WS snapshot with REST snapshot.
7. Track metrics:
   - `last_msg_age`, `book_age`, `oracle_age`, `twap_age`;
   - `ws_reconnects`, `rest_fail_rate`, `source_disagreement_bps`;
   - `twap_sanity_failed_count`, `book_crosscheck_failed_count`.
8. If quality score below threshold, strategy must skip, not degrade silently.

## 7. Следующие шаги реализации

1. Вынести `DataQualityGate` и `CleanSnapshot` в `backend/app/marketdata/quality.py`.
2. Разделить `LivePriceStore` на `OracleStore`, `TwapStore`, `OrderBookStore`, `MarketCatalogStore`.
3. Добавить batch REST CLOB polling с `/books`, если endpoint доступен, иначе parallel `/book`.
4. Добавить Gamma result cache для всех `cross_disagree`, `twap_only`, `unresolved` markets.
5. Изменить `TWAPInertiaStrategy.check()` так, чтобы он принимал `CleanSnapshot`, а не сам доставал данные из stores.
6. Расширить sample logs: `source`, `twap_source`, `book_source`, `quality_score`, `reject_reasons`, `gamma_outcome`, `acceptingOrders`.
7. После 7–14 дней новых логов перезапустить WFO с тем же `tools/demo_log_wfo.py` и сравнить распределения quality failures до/после.
