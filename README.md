# Polymarket UP/DOWN Bot — Web Application (v6.0)

Рефакторинг скрипта `polybot v5.7.3` в полноценное веб-приложение с дашбордом
и централизованным управлением рисками. Торговая логика **сохранена без
изменений поведения**; изменилась только архитектура.

---

## ⚡ Быстрый старт

```bash
cd polymarket_bot

# 1. Конфигурация
cp config/.env.example config/.env
#    → впишите POLYMARKET_PRIVATE_KEY и POLYMARKET_FUNDER_ADDRESS

# 2. (опционально) тест без денег — дашборд и стратегии работают, ордера симулируются
#    в config/.env:  POLY_PAPER_TRADING=true

# 3. Установка и запуск
pip install -r backend/requirements.txt
cd backend && uvicorn app.main:app --reload --port 8000

# 4. Открыть дашборд
open http://localhost:8000
```

### Docker (24/7)
```bash
docker compose up -d --build
```

---

## 🏛 Архитектура

```
polymarket_bot/
├── config/.env              ← ВСЕ параметры и ключи (внешние)
├── backend/app/
│   ├── main.py              ← FastAPI-приложение, точка входа
│   ├── config.py            ← Pydantic Settings (валидация, ~120 параметров)
│   ├── domain/              ← чистые модели: Position, TradeStats, enums
│   ├── core/                ← логирование (SLog), HTTP-сессии
│   ├── marketdata/          ← LivePriceStore, FillStore, 4× WebSocket, поиск маркетов
│   ├── execution/           ← обёртка CLOB-клиента, buy/sell, каскадный SL
│   ├── risk/                ← ★ RiskManager — единый центр риск-решений
│   ├── strategies/          ← VacuumScalp, EarlyTrend, Standard (по приоритету)
│   ├── engine/              ← TradingEngine (главный цикл), Monitor, Balance
│   ├── api/routes.py        ← REST + WebSocket для дашборда
│   ├── api/data_routes.py    ← сбор данных (collector) + инвентарь кеша
│   ├── api/optimizer_routes.py ← walk-forward оптимизация
│   ├── backtest/collector.py ← ★ continuous-рекордер свежих интервалов
│   ├── backtest/optimizer.py ← ★ walk-forward оптимизация параметров
│   └── db/database.py       ← SQLite/SQLAlchemy — история сделок
├── frontend/static/         ← дашборд (чистый HTML/JS, без сборки)
├── tests/test_risk.py       ← 18 тестов риск-модуля (всё зелёное)
├── Dockerfile + compose     ← запуск 24/7
└── storage/bot.db           ← БД (появляется при первом запуске)
```

**Поток данных:** `WebSocket → LivePriceStore → Strategy.check() → RiskManager.can_open_new() → Execution → Position → Monitor → RiskManager.evaluate() → close`

---

## 🛡 Управление рисками (ключевое изменение)

В старом скрипте расчёт стоп-лосса дублировался в **4 функциях**:
`check_and_handle_urgent_sl`, `check_sl_inline`, `monitor_positions_async`,
`execute_cascading_sl_sell`. Теперь всё в одном классе `RiskManager` —
любое изменение правила применяется ко всем позициям сразу и тестируется.

### Что перенесено из оригинала
| Механизм | Где живёт теперь |
|---|---|
| Каскадный SL (FAK → GTC-chase → nuclear) | `execution/orders.py: execute_cascading_sl` |
| Nuclear crash exit | `risk/manager.py: is_nuclear()` |
| Trailing stop после partial TP | `domain/models.py: update_trailing()` |
| Fill-anomaly → мгновенный выход | `risk/manager.py: is_fill_anomaly()` |
| Pre-entry SL guard | `risk/manager.py: evaluate()` + engine |
| Конкурентный SL во время `wait_for_fill` | встроен в monitor/engine |

### Что добавлено (NEW — портфельные лимиты)
Этих защит в оригинале **не было** — бот мог открывать позиции, пока терял деньги:

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `MAX_CONCURRENT_POSITIONS` | 2 | Жёсткий лимит одновременных позиций |
| `MAX_DAILY_LOSS_PCT` | 20% | Остановка торговли при убытке за день |
| `MAX_DRAWDOWN_PCT` | 35% | Kill switch от стартового баланса |
| Кнопка **Halt/Resume** в дашборде | — | Ручная пауза входов в один клик |

Все триггеры централизованы в `RiskManager.can_open_new()`, который вызывается
**перед каждым входом**.

---

## 📊 Дашборд

- **Live-статус** через WebSocket (баланс, P&L, win-rate, аптайм)
- **График кумулятивного P&L** по истории сделок из БД
- **Открытые позиции** (вход, TP, SL, нереализованный P&L)
- **Панель рисков** (статус halt, пик баланса, дневной P&L, заполненность лимита позиций)
- **Оракул-цены** (BTC/ETH: значение, источник, возраст Chainlink)
- **История сделок** с фильтрами по причине закрытия
- Вкладка **📦 Данные**: continuous-сбор, backfill, walk-forward оптимизация, инвентарь кеша
- Кнопки: **Старт / Стоп** бота, **Halt / Resume** рисков

---

## 🔧 Настройка стратегий

Все параметры — в `config/.env` с префиксом `POLY_`:

```bash
POLY_VACUUM_SCALP_ENABLED=true          # активная стратегия
POLY_VACUUM_SCALP_TP_DELTA=0.02         # тейк-профит +2 цента
POLY_VACUUM_SCALP_SL_PCT=0.10           # стоп-лосс -10%
POLY_VACUUM_SCALP_MAX_STAKE_RATIO=0.72  # max доля баланса на вход
```

---

## 🗺 Карта миграции (старый скрипт → новое приложение)

| Было (монолит) | Стало |
|---|---|
| ~120 констант module-level | `config.py: Settings` (из `.env`) |
| `Position`, `TradeStats`, `BalanceState` | `domain/models.py` |
| глобальные `prices`, `fills` | инжектируются в `TradingEngine` |
| `run_*_websocket` (4 шт.) | `marketdata/websockets.py: WebSocketManager` |
| `analyze_market`, `track_oracle_price` | `marketdata/markets.py` |
| `execute_buy/sell/cascading_sl` | `execution/orders.py: OrderExecutor` |
| SL-логика в 4 местах | **`risk/manager.py: RiskManager`** (1 место) |
| `check_vacuum_scalp_opportunity` | `strategies/vacuum_scalp.py` |
| `check_early_trend_opportunity` | `strategies/early_trend.py` |
| `main_async` (300+ строк) | `engine/bot.py: TradingEngine._run()` |
| нет персистентности | `db/database.py` (SQLite) |
| нет интерфейса | `frontend/` + `api/routes.py` |

---

## ✅ Тесты

```bash
cd polymarket_bot && python -m pytest tests/ -v
# 74 passed:
#   - test_risk.py          (18) — риск-модуль
#   - test_backtest.py      (14) — симулятор/метрики
#   - test_poly_fetcher.py  (17) — реальные данные Polymarket
#   - test_collector.py     (10) — сборщик данных в реальном времени
#   - test_optimizer.py     (15) — walk-forward оптимизация
```

---

## 🧪 Бэктест стратегий

Полноценный фреймворк для проверки стратегий **до** реальных денег. Переиспользует
**реальную логику решений** стратегий (те же пороги из Settings/RiskManager), но
симулирует исполнение пессимистично: покупка по ask, продажа по bid, комиссия
Polymarket 2%, проскальзывание.

### Из CLI
```bash
cd polymarket_bot/backend

# сравнить стратегии на синтетическом тренде
python run_backtest.py --compare vacuum_scalp spread_capture early_trend \
    --capital 7 --duration 10800 --volatility 0.00006 --drift 0.000015 --sl 0.06

# на реальном оракуле Binance + модельный стакан
python run_backtest.py --strategy vacuum_scalp --mode historical \
    --start 1735689600 --end 1735776000

# 🎯 на РЕАЛЬНЫХ данных Polymarket (цены токенов + winners)
python run_backtest.py --mode poly --compare vacuum_scalp spread_capture \
    --start 1771168800 --end 1771183200 --fidelity 1

# оценить валовое edge без комиссий
python run_backtest.py --strategy vacuum_scalp --no-fees
```

### Из дашборда
Вкладка **🧪 Бэктест** → выбор стратегии, капитала, SL, режима данных →
«Запустить» или «Сравнить все». Кривая капитала, метрики (Sharpe, Sortino, PF,
max drawdown, влияние комиссий) и список сделок отображаются в реальном времени.

### Три режима данных
| Режим | Что используется | Когда |
|---|---|---|
| `synthetic` | Сгенерированный путь цены (модельный стакан) | Быстрые тесты логики, без сети |
| `historical` | Реальный путь цены BTC/ETH (Binance klines) + модельный стакан | Проверка поведения на реальном оракуле |
| **`poly`** ★ | **Реальные цены токенов UP/DOWN + реальные winners + реальный оракул** | Максимально честный бэктест |

### Архитектура бэктеста
| Модуль | Назначение |
|---|---|
| `backtest/data.py` | Загрузка/кеш klines Binance (+ geo-fallback) + синтетика |
| `backtest/poly_fetcher.py` | ★ Реальные данные: Gamma (winners) + Data API trades (суб-сек) + CLOB fallback |
| `backtest/simulator.py` | Симулятор биржи с комиссиями и проскальзыванием |
| `backtest/engine.py` | Движок: реальные или модельные книги через реальную логику стратегий |
| `backtest/metrics.py` | Sharpe, Sortino, max DD, profit factor, fee drag |

### 🎯 Режим `poly` — реальные данные Polymarket (главное обновление)
Бэктест больше **не моделирует** цены токенов. Режим `poly` скачивает:
- **Gamma API** (`/events/slug/btc-updown-5m-{epoch}`) — token IDs, реальный
  winner (`outcomePrices`), объём, границы интервала;
- **CLOB** (`/prices-history?market={token_id}&startTs=&endTs=&fidelity=`) —
  реальную историю цен токенов UP/DOWN с минутной гранулярностью;
- **Binance klines** — реальный путь цены BTC (оракул, определяющий резолвцию).

Каждый интервал кешируется отдельно → данные накапливаются, повторные
прогоны мгновенны.

```bash
# бэктест на РЕАЛЬНЫХ данных Polymarket (4 часа BTC 5-мин рынков)
python run_backtest.py --mode poly --compare vacuum_scalp spread_capture \
    --start 1771168800 --end 1771183200 --fidelity 1 --sl 0.06
```

### 🔬 Два источника цен токенов (настройка плотности)

| Источник | Точек на 5-мин рынок | Разрешение | Когда |
|---|---|---|---|
| `trades` (по умолчанию) | **~3300** | суб-секундное | Честный бэктест: fill по реальным ценам сделок |
| `prices` | ~5–10 | ~1/мин | Быстро, без пагинации (fallback) |

`trades` берёт каждую отдельную сделку из **Data API** (`data-api.polymarket.com/trades`),
где таймстампы в секундах. На ликвидном BTC-рынке это измеренные **336× больше
данных** (10 067 точек против 30). Так как это реальные цены исполнения, бэктест
заполняет ордера по настоящим trade-prices, а не моделированным mid.

```bash
# выбор источника (через env, CLI или дашборд)
POLY_BACKTEST_TOKEN_SOURCE=trades   # или prices
```

Когда активны `trades`, движок автоматически **уплотняет оракул до 1-секундного
разрешения** (линейная интерполяция минутных Binance-klines), иначе плотные
trade-цены схлопывались бы в ~6 lookup'ов. Результат: 301 шаг на интервал
вместо 6, каждое реальное движение цены токена учитывается.

> ⚠️ **Остаточные ограничения:** таймстампы сделок — в секундах (не мс); в пиковые
> моменты несколько сделок на одну секунду. Data API требует пагинации (500/стр,
> ~7 страниц на ликвидный рынок) и User-Agent + rate-limit. Для суб-секундного
> live-данных нужен **WebSocket market channel** (есть в рекордере).

### Метрики
final equity, return%, win rate, profit factor, expectancy, max drawdown,
Sharpe, Sortino, total fees, **fee drag %** (доля комиссий в валовом профите —
ключевая метрика для малобюджетной торговли).

---

## 📦 Сбор данных в реальном времени

Continuous-рекордер накапливает свежие resolved-интервалы автоматически — кеш
растёт сам, и walk-forward оптимизация всегда работает с актуальными данными.

**Как работает:** после закрытия каждого интервала (5 мин) бот ждёт ~75с (до
резолвции Polymarket), затем скачивает Gamma + CLOB + Binance данные и кеширует.
Повторные запросы мгновенны (idempotent). Полностью независим от торгового
движка — можно писать данные при остановленном боте.

```bash
# CLI backfill — докачать диапазон прошлых интервалов
# (через дашборд: вкладка 📦 Данные → Backfill)
```

### API
| Эндпоинт | Действие |
|---|---|
| `POST /api/collector/start` | Запустить непрерывную запись |
| `POST /api/collector/stop` | Остановить |
| `POST /api/collector/backfill` | Докачать диапазон (asset, start, end) |
| `GET /api/collector/status` | Статус: собрано, ошибки, следующий сбор |
| `GET /api/data/inventory` | Что в кеше (по активам: кол-во, resolved, объём) |

```bash
# автозапуск при старте приложения
AUTOSTART_COLLECTOR=1
```

---

## ⚖ Walk-forward оптимизация

Золотой стандарт валидации параметров без переобучения: данные делятся на
скользящие окна, параметры подбираются на **train**, а проверяются на следующем
**незримом test**. Если train ≫ test — параметры переобучены и не выживут в live.

```
[train: оптимизация] [test: проверка] →
                     [train] [test] →
                              [train] [test] → ...
```

### Что оптимизируется
Grid/random-search по стратегии-релевантному пространству:
- **vacuum_scalp**: SL %, TP delta, спред стакана
- **early_trend**: SL %, спред стакана

Ранжирование по score = `return − 0.5·drawdown + min(trades,10)` (штраф за
глубокую просадку и малое число сделок).

### Ключевые метрики результата
- **In-sample vs Out-of-sample** — главное сравнение
- **Overfit warning** — автоматически, если train >0 а test ≤0
- **Консенсус-параметры** — какие значения выбирались чаще всего
- **Per-fold breakdown** — train/test по каждому окну + overfit ratio

### CLI
```bash
cd polymarket_bot/backend

# оптимизация на РЕАЛЬНЫХ данных (нужен кеш ≥ train+test интервалов)
python run_optimizer.py --strategy vacuum_scalp \
    --start 1771168800 --end 1771212000 --train 24 --test 12

# на синтетике (всегда есть сделки — для проверки самого оптимизатора)
python run_optimizer.py --synthetic --synth-hours 12 --train 24 --test 12
```

### Дашборд
Вкладка **📦 Данные** → «Walk-forward оптимизация» → выбор стратегии, размеров
окон, диапазона дат → «Запустить». Результат: in/out-of-sample метрики,
диагноз (оверфит/робастно), таблица по фолдам.

### Реальный результат (12ч BTC, 144 интервала, 10 фолдов)
```
In-sample:      -2.50%    ← что показал train
Out-of-sample:  -2.86%    ← что показал незримый test
Диагноз: ✓ робастно        ← train ≈ test → НЕ переобучено, просто нет edge
```
Стратегия теряет деньги **одинаково** на train и test → результат достоверен
(не переобучен), просто vacuum_scalp не имеет edge на этих данных. Именно для
такого честного вывода и нужен walk-forward.

---

## 🍖 Анализ стратегии gabagool + адаптация

Подробный разбор: `docs/GABAGOOL_ANALYSIS.md`. Кратко:

**Что делает gabagool** — хеджированный арбитраж на бинарных UP/DOWN рынках:
покупает обе стороны асинхронно, держит среднюю пару (UP+DOWN) ниже $1, тогда
одна сторона всегда платит $1 → «гарантированная» прибыль.

**Жёсткая правда** (по данным обратного инжиниринга): его средняя пара ~$1.015
(НАД $1), сбалансированные пары теряют деньги, реальный edge — в
маркет-мейкинге + направленном экспоужре. Чистый арбитраж конкурируется за
миллисекунды и съедается 2–3% комиссией.

**Адаптация под малый бюджет** — реализована стратегия **SpreadCapture**
(Hedge-Lite, «Уровень 1»): после направленного входа (vacuum scalp)
опционально докупает противоположную сторону, когда `up_ask + down_ask < 0.97`
с запасом под комиссию, превращая ставку в частично нейтральную к риску пару.
Полноценный маркет-мейкинг (Уровень 2) требует $50+; чистый арбитраж (Уровень 3) — $200+.

```bash
# включить hedge-lite в config/.env
POLY_SPREAD_CAPTURE_ENABLED=true
POLY_SPREAD_CAPTURE_PAIR_THRESHOLD=0.97
POLY_SPREAD_CAPTURE_MIN_EDGE=0.03
```
Сначала **обязательно** прогнать через бэктест с комиссиями.

---



---

## 🎮 Демо-режим — живой рынок, виртуальные деньги

Бот подключается к **реальным** WebSocket-фидам (Chainlink, Binance, Polymarket
market channel) и торгует **виртуальным** капиталом. Входы, цены, резолвция —
реальные. Деньги — нет. API-ключ не нужен.

### Что моделируется
- **Входы** по реальным ценам order book (fill = best_ask + slippage)
- **Компаундинг** капитала: выигрыши растят ставку, проигрыши — уменьшают
- **Резолвция** по реальному winner из Gamma API
- **HOLD-режим**: удержание до закрытия рынка (TP/SL проигрывает — см. walk-forward)

### Настройки по умолчанию (из walk-forward анализа)
- Стартовый капитал: **$15**
- Порог входа: **0.75**
- Stake ratio: **30%** (≈ min-order floor при $15)
- Режим: **HOLD** (удержание до резолвции)

### Запуск
```bash
# дашборд → вкладка 🎮 Демо → "Старт демо"
# или API:
curl -X POST http://localhost:8000/api/demo/start   -H 'Content-Type: application/json'   -d '{"start_capital": 15, "threshold": 0.75, "stake_ratio": 0.30}'
```

### API
| Эндпоинт | Действие |
|---|---|
| `POST /api/demo/start` | Запуск (с настройками) |
| `POST /api/demo/stop` | Стоп |
| `GET /api/demo/status` | Виртуальный капитал, позиции, цены |
| `GET /api/demo/trades` | История виртуальных сделок |
| `WS /ws/demo` | Live-обновления каждые 1.5с |

## ⚠️ Безопасность

- Ключи **только** в `config/.env` (в `.gitignore`)
- `POLY_PAPER_TRADING=true` — полный прогон без реальных ордеров
- Дашборд слушает `0.0.0.0` — для продакшена ставьте reverse-proxy с auth
