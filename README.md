# Polymarket UP/DOWN Bot — Web Application (v6.0)

Высокочастотный торговый бот и веб-платформа для 5-минутных бинарных рынков Polymarket UP/DOWN (`BTC`, `ETH`, `SOL`, `XRP`). Включает движки реальной и демо-торговли, математическую стратегию **TWAP Inertia & Barrier Lock** (для пост-августовского 60s TWAP-механизма Polymarket), сквозную аналитику, непрерывный сбор данных и walk-forward оптимизацию.

---

## ⚡ Быстрый старт

```bash
cd Polybot2

# 1. Конфигурация
cp config/.env.example config/.env
#    → укажите POLYMARKET_PRIVATE_KEY и POLYMARKET_FUNDER_ADDRESS

# 2. (Опционально) Paper trading — симуляция исполнения ордеров без отправки в блокчейн
#    в config/.env:  POLY_PAPER_TRADING=true

# 3. Установка зависимостей и запуск
pip install -r backend/requirements.txt
cd backend && uvicorn app.main:app --host 0.0.0.0 --port 8000

# 4. Открыть дашборд в браузере
open http://localhost:8000
```

### Запуск 24/7 (Docker Compose)
```bash
docker compose up -d --build
```

---

## 🏛 Архитектура проекта

```
Polybot2/
├── config/.env                   ← Все параметры конфигурации и API-ключи
├── backend/app/
│   ├── main.py                   ← Точка входа FastAPI, инициализация сервисов
│   ├── config.py                 ← Pydantic Settings (~130 валидируемых параметров)
│   ├── domain/                   ← Доменные модели: Position, TradeStats, enums, resolution
│   ├── core/                     ← Асинхронный HTTP (AsyncHTTP), структурированное логирование
│   ├── marketdata/               ← LivePriceStore, FillStore, WebSocketManager, BookPoller
│   ├── execution/                ← CLOB API клиент, OrderExecutor, round_size, каскадный SL
│   ├── risk/                     ← RiskManager — единый центр риск-контроля и сайзинга
│   ├── strategies/               ← TWAPInertia, VacuumScalp, FavDip, PairFirst, Longshot
│   ├── engine/                   ← Live TradingEngine (главный цикл), PositionMonitor, BalanceManager
│   ├── demo/                     ← DemoEngine — изолированная виртуальная торговля на живых фидах
│   ├── sample_io.py              ← Асинхронный буфер AsyncSampleBuffer (TSV / JSONL рекордер)
│   ├── api/                      ← REST & WebSocket эндпоинты дашборда
│   └── db/database.py            ← SQLite/SQLAlchemy — персистентная история сделок
├── frontend/static/              ← Веб-дашборд (HTML5, Vanilla JS, Chart.js, Tailwind)
├── tests/                        ← Полный тестовый сьют (117 тестов, pytest)
└── storage/                      ← База данных SQLite и файловые логи
```

---

## 🎯 Стратегия: TWAP Inertia & Barrier Lock

После перехода Polymarket на расчет цены закрытия через **60-секундный скользящий TWAP Chainlink** (trailing 60s TWAP до экспирации), исход рынка определяется интегралом цены за последнюю минуту интервала.

### 1. Математический барьер и TWAP-инерция
Бот непрерывно отслеживает накопленный TWAP оракула с момента `STC = 60s`. В окне входа (`STC ∈ [10s, 35s]`):
- Рассчитывается минимальное ценовое движение оракула (**TWAP Barrier**), необходимое для изменения знака исхода рынка:
  $$\text{Barrier} = \Delta \text{TWAP}_{\text{acc}} \times \frac{t_{\text{elapsed}}}{t_{\text{remaining}}}$$
- Если $\text{Barrier} \ge \text{twap\_min\_barrier\_pct}$ (по умолчанию $\ge 0.070\%$), математически исход необратим при стандартной волатильности базового актива.

### 2. Защита от замороженных стаканов (Frozen Book Guard)
- Отсекает неликвидные или залипшие маркеты, где цены токенов застыли на $0.50 / $0.51 при значительном отклонении оракула.
- Проверяет активность и глубину стакана на стороне лидера.

### 3. Кусочно-непрерывный интеграл TWAP и контроль покрытия фида
- Функция `compute_time_weighted_twap` вычисляет точный кусочно-постоянный интеграл Римана по фактическим временным интервалам между тиками оракула.
- При отсутствии официального потока RTDS бот требует $\ge 70\%$ подтвержденного временного покрытия окна (`twap_min_coverage_pct = 0.70`), блокируя входы при пропуске тиков.

---

## 🛡 Риск-менеджмент и исполнение ордеров

### 1. Жесткое ограничение ставки (Hard Stake Cap)
- Исключает превышение целевого процента баланса (`max_stake_ratio = 20%`).
- Расчет максимального бюджета ставки выполняется с точностью до цента с округлением строго вниз (`ROUND_DOWN`):
  $$\text{max\_stake} = \lfloor \text{effective\_balance} \times \text{max\_stake\_ratio} \rfloor_{\text{cents}}$$
- Итеративный guard в `_enter()` уменьшает объем ордера, гарантируя $\text{cost} \le \text{max\_stake}$.

### 2. Мульти-активное ранжирование возможностей (Multi-Asset Opportunity Ranking)
- На каждом шаге цикла бот параллельно сканирует все настроенные активы (`BTC`, `ETH`, `SOL`, `XRP`).
- Все валидные сигналы сортируются по убыванию силы барьера / величины отклонения:
  ```python
  opportunities.sort(
      key=lambda x: (
          x[3].extra.get("barrier_pct", 0.0) if hasattr(x[3], "extra") and x[3].extra else 0.0,
          abs(x[3].deviation or 0.0)
      ),
      reverse=True
  )
  ```
- Исполняется только самая надежная возможность с учетом портфельных лимитов (`max_concurrent_positions`).

### 3. Pre-Flight валидация стакана
Непосредственно перед отправкой ордера в CLOB выполняется моментальная проверка:
- Отмена, если `best_ask > 0.92` (максимально допустимая цена покупки) или спред расширился $> \$0.02$ от расчетной точки.
- Отмена, если ликвидность на уровне `best_ask` исчезла или упала ниже `twap_min_level_depth` (5 акций).

### 4. Динамическая комиссия Polymarket (Dynamic Taker Fee)
Расчет точной динамической комиссии тейкера:
$$\text{Fee} = C \times 0.07 \times p \times (1 - p)$$
где $C$ — количество акций, $p$ — цена исполнения.

### 5. Отложенный пост-аудит баланса (+60s Delay)
- По завершении каждого 5-минутного интервала бот ожидает 60 секунд для финализации ончейн-расчетов и обновления баланса на платформе.
- Выполняется аудит ожидаемого PnL против дельты кошелька; данные платформы являются безусловным источником истины (`ground truth`) и синхронизируют локальный баланс.

---

## 📡 Сетевая устойчивость и WebSocket

1. **Экспоненциальный бэкофф с джиттером**: Защита от rate-limit и блокировок Cloudflare при реконнектах.
2. **Активный Heartbeat PING**: Отдельный асинхронный таск отправки `PING` каждые 20 секунд в Polymarket RTDS.
3. **Батчинг подписок**: Подписки на токены отправляются пачками по $\le 50$ штук для предотвращения переполнения буфера сокетов.
4. **Непрерывное логирование срезов (`live_interval_samples.jsonl`)**: Высокочастотный сбор параметров стакана и оракула с точностью до 4 знаков для низкоценовых активов (`XRP`).

---

## 🧪 Тестирование

Запуск полного тестового сьюта:
```bash
PYTHONPATH=/home/user/packages:. python3 -m pytest tests/ -v
# 117 passed in ~1.8s
```

Включает тесты:
- `test_twap_inertia.py`: расчет кусочно-постоянного TWAP, проверка барьеров, отсечение замороженных стаканов, multi-asset ranking, pre-flight orderbook checks, AsyncSampleBuffer.
- `test_asset_selection.py`: многоактивные фиды, нормализация тикеров, динамические комиссии, аудит баланса.
- `test_risk.py`: портфельные лимиты, сайзинг ставок, аномалии исполнения, каскадный SL.
- `test_backtest.py`, `test_poly_fetcher.py`, `test_collector.py`, `test_optimizer.py`.

---

## ⚠️ Безопасность и эксплуатация

- Приватные ключи хранятся исключительно в файле `config/.env` (добавлен в `.gitignore`).
- Для безопасной отладки без риска реальных средств предусмотрены режимы `POLY_PAPER_TRADING=true` и изолированный **Демо-режим** (`DemoEngine`).
- Для промышленного деплоя рекомендуется запуск через Docker Compose под управлением reverse-proxy (Nginx / Caddy) с поддержкой SSL и Basic Auth.
