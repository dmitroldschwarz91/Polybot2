# Можно ли прогнозировать благоприятный режим XRP?

## Краткий ответ

Точный момент смены режима по имеющимся логам предсказать нельзя. Его можно **вероятностно оценивать и быстро детектировать**. Для текущих данных наиболее надёжная схема — не пытаться угадать фазу одним индикатором цены, а объединять:

1. ex-ante характеристики базового актива до входа;
2. качество текущего TWAP-сигнала;
3. rolling realised edge самой стратегии;
4. online change-point detector с состояниями `OFF / PROBE / ON`.

## Что показала дополнительная проверка

Параметры стратегии до и после reset одинаковы. На всех XRP 5m-интервалах не появилось обычной трендовости: доля одинакового знака соседних returns осталась около 49%, lag-1 correlation около нуля. Поэтому MACD/направление предыдущей свечи само по себе смену не предскажет.

В то же время на XRP-сделках после reset изменилось условное распределение:

- `|TWAP deviation|`: 0.0737% → 0.0863%;
- barrier: 0.0759% → 0.0972%;
- signed final move: 0.0153% → 0.0209%;
- entry price: 0.834 → 0.785;
- reversal/loss: 28.6% → 6.7%.

Это означает, что прогнозировать нужно не общий bull/bear/trend regime, а вероятность **продолжения конкретного late-TWAP импульса до resolution**.

## Кандидаты на leading indicators

По доступной части interval samples для XRP были рассчитаны только из прошлых данных rolling features за 1–12 часов. На этой маленькой выборке наиболее информативными оказались:

- абсолютный directional drift последних 12–24 интервалов;
- efficiency ratio за 12 часов;
- доля UP-интервалов и sign persistence;
- rolling PnL последних 5–10 XRP-сделок.

Одномерный in-sample AUC для признаков долгого окна достигал примерно 0.68–0.76, а для rolling PnL — 0.60–0.66. Это сигнал, что информация есть, но это **не OOS-доказательство**: выборка мала, часть старых samples уже отсутствует в текущей ротации логов, а признаки исследованы после наблюдения результата.

Интересная гипотеза: проигрыши чаще происходили после уже накопленного одностороннего drift, то есть late-TWAP сигнал попадал в локальное истощение/mean reversion. Победы чаще возникали в более сбалансированном предшествующем режиме, после чего появлялся свежий внутриминутный импульс. Это нужно проверить на новом непрерывном периоде.

## Предлагаемый online regime score

Для каждого актива раз в 5 минут считать только по данным, доступным к этому моменту:

### Base-market block

- realised volatility за 1h, 4h, 12h;
- directional drift за 1h/4h/12h;
- efficiency ratio `abs(sum returns) / sum(abs returns)`;
- sign persistence и lag-1 autocorrelation;
- долю возвратов/разворотов после движения последней минуты;
- relative volatility актива к BTC/ETH и cross-asset dispersion.

### Signal block

- `abs(deviation_twap_pct)`;
- barrier и его скорость изменения;
- STC;
- token ask и потенциальный payoff `(1-p)/p`;
- расхождение TWAP direction с движением token price;
- изменение ask за последние 5–15 секунд;
- book depth, spread, frozen/stale flags.

### Realised-strategy block

- rolling WR, PnL и expected value за 5/10/20 последних сделок;
- reversal rate;
- signed final TWAP margin;
- calibration: фактический WR против среднего entry probability;
- Bayesian posterior для win probability и EV.

Затем оценивать не `will XRP go up`, а:

```text
P(signal direction survives until resolution | current regime, signal, book)
```

и переводить её в ожидаемую доходность с учётом цены и fee.

## Безопасная state machine

- `OFF`: актив не торгуется, только собираются virtual trades.
- `PROBE`: минимальный stake 2–5%, когда posterior EV стал положительным, но уверенность мала.
- `ON`: обычный stake только если нижняя граница Bayesian EV > 0, rolling reversal rate ниже лимита и нет data-quality alarms.
- Возврат в `OFF`: CUSUM/change-point alarm, отрицательный rolling EV, рост reversal rate или проблемы feed/book.

Для нынешней выборки разумные стартовые правила для тестирования, но не для немедленного live:

```text
PROBE:
  last_10_virtual_trades WR >= 0.80
  last_10_virtual_trades PnL > 0
  reversal_rate_10 <= 0.20
  current barrier >= 0.07%
  expected payoff after fee > 0

ON:
  не менее 15 virtual/probe trades
  Bayesian P(EV > 0) >= 0.90
  reversal_rate_15 <= 0.15
  feed quality OK
```

Нельзя включать режим только по WR: при ask около 0.90 требуемый break-even WR после fee очень высокий. Основной критерий должен быть EV/PnL с учётом entry price.

## Можно ли было предсказать именно reset-фазу заранее?

По текущим данным — нет доказательств. Между runs есть разрыв 10 ч 35 мин, поэтому начало режима не наблюдалось. Первые четыре доступные XRP-сделки нового run включают две победы и две потери; уверенный rolling сигнал появляется лишь после последующей серии побед. То есть detector смог бы **подтвердить режим с задержкой**, но не гарантированно включиться до его начала.

Это всё равно полезно: virtual shadow execution позволяет обнаруживать фазу без риска, затем включать малый stake и повышать его только после подтверждения.

## Что нужно логировать для полноценной модели

Текущая ротация sample-файлов уже удалила часть интервалов, соответствующих `demo_results`. Для честного прогнозного исследования нужно сохранять immutable dataset:

- все 5m bars и final-60 samples;
- features snapshot непосредственно перед каждым решением;
- virtual signal даже когда актив `OFF`;
- ask trajectory, spread/depth/staleness;
- Gamma-confirmed outcome;
- regime score и model version.

После 2–4 недель данных выполнить anchored/rolling walk-forward: обучать detector только на прошлом, выбирать threshold на train и оценивать включение/выключение и PnL на следующем блоке. Отдельно учитывать latency детектора и сделки, пропущенные до подтверждения режима.

## Итог

Благоприятную фазу XRP, вероятно, можно **детектировать и эксплуатировать**, но текущие данные не доказывают возможность заранее предсказать точку её начала. Наиболее реалистичный путь — virtual trades + Bayesian EV + change-point detection. Цена/доходность сигнала и reversal rate важнее общего рыночного тренда. До получения настоящего walk-forward подтверждения такой detector должен управлять только `OFF/PROBE`, а не автоматически разрешать полный live stake.
