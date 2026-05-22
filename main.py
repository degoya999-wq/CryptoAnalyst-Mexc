import ccxt
import requests
import time
import os
import logging
from datetime import datetime
from flask import Flask
import threading
import numpy as np

# ─────────────────────────────────────────────
# Binance v7.2: фикс циклических банов 418
# - НОВОЕ: глобальный circuit breaker IP_BAN_UNTIL (потокобезопасный)
#   Один 418 → весь бот замирает до точного timestamp из ответа Binance.
#   Парсим "banned until <ms>" из текста ошибки. Гибридное ожидание:
#   <3 мин → ждём активно, ≥3 мин → пропускаем итерацию.
# - НОВОЕ: контроль X-MBX-USED-WEIGHT-1m из last_response_headers.
#   Если weight ≥ 1800/2400 за минуту — превентивный sleep до новой минуты.
# - limit 4H: 100 → 99 (weight 2→1 на Binance, экономия ~600 weight/итерацию).
#   ATR Map baseline 50 и REVERSAL lookback 10 не задеты — запас 30 свечей.
# - rateLimit: 200мс → 280мс (3.57 req/sec, итерация ~9-10 мин).
# - market_context, fetch_tickers, fetch_funding_rates, 1H reversal —
#   все обёрнуты проверкой IP_BAN_UNTIL перед вызовом.
# - Логика ATR Map / is_sq / скоринга / порогов НЕ ИЗМЕНЕНА — только инфраструктура API.
#
# Binance v7.1: правки после первого деплоя
# - limit 4H снижен с 120 до 100 (weight 2→1, фикс 429 Too Many Requests)
# - rateLimit 200мс между запросами (повышен с 100мс после получения ~100 банов 418 за 5ч)
# - helper fetch_ohlcv_with_retry с обработкой 418/429/Network
# - OI временно отключён (Binance не отдаёт OI в fetch_tickers)
# - Эмодзи к Vol текущей 2H (🔇/⚪/⚡/🚨)
# - Грамматика "1 бар / 2 бара / 5 баров"
# - Убран дубликат старого MEXC-текста про "Сжатие: 3 св | наклон | Готов"
#
# Binance v7.0: миграция с MEXC, новый блок СЖАТИЕ с EMA + скоринг направления
# - Все активные USDT-perp Binance (динамический список ~577)
# - Прямой fetch_ohlcv('2h') без склейки 1H→2H
# - Funding rate из массового запроса
# - Скоринг направления взрыва по 5 факторам (после v7.1)
# - Сортировка алертов СЖАТИЕ по ATR Map score DESC
# v6.2 (MEXC): новый блок REVERSAL_4H + ATR Map mature/forming
# ─────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════
# Встроенный модуль REVERSAL_4H (раньше был отдельным файлом reversal_4h.py)
# Логика: 5 жёстких слотов для разворотных сигналов на 4H
# Все имена внутри начинаются с _rev_ или REV_ чтобы не было конфликтов
# ═══════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────
# КОНСТАНТЫ
# ─────────────────────────────────────────────────────────────────────
REV_WEEK_BARS_4H        = 42      # 7 дней × 6 свечей/день
REV_EXTREME_ZONE_PCT    = 5.0     # цена в нижних/верхних 5% диапазона
REV_MIN_RANGE_PCT       = 8.0     # размах недели должен быть ≥ 8% (иначе нет от чего отскакивать)

REV_VETO_RECENT_3BARS_PCT = 12.0  # за последние 3 свечи 4H движение не более 12% (иначе нож)
REV_VETO_CAPITULATION_PCT = 5.0   # текущая 4H свеча: если -5% и объём ×3 — НЕ входим
REV_VETO_BTC_CH_4H        = 2.0   # |BTC ch| ≥ 2% за 4H — макрорежим, режем альты против BTC

# Цели и стопы (горизонт сделки 2-5 дней)
REV_MIN_TP1_PCT         = 1.5     # минимум 1.5% до TP1, иначе не интересно
REV_MAX_TP1_PCT         = 7.0     # максимум 7% (за 5 дней реалистично)
REV_MIN_RR              = 1.8
REV_ATR_STOP_BUFFER     = 0.3     # стоп = за экстремум + 0.3 ATR
REV_ATR_CAP_MULT        = 4.0     # потолок TP = current ± 4*ATR (если Fibo дальше)

# Объём
REV_MIN_VOLUME_24H      = 5_000_000
REV_TRIGGER_VOL_MULT    = 1.5     # объём 1H триггерной свечи должен быть ≥ avg × 1.5


# ─────────────────────────────────────────────────────────────────────
# БАЗОВЫЕ ИНДИКАТОРЫ (минимум, без зависимости от старого кода)
# ─────────────────────────────────────────────────────────────────────
def _rev_atr(ohlcv, period=14):
    if len(ohlcv) < period + 1:
        return None
    trs = []
    for i in range(1, len(ohlcv)):
        h, l, pc = ohlcv[i][2], ohlcv[i][3], ohlcv[i-1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = np.mean(trs[:period])
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def _rev_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    d = np.diff(closes)
    g = np.where(d > 0, d, 0.0)
    l = np.where(d < 0, -d, 0.0)
    ag = np.mean(g[:period])
    al = np.mean(l[:period])
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    if al == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + ag / al))


def _rev_cvd_proxy(ohlcv):
    """Возвращает массив накопительного дельта-объёма (упрощённо)."""
    cum = 0.0
    out = []
    for c in ohlcv:
        h, l, cl, v = c[2], c[3], c[4], c[5]
        ratio = (cl - l) / (h - l) if h != l else 0.5
        cum += (ratio - 0.5) * 2 * v
        out.append(cum)
    return out


# ─────────────────────────────────────────────────────────────────────
# СЛОТ 1: НЕДЕЛЬНЫЙ ЭКСТРЕМУМ
# ─────────────────────────────────────────────────────────────────────
def _rev_check_weekly_extreme(closed_4h, mode='long'):
    """
    Проверяет: цена в крайних 5% от 7-дневного диапазона + размах ≥ 8%.
    Возвращает (passed: bool, info: dict).
    """
    if len(closed_4h) < REV_WEEK_BARS_4H:
        return False, {}

    window = closed_4h[-REV_WEEK_BARS_4H:]
    week_high = max(c[2] for c in window)
    week_low  = min(c[3] for c in window)
    current   = closed_4h[-1][4]

    week_range = week_high - week_low
    if week_low <= 0:
        return False, {}

    range_pct = week_range / week_low * 100
    if range_pct < REV_MIN_RANGE_PCT:
        return False, {"reason": f"range_pct={range_pct:.1f}% < {REV_MIN_RANGE_PCT}%"}

    # положение в диапазоне (0% = на лоу, 100% = на хае)
    position_pct = (current - week_low) / week_range * 100

    if mode == 'long':
        # цена в нижних 5%
        if position_pct > REV_EXTREME_ZONE_PCT:
            return False, {"reason": f"position={position_pct:.1f}% > {REV_EXTREME_ZONE_PCT}%"}
        return True, {
            "week_high": week_high,
            "week_low":  week_low,
            "range_pct": range_pct,
            "position_pct": position_pct,
            "extreme":   week_low,
        }
    else:
        # цена в верхних 5% (position_pct ≥ 95)
        if position_pct < (100 - REV_EXTREME_ZONE_PCT):
            return False, {"reason": f"position={position_pct:.1f}% < {100-REV_EXTREME_ZONE_PCT}%"}
        return True, {
            "week_high": week_high,
            "week_low":  week_low,
            "range_pct": range_pct,
            "position_pct": position_pct,
            "extreme":   week_high,
        }


# ─────────────────────────────────────────────────────────────────────
# СЛОТ 2: ИСТОЩЕНИЕ ДВИЖЕНИЯ (на 4H)
# ─────────────────────────────────────────────────────────────────────
def _rev_check_exhaustion(closed_4h, mode='long'):
    """
    Минимум одно из:
    - RSI дивергенция
    - CVD дивергенция
    - Падающий объём на последних 3 свечах
    - Climax bar (объём ×2 + длинный фитиль)

    Возвращает (passed, list_of_signals).
    """
    if len(closed_4h) < 25:
        return False, []

    closes = [c[4] for c in closed_4h]
    highs  = [c[2] for c in closed_4h]
    lows   = [c[3] for c in closed_4h]
    vols   = [c[5] for c in closed_4h]

    signals = []

    # --- RSI дивергенция ---
    lookback = 10
    rsi_now = _rev_rsi(closes[-(lookback + 14):])
    rsi_past_window = []
    for i in range(lookback):
        end = len(closes) - lookback + i + 1
        rsi_past_window.append(_rev_rsi(closes[max(0, end - 30):end]))

    price_window = closes[-lookback:]
    if mode == 'long':
        # цена сделала новый минимум, RSI — нет
        if (closes[-1] <= min(price_window[:-1])
                and rsi_now > min(rsi_past_window[:-1])):
            signals.append(f"RSI bull div ({rsi_now:.0f})")
    else:
        if (closes[-1] >= max(price_window[:-1])
                and rsi_now < max(rsi_past_window[:-1])):
            signals.append(f"RSI bear div ({rsi_now:.0f})")

    # --- CVD дивергенция ---
    cvd = _rev_cvd_proxy(closed_4h)
    if mode == 'long':
        if (closes[-1] <= min(price_window[:-1])
                and cvd[-1] > min(cvd[-lookback:-1])):
            signals.append("CVD bull div")
    else:
        if (closes[-1] >= max(price_window[:-1])
                and cvd[-1] < max(cvd[-lookback:-1])):
            signals.append("CVD bear div")

    # --- Падающий объём на последних 3 свечах ---
    avg_vol_20 = np.mean(vols[-23:-3]) if len(vols) >= 23 else np.mean(vols[:-3])
    last3_vol  = np.mean(vols[-3:]) if len(vols) >= 3 else 0
    if avg_vol_20 > 0 and last3_vol < avg_vol_20 * 0.8:
        signals.append(f"vol fading (×{last3_vol/avg_vol_20:.2f})")

    # --- Climax bar в одной из последних 2 свечей ---
    for idx in (-1, -2):
        if abs(idx) > len(closed_4h):
            continue
        c = closed_4h[idx]
        h, l, op, cl, v = c[2], c[3], c[1], c[4], c[5]
        full_range = h - l
        if full_range <= 0 or avg_vol_20 <= 0:
            continue
        body = abs(cl - op)
        # вместо max(...) объём считаем относительно среднего
        vol_mult = v / avg_vol_20

        if mode == 'long':
            lower_wick = (min(op, cl) - l) / full_range
            if vol_mult >= 2.0 and lower_wick >= 0.5:
                signals.append(f"climax bar (vol ×{vol_mult:.1f}, wick {lower_wick*100:.0f}%)")
                break
        else:
            upper_wick = (h - max(op, cl)) / full_range
            if vol_mult >= 2.0 and upper_wick >= 0.5:
                signals.append(f"climax bar (vol ×{vol_mult:.1f}, wick {upper_wick*100:.0f}%)")
                break

    return len(signals) > 0, signals


# ─────────────────────────────────────────────────────────────────────
# СЛОТ 3: ТРИГГЕР НА 1H (после закрытия 1H свечи)
# ─────────────────────────────────────────────────────────────────────
def _rev_check_1h_trigger(ohlcv_1h, mode='long'):
    """
    Анализирует ПОСЛЕДНЮЮ ЗАКРЫТУЮ 1H свечу.
    Минимум одно из:
    - Hammer/Pin Bar + объём × ≥ 1.5
    - Engulfing
    - BB-style: предыдущая свеча была выходом за экстремум, текущая закрылась обратно

    closed_1h[-1] = последняя закрытая.
    Возвращает (passed, list_of_signals, last_close).
    """
    if len(ohlcv_1h) < 22:
        return False, [], None

    closed_1h = ohlcv_1h[:-1]   # последняя закрытая
    if len(closed_1h) < 21:
        return False, [], None

    last = closed_1h[-1]
    prev = closed_1h[-2]
    op, h, l, cl, v = last[1], last[2], last[3], last[4], last[5]

    vols = [c[5] for c in closed_1h[-21:-1]]
    avg_vol = np.mean(vols) if vols else 1.0

    signals = []
    full_range = h - l
    if full_range <= 0:
        return False, [], cl

    body = abs(cl - op)
    body_ratio = body / full_range

    # --- Hammer/Pin Bar ---
    if mode == 'long':
        lower_wick = (min(op, cl) - l) / full_range
        if (lower_wick >= 0.55          # длинный нижний фитиль
                and body_ratio <= 0.35   # маленькое тело
                and cl > op              # бычье закрытие
                and v >= avg_vol * REV_TRIGGER_VOL_MULT):
            signals.append(f"hammer 1H (wick {lower_wick*100:.0f}%, vol ×{v/avg_vol:.1f})")
    else:
        upper_wick = (h - max(op, cl)) / full_range
        if (upper_wick >= 0.55
                and body_ratio <= 0.35
                and cl < op
                and v >= avg_vol * REV_TRIGGER_VOL_MULT):
            signals.append(f"pin bar 1H (wick {upper_wick*100:.0f}%, vol ×{v/avg_vol:.1f})")

    # --- Engulfing ---
    p_op, p_cl = prev[1], prev[4]
    if mode == 'long':
        if (cl > op              # текущая бычья
                and p_cl < p_op  # предыдущая медвежья
                and cl >= p_op   # закрылись выше открытия предыдущей
                and op <= p_cl   # открылись ниже закрытия предыдущей
                and v >= avg_vol * 1.2):
            signals.append("bullish engulfing 1H")
    else:
        if (cl < op
                and p_cl > p_op
                and cl <= p_op
                and op >= p_cl
                and v >= avg_vol * 1.2):
            signals.append("bearish engulfing 1H")

    # --- Reclaim: предыдущая 1H пробила экстремум, текущая закрылась обратно ---
    # Используем минимум/максимум за 20 предыдущих 1H свечей как локальный экстремум
    window_lows  = [c[3] for c in closed_1h[-21:-1]]
    window_highs = [c[2] for c in closed_1h[-21:-1]]
    if window_lows and window_highs:
        local_low_20  = min(window_lows)
        local_high_20 = max(window_highs)
        if mode == 'long':
            # текущая 1H зашла под локальный лоу, но закрылась выше него
            if l < local_low_20 * 0.999 and cl > local_low_20 * 1.001 and cl > op:
                signals.append(f"reclaim 1H (failed breakdown {local_low_20:.6g})")
        else:
            if h > local_high_20 * 1.001 and cl < local_high_20 * 0.999 and cl < op:
                signals.append(f"reject 1H (failed breakout {local_high_20:.6g})")

    return len(signals) > 0, signals, cl


def _rev_find_pivot_levels(closed_4h, current_price, mode='long', tolerance=0.005):
    """
    Ищет горизонтальные уровни (кластеры пивотов) в 7-дневном окне.
    Для long — выше цены (сопротивления), для short — ниже (поддержки).
    """
    window = closed_4h[-REV_WEEK_BARS_4H:]
    highs = [c[2] for c in window]
    lows  = [c[3] for c in window]

    pivots = []
    for i in range(2, len(window) - 2):
        if mode == 'long':
            # хаи как сопротивления
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2]
                    and highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                pivots.append(highs[i])
        else:
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2]
                    and lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                pivots.append(lows[i])

    if not pivots:
        return []

    # кластеризуем близкие уровни
    pivots.sort()
    clusters = [pivots[0]]
    for p in pivots[1:]:
        if (p - clusters[-1]) / clusters[-1] <= tolerance:
            clusters[-1] = (clusters[-1] + p) / 2
        else:
            clusters.append(p)

    if mode == 'long':
        return sorted([c for c in clusters if c > current_price * 1.005])
    else:
        return sorted([c for c in clusters if c < current_price * 0.995], reverse=True)


# ─────────────────────────────────────────────────────────────────────
# СЛОТ 4: ДОСТИЖИМАЯ ЦЕЛЬ + R/R
# ─────────────────────────────────────────────────────────────────────
def _rev_calc_targets(closed_4h, current_price, extreme_value, mode='long'):
    """
    TP1 = ближайший структурный уровень (Fibo 50% от движения к экстремуму
          или ближайший локальный уровень за неделю).
    Stop = за экстремум + 0.3 ATR буфер.

    Возвращает (passed, target, stop, target_pct, stop_pct, rr, comment).
    """
    atr = _rev_atr(closed_4h, period=14)
    if atr is None or atr <= 0:
        return False, None, None, 0, 0, 0, "no ATR"

    # Найдём "якорь" движения — крайний хай/лой С ДРУГОЙ СТОРОНЫ за неделю
    window = closed_4h[-REV_WEEK_BARS_4H:]

    if mode == 'long':
        # отскок ОТ лоя ВВЕРХ → якорь = недельный хай
        anchor_high = max(c[2] for c in window)
        impulse_down = anchor_high - extreme_value  # размах движения вниз
        if impulse_down <= 0:
            return False, None, None, 0, 0, 0, "no impulse"

        # Кандидаты для TP1:
        #   - Fibo 38.2% восстановления
        #   - ближайший pivot (если есть)
        #   - "ATR cap" — current + 3*ATR (чтобы не ставить мечту)
        fib_382 = extreme_value + impulse_down * 0.382
        pivots_above = _rev_find_pivot_levels(closed_4h, current_price, mode='long')
        atr_cap = current_price + REV_ATR_CAP_MULT * atr

        candidates = [fib_382, atr_cap]
        candidates += pivots_above
        candidates = [c for c in candidates if c > current_price * 1.001]

        if not candidates:
            return False, None, None, 0, 0, 0, "no TP candidates"

        # БЛИЖАЙШИЙ — это TP1 (фиксация на ближайшем сопротивлении)
        target = min(candidates)

        stop = extreme_value - atr * REV_ATR_STOP_BUFFER

        target_pct = (target - current_price) / current_price * 100
        stop_pct   = (current_price - stop) / current_price * 100

    else:
        anchor_low = min(c[3] for c in window)
        impulse_up = extreme_value - anchor_low
        if impulse_up <= 0:
            return False, None, None, 0, 0, 0, "no impulse"

        fib_382 = extreme_value - impulse_up * 0.382
        pivots_below = _rev_find_pivot_levels(closed_4h, current_price, mode='short')
        atr_cap = current_price - REV_ATR_CAP_MULT * atr

        candidates = [fib_382, atr_cap]
        candidates += pivots_below
        candidates = [c for c in candidates if c < current_price * 0.999]

        if not candidates:
            return False, None, None, 0, 0, 0, "no TP candidates"

        target = max(candidates)

        stop = extreme_value + atr * REV_ATR_STOP_BUFFER

        target_pct = (current_price - target) / current_price * 100
        stop_pct   = (stop - current_price) / current_price * 100

    if target_pct <= 0 or stop_pct <= 0:
        return False, None, None, 0, 0, 0, "negative pct"

    if target_pct < REV_MIN_TP1_PCT:
        return False, None, None, target_pct, stop_pct, 0, f"TP {target_pct:.2f}% < {REV_MIN_TP1_PCT}%"
    if target_pct > REV_MAX_TP1_PCT:
        return False, None, None, target_pct, stop_pct, 0, f"TP {target_pct:.2f}% > {REV_MAX_TP1_PCT}%"

    rr = target_pct / stop_pct
    if rr < REV_MIN_RR:
        return False, None, None, target_pct, stop_pct, rr, f"RR {rr:.2f} < {REV_MIN_RR}"

    return True, target, stop, target_pct, stop_pct, rr, "ok"


# ─────────────────────────────────────────────────────────────────────
# СЛОТ 5: НЕ ЛОВЛЯ НОЖА
# ─────────────────────────────────────────────────────────────────────
def _rev_check_not_knife(ohlcv_4h, mode='long'):
    """
    - За последние 3 закрытые 4H свечи движение к экстремуму ≤ 12%
    - Текущая (формирующаяся) 4H свеча НЕ должна быть капитуляцией
      (-/+ 5% и объём ×3 — ждём)

    Возвращает (passed, reason).
    """
    if len(ohlcv_4h) < 5:
        return False, "not enough data"

    closed = ohlcv_4h[:-1]
    current_candle = ohlcv_4h[-1]

    # Движение за последние 3 закрытые свечи
    price_3back = closed[-3][1]   # open 3-й свечи назад
    price_now   = closed[-1][4]   # close последней закрытой
    move_3bars  = (price_now - price_3back) / price_3back * 100

    if mode == 'long':
        if move_3bars < -REV_VETO_RECENT_3BARS_PCT:
            return False, f"3-bar dump {move_3bars:.1f}% < -{REV_VETO_RECENT_3BARS_PCT}%"
    else:
        if move_3bars > REV_VETO_RECENT_3BARS_PCT:
            return False, f"3-bar pump {move_3bars:.1f}% > {REV_VETO_RECENT_3BARS_PCT}%"

    # Капитуляция в текущей формирующейся свече
    op = current_candle[1]
    cl = current_candle[4]
    v_cur = current_candle[5]
    vols = [c[5] for c in closed[-20:]]
    avg_v = np.mean(vols) if vols else 1.0

    if op > 0:
        cur_change = (cl - op) / op * 100
        vol_mult = v_cur / avg_v if avg_v > 0 else 1.0

        if mode == 'long' and cur_change < -REV_VETO_CAPITULATION_PCT and vol_mult > 3.0:
            return False, f"capitulation now ({cur_change:.1f}%, vol ×{vol_mult:.1f})"
        if mode == 'short' and cur_change > REV_VETO_CAPITULATION_PCT and vol_mult > 3.0:
            return False, f"euphoria now ({cur_change:.1f}%, vol ×{vol_mult:.1f})"

    return True, "ok"


# ─────────────────────────────────────────────────────────────────────
# ВЕТО ПО МАКРО (BTC)
# ─────────────────────────────────────────────────────────────────────
def _rev_check_macro(btc_ch_4h, mode='long'):
    """Только на сильных движениях BTC режем альты против."""
    if mode == 'long' and btc_ch_4h < -REV_VETO_BTC_CH_4H:
        return False, f"BTC -{abs(btc_ch_4h):.1f}% за 4H"
    if mode == 'short' and btc_ch_4h > REV_VETO_BTC_CH_4H:
        return False, f"BTC +{btc_ch_4h:.1f}% за 4H"
    return True, "ok"


# ─────────────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ ЛОНГ
# ─────────────────────────────────────────────────────────────────────
def scan_reversal_4h_long(*, symbol, ohlcv_4h, ohlcv_1h, vol_24h, btc_ch_4h):
    """
    Возвращает None если сигнала нет, иначе dict:
        {
            'side': 'long',
            'entry': float,
            'target': float,
            'stop': float,
            'target_pct': float,
            'stop_pct': float,
            'rr': float,
            'quality': int (1-5, число пройденных слотов; всегда 5),
            'details': list[str],
            'message': str (готовый HTML для send_msg),
        }
    """
    return _rev_scan(symbol, ohlcv_4h, ohlcv_1h, vol_24h, btc_ch_4h, mode='long')


def scan_reversal_4h_short(*, symbol, ohlcv_4h, ohlcv_1h, vol_24h, btc_ch_4h):
    return _rev_scan(symbol, ohlcv_4h, ohlcv_1h, vol_24h, btc_ch_4h, mode='short')


def _rev_scan(symbol, ohlcv_4h, ohlcv_1h, vol_24h, btc_ch_4h, mode):
    # 0. Базовая проверка объёма
    if vol_24h < REV_MIN_VOLUME_24H:
        return None
    if not ohlcv_4h or len(ohlcv_4h) < REV_WEEK_BARS_4H + 5:
        return None
    if not ohlcv_1h or len(ohlcv_1h) < 25:
        return None

    closed_4h = ohlcv_4h[:-1]
    current_price = closed_4h[-1][4]
    details = []

    # МАКРО ВЕТО
    macro_ok, macro_reason = _rev_check_macro(btc_ch_4h, mode)
    if not macro_ok:
        return None

    # СЛОТ 1: НЕДЕЛЬНЫЙ ЭКСТРЕМУМ
    s1_ok, s1_info = _rev_check_weekly_extreme(closed_4h, mode)
    if not s1_ok:
        return None
    extreme_value = s1_info["extreme"]
    range_pct = s1_info["range_pct"]
    position_pct = s1_info["position_pct"]
    if mode == 'long':
        details.append(f"📍 7d low {extreme_value:.6g} (range {range_pct:.1f}%, pos {position_pct:.1f}%)")
    else:
        details.append(f"📍 7d high {extreme_value:.6g} (range {range_pct:.1f}%, pos {position_pct:.1f}%)")

    # СЛОТ 5 (раньше остальных — дешевле): НЕ ЛОВЛЯ НОЖА
    s5_ok, s5_reason = _rev_check_not_knife(ohlcv_4h, mode)
    if not s5_ok:
        return None

    # СЛОТ 2: ИСТОЩЕНИЕ
    s2_ok, s2_signals = _rev_check_exhaustion(closed_4h, mode)
    if not s2_ok:
        return None
    details.append("💤 Истощение: " + ", ".join(s2_signals))

    # СЛОТ 3: ТРИГГЕР НА 1H
    s3_ok, s3_signals, _ = _rev_check_1h_trigger(ohlcv_1h, mode)
    if not s3_ok:
        return None
    details.append("🎯 Триггер 1H: " + ", ".join(s3_signals))

    # СЛОТ 4: ЦЕЛЬ + R/R
    s4_ok, target, stop, target_pct, stop_pct, rr, s4_comment = \
        _rev_calc_targets(closed_4h, current_price, extreme_value, mode)
    if not s4_ok:
        return None

    # ─── Все 5 слотов пройдены, формируем сигнал ───
    side_emoji = "🟢" if mode == 'long' else "🔴"
    side_label = "ЛОНГ" if mode == 'long' else "ШОРТ"
    arrow = "📈" if mode == 'long' else "📉"

    tv = symbol.replace('/', '').replace(':USDT', '.P')
    coin = symbol.split("/")[0]
    # На Binance нет STOCK тикеров (это был MEXC-специфик).
    # Оставляем чистые имена как есть.

    msg_lines = [
        f"{side_emoji} <b>РАЗВОРОТ 4H {side_label}</b> {arrow}",
        f"Монета: <b>{symbol}</b>",
        f"Цена: <code>{current_price:.6g}</code>",
        "───────────────────",
        *[f"  {d}" for d in details],
        "───────────────────",
        f"🎯 TP: <code>{target:.6g}</code> ({'+' if mode == 'long' else '-'}{target_pct:.2f}%)",
        f"🛑 SL: <code>{stop:.6g}</code> ({'-' if mode == 'long' else '+'}{stop_pct:.2f}%)",
        f"⚖️ R/R: <b>{rr:.2f}</b>",
        f"📦 Объём 24H: ${vol_24h/1_000_000:.1f}M",
        f"👑 BTC за 4H: {btc_ch_4h:+.2f}%",
        "───────────────────",
        f"⏱ Горизонт: 2-5 дней",
        f"🔗 <a href='https://www.tradingview.com/chart/?symbol=MEXC:{tv}'>TradingView</a>",
        f"📊 <a href='https://www.coinglass.com/tv/Binance_{coin}USDT'>CoinGlass СуперГрафик</a>",
    ]

    return {
        'side': mode,
        'entry': current_price,
        'target': target,
        'stop': stop,
        'target_pct': target_pct,
        'stop_pct': stop_pct,
        'rr': rr,
        'quality': 5,
        'details': details,
        'message': "\n".join(msg_lines),
    }

# ═══════════════════════════════════════════════════════════════════
# Конец встроенного модуля REVERSAL_4H
# ═══════════════════════════════════════════════════════════════════


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

@app.route('/')
def home():
    return f"✅ АНАЛИТИК Binance v7.1 АКТИВЕН. Время: {datetime.now().strftime('%H:%M:%S')}"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    logging.warning("⚠️ TELEGRAM_TOKEN или CHAT_ID не заданы!")

exchange = ccxt.binance({
    'enableRateLimit': True,
    'rateLimit': 280,                       # 280мс пауза между запросами (Binance v7.2)
                                             # v7.1: 200мс не помогли — баны 418 продолжались.
                                             # 280мс = 3.57 req/sec → итерация ~9-10 мин.
                                             # Нагрузка на минуту ниже, меньше шансов попасть
                                             # под "пик активности" соседей по shared IP Render.
                                             # Главная защита теперь — global circuit breaker.
    'timeout': 15000,
    'options': {
        'defaultType': 'future',         # USDM Futures
        'adjustForTimeDifference': True,  # синхронизация времени с биржей
    }
})

# ═══════════════════════════════════════════════════════════════════
# GLOBAL CIRCUIT BREAKER (Binance v7.2)
# ═══════════════════════════════════════════════════════════════════
# IP_BAN_UNTIL — глобальная переменная-флаг (timestamp в секундах Unix).
# Когда Binance возвращает 418 "banned until <ms>", парсим этот ms,
# выставляем флаг и ВСЕ обращения к API проверяют этот флаг перед вызовом.
# Это исключает цепную реакцию: SPELL поймал 418 → MELANIA/HIVE/TAKE/RATS
# не пойдут в API, потому что флаг активен.
#
# Логика wait_if_banned (гибридная):
#   - бан < 180 сек → ждём активно (sleep до момента + 2 сек запаса)
#   - бан ≥ 180 сек → возвращаем False (skip), вызывающий код пропускает монету
#
# Защита от race condition — threading.Lock.
# ═══════════════════════════════════════════════════════════════════
import re as _re

IP_BAN_UNTIL = 0.0                 # Unix timestamp в секундах
_IP_BAN_LOCK = threading.Lock()
BAN_WAIT_THRESHOLD_SEC = 180       # порог гибридного режима (3 минуты)

# Контроль weight — обновляется после каждого запроса из last_response_headers
USED_WEIGHT_1M       = 0           # текущий used weight за последнюю минуту
WEIGHT_LIMIT_BINANCE = 2400        # официальный лимит Binance Futures
WEIGHT_SOFT_THRESHOLD = 1800       # порог превентивного sleep (75% от лимита)
_WEIGHT_LOCK = threading.Lock()


def _parse_banned_until_ms(err_str: str):
    """Парсит timestamp из сообщения Binance.
    Пример: 'banned until 1779426534756' → возвращает 1779426534.756 (секунды)
    """
    m = _re.search(r'banned until (\d{13})', err_str)
    if m:
        return int(m.group(1)) / 1000.0
    return None


def set_ip_ban_until(ban_ts_sec: float):
    """Атомарно обновляет глобальный флаг бана. Берёт максимум — чтобы
    случайный 'свежий' ответ с более ранним timestamp не сбросил более поздний."""
    global IP_BAN_UNTIL
    with _IP_BAN_LOCK:
        if ban_ts_sec > IP_BAN_UNTIL:
            IP_BAN_UNTIL = ban_ts_sec
            logging.warning(
                f"🚫 IP_BAN_UNTIL установлен на {datetime.fromtimestamp(ban_ts_sec).strftime('%H:%M:%S')} "
                f"(через {ban_ts_sec - time.time():.1f} сек)"
            )


def wait_if_banned() -> bool:
    """
    Проверяет глобальный флаг бана. Должен вызываться ПЕРЕД каждым API запросом.
    Возвращает:
      True  — можно продолжать (бан не активен или уже отсидели)
      False — бан слишком долгий, пропустить операцию
    """
    global IP_BAN_UNTIL
    with _IP_BAN_LOCK:
        ban_until = IP_BAN_UNTIL

    if ban_until <= 0:
        return True

    now = time.time()
    wait = ban_until - now

    if wait <= 0:
        # бан уже истёк — снимаем флаг
        with _IP_BAN_LOCK:
            if IP_BAN_UNTIL <= time.time():
                IP_BAN_UNTIL = 0.0
        return True

    if wait < BAN_WAIT_THRESHOLD_SEC:
        # короткий бан — ждём активно, плюс 2 сек запаса
        logging.info(f"⏸ Global ban active — sleeping {wait + 2:.1f}s")
        time.sleep(wait + 2)
        # после сна снимаем флаг, если время вышло
        with _IP_BAN_LOCK:
            if IP_BAN_UNTIL <= time.time():
                IP_BAN_UNTIL = 0.0
        return True
    else:
        # длинный бан — пропускаем (вызывающий код сделает skip)
        logging.warning(f"⏭ Global ban {wait:.0f}s ≥ {BAN_WAIT_THRESHOLD_SEC}s — skip")
        return False


def update_used_weight_from_headers():
    """После каждого успешного запроса — обновляем USED_WEIGHT_1M из заголовков.
    Binance кладёт X-MBX-USED-WEIGHT-1m в каждый ответ.
    Если weight ≥ WEIGHT_SOFT_THRESHOLD (1800/2400) — превентивный sleep до новой минуты.
    """
    global USED_WEIGHT_1M
    try:
        headers = getattr(exchange, 'last_response_headers', None) or {}
        # ccxt нормализует имена в нижний регистр в last_response_headers
        weight_str = (headers.get('x-mbx-used-weight-1m')
                      or headers.get('X-MBX-USED-WEIGHT-1m')
                      or headers.get('X-MBX-USED-WEIGHT-1M'))
        if weight_str:
            with _WEIGHT_LOCK:
                USED_WEIGHT_1M = int(weight_str)
            if USED_WEIGHT_1M >= WEIGHT_SOFT_THRESHOLD:
                # сколько секунд до начала новой минуты
                now = time.time()
                seconds_to_next_minute = 60 - (now % 60) + 1  # +1 запас
                logging.warning(
                    f"⚠️ X-MBX-USED-WEIGHT-1m={USED_WEIGHT_1M} ≥ {WEIGHT_SOFT_THRESHOLD}, "
                    f"sleep {seconds_to_next_minute:.1f}s до новой минуты"
                )
                time.sleep(seconds_to_next_minute)
    except Exception as e:
        logging.debug(f"update_used_weight: {e}")


def safe_api_call(fn, *args, **kwargs):
    """Универсальная обёртка для API вызовов, которая:
      1. Проверяет глобальный флаг бана.
      2. Вызывает функцию.
      3. Парсит 418/RateLimitExceeded → выставляет флаг.
      4. Обновляет used weight из headers.
    Возвращает результат вызова или None при ошибке/бане.
    """
    if not wait_if_banned():
        return None
    try:
        result = fn(*args, **kwargs)
        update_used_weight_from_headers()
        return result
    except (ccxt.RateLimitExceeded, ccxt.ExchangeNotAvailable) as e:
        err_str = str(e)
        ban_ts = _parse_banned_until_ms(err_str)
        if ban_ts:
            set_ip_ban_until(ban_ts)
        elif '418' in err_str or 'banned' in err_str.lower():
            # 418 без точного timestamp — ставим +60 сек
            set_ip_ban_until(time.time() + 60)
        else:
            # 429 без явного бана — короткая пауза
            set_ip_ban_until(time.time() + 10)
        return None
    except ccxt.NetworkError as e:
        logging.warning(f"NetworkError in safe_api_call: {e}")
        time.sleep(3)
        return None
    except Exception as e:
        logging.error(f"safe_api_call unexpected: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# КОНЕЦ блока GLOBAL CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════════

WATCHLIST = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT',
    'BNB/USDT:USDT', 'XRP/USDT:USDT', 'DOGE/USDT:USDT',
    'ADA/USDT:USDT', 'AVAX/USDT:USDT', 'LINK/USDT:USDT',
    'DOT/USDT:USDT',
]

# ─────────────────────────────────────────────
# ПОРОГИ ФИЛЬТРОВ
# ─────────────────────────────────────────────
MIN_VOLUME_SIGNAL        = 5_000_000   # $5M для СИГНАЛ
MIN_VOLUME_ATTENTION     = 1_000_000   # $1M для ВНИМАНИЕ и РАННИХ лонг/шорт 2H
MIN_RR_SIGNAL            = 3.0
MIN_RR_ATTENTION         = 2.0
MIN_TARGET_PCT_SIGNAL    = 5.0
MIN_TARGET_PCT_ATTENTION = 2.0
MAX_STOP_PCT             = 3.0
SCORE_SIGNAL             = 7
SCORE_ATTENTION          = 4
SWING_ATTENTION_PCT      = 3.0

SWING_BARS_2H            = 5           # ← НОВОЕ: было 10, стало 5 для 2H ранних сигналов

bot_status = {
    "started_at":       datetime.now().isoformat(),
    "last_iteration":   None,
    "iterations":       0,
    "errors":           0,
    "signals_sent":     0,
    "attention_sent":   0,
    "early_2h_sent":    0,
    "reversal_sent":    0,
}

oi_cache: dict = {}


def calculate_ssma(ohlcv, period=24):
    if len(ohlcv) < period + 10:
        return None, 'neutral', 0.0

    typical = [(c[2] + c[3] + c[4]) / 3 for c in ohlcv]
    smma = np.mean(typical[:period])
    smma_history = [smma]

    for i in range(period, len(typical)):
        smma = (smma * (period - 1) + typical[i]) / period
        smma_history.append(smma)

    current_smma  = smma_history[-1]
    prev_smma     = smma_history[-2] if len(smma_history) >= 2 else smma_history[-1]
    current_price = ohlcv[-1][4]
    slope         = (current_smma - prev_smma) / prev_smma * 100

    if current_price > current_smma and slope > 0:
        trend = 'bull_strong'
    elif current_price > current_smma and slope <= 0:
        trend = 'bull_weak'
    elif current_price < current_smma and slope < 0:
        trend = 'bear_strong'
    else:
        trend = 'bear_weak'

    return current_smma, trend, slope


def calculate_ema(ohlcv, period=20):
    """
    EMA (Exponential Moving Average) — быстрее реагирует на изменения чем SSMA.
    Используется в блоке СЖАТИЕ для определения направления взрыва.

    Возвращает: (current_ema, trend, avg_slope_pct_per_bar)
    - current_ema: текущее значение EMA
    - trend: 'bull_strong' | 'bull_weak' | 'bear_strong' | 'bear_weak' | 'neutral'
    - avg_slope_pct_per_bar: средний наклон в %/свеча за последние 5 свечей
    """
    if len(ohlcv) < period + 10:
        return None, 'neutral', 0.0

    closes = [c[4] for c in ohlcv]
    k = 2.0 / (period + 1)

    # Стартовый EMA = SMA первых period свечей
    ema = sum(closes[:period]) / period
    ema_history = [ema]

    for i in range(period, len(closes)):
        ema = closes[i] * k + ema * (1 - k)
        ema_history.append(ema)

    if len(ema_history) < 6:
        return ema_history[-1], 'neutral', 0.0

    current_ema = ema_history[-1]
    ema_5_ago   = ema_history[-6]
    current_price = closes[-1]

    # Средний наклон в % за свечу (за последние 5 свечей)
    if ema_5_ago > 0:
        avg_slope = (current_ema - ema_5_ago) / ema_5_ago / 5 * 100
    else:
        avg_slope = 0.0

    # Определяем тренд по сочетанию: цена vs EMA + наклон EMA
    # сильный bull: цена выше EMA + EMA растёт уверенно (>0.05%/св)
    # слабый bull:  цена выше EMA + EMA слабо растёт (0..0.05%/св)
    # слабый bear:  цена ниже EMA + EMA слабо падает (-0.05..0%/св)
    # сильный bear: цена ниже EMA + EMA падает уверенно (<-0.05%/св)
    if current_price > current_ema and avg_slope > 0.05:
        trend = 'bull_strong'
    elif current_price > current_ema and avg_slope > -0.05:
        trend = 'bull_weak'
    elif current_price < current_ema and avg_slope < -0.05:
        trend = 'bear_strong'
    elif current_price < current_ema and avg_slope < 0.05:
        trend = 'bear_weak'
    else:
        trend = 'neutral'

    return current_ema, trend, avg_slope


def ssma_allows_long(ssma_val, ssma_trend, ssma_slope, current_price):
    if ssma_val is None:
        return True
    price_above = current_price > ssma_val
    ssma_rising = ssma_slope > 0
    return price_above or ssma_rising


def ssma_allows_short(ssma_val, ssma_trend, ssma_slope, current_price):
    if ssma_val is None:
        return True
    price_below  = current_price < ssma_val
    ssma_falling = ssma_slope < 0
    strong_fall  = ssma_slope < -0.05
    return (price_below and ssma_falling) or strong_fall


def check_rr(current_price, target, stop, mode='long',
             min_target_pct=MIN_TARGET_PCT_SIGNAL,
             max_stop_pct=MAX_STOP_PCT):
    if target is None or stop is None:
        return False, 0.0, 0.0, 0.0

    if mode == 'long':
        target_pct = (target - current_price) / current_price * 100
        stop_pct   = (current_price - stop)   / current_price * 100
    else:
        target_pct = (current_price - target) / current_price * 100
        stop_pct   = (stop - current_price)   / current_price * 100

    if target_pct <= 0 or stop_pct <= 0:
        return False, target_pct, stop_pct, 0.0

    rr = target_pct / stop_pct

    passed = (target_pct >= min_target_pct
              and stop_pct  <= max_stop_pct
              and rr        >= (min_target_pct / max_stop_pct))

    return passed, target_pct, stop_pct, rr


def get_volume_info(closed, volume_24h):
    v_history = [x[5] for x in closed[-21:-1]]
    v_avg     = np.mean(v_history) if v_history else 1.0
    v_cur     = closed[-1][5]
    v_rel     = v_cur / v_avg if v_avg > 0 else 1.0

    std = np.std(v_history) if len(v_history) > 1 else 1.0
    v_zscore = (v_cur - np.mean(v_history)) / std if std > 0 else 0.0

    buy_pct_list = []
    for c in closed[-5:]:
        h, l, cl = c[2], c[3], c[4]
        bp = (cl - l) / (h - l) * 100 if h != l else 50.0
        buy_pct_list.append(bp)
    avg_buy_pct = np.mean(buy_pct_list)
    sell_pct    = 100 - avg_buy_pct

    vol_24h_m = volume_24h / 1_000_000

    if v_rel >= 10 or v_zscore >= 3.0:
        vol_score = 3
        vol_label = f"🚀 Vol x{v_rel:.0f} Z:{v_zscore:.1f}σ ВЗРЫВ"
    elif v_rel >= 5 or v_zscore >= 2.0:
        vol_score = 2
        vol_label = f"📊 Vol x{v_rel:.1f} Z:{v_zscore:.1f}σ"
    elif v_rel >= 1.8:
        vol_score = 1
        vol_label = f"📊 Vol x{v_rel:.1f}"
    else:
        vol_score = 0
        vol_label = ""

    detail_block = (
        f"📦 Объём 24H: <b>${vol_24h_m:.1f}M</b> | "
        f"x{v_rel:.1f} от нормы | Z: {v_zscore:.1f}σ\n"
        f"   🟢 Покупки ~{avg_buy_pct:.0f}% / 🔴 Продажи ~{sell_pct:.0f}%"
    )

    return vol_score, vol_label, v_rel, v_zscore, avg_buy_pct, detail_block


def calculate_bb_outside_atr(ohlcv, bb_length=55, bb_std=0.712,
                               atr_length=20, atr_mult=0.618,
                               tilson_length=10, tilson_factor=0.3):
    if len(ohlcv) < bb_length + atr_length + 10:
        return 'neutral', 0.0, None, None, None

    closes = [c[4] for c in ohlcv]
    highs  = [c[2] for c in ohlcv]
    lows   = [c[3] for c in ohlcv]

    f  = tilson_factor
    c1 = -(f**3)
    c2 = 3 * f**2 + 3 * f**3
    c3 = -6 * f**2 - 3 * f - 3 * f**3
    c4 = 1 + 3 * f + f**3 + 3 * f**2

    chunk = closes[-min(len(closes), bb_length + tilson_length * 6):]
    k     = 2 / (tilson_length + 1)

    def make_ema(src):
        e = src[0]
        out = []
        for v in src:
            e = v * k + e * (1 - k)
            out.append(e)
        return out

    e1 = make_ema(chunk)
    e2 = make_ema(e1)
    e3 = make_ema(e2)
    e4 = make_ema(e3)
    e5 = make_ema(e4)
    e6 = make_ema(e5)

    basis = c1*e6[-1] + c2*e5[-1] + c3*e4[-1] + c4*e3[-1]

    trs = []
    for i in range(1, len(ohlcv)):
        h  = highs[i]; l = lows[i]; pc = closes[i-1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    if len(trs) < atr_length:
        return 'neutral', 0.0, None, None, None

    atr_val = np.mean(trs[-atr_length:])
    upper   = basis + atr_mult * atr_val
    lower   = basis - atr_mult * atr_val
    current = closes[-1]

    dist_lower = (lower - current) / lower * 100
    dist_upper = (current - upper) / upper * 100

    if current < lower:
        return 'oversold',   dist_lower, upper, lower, basis
    elif current > upper:
        return 'overbought', dist_upper, upper, lower, basis
    else:
        return 'neutral', 0.0, upper, lower, basis


def calculate_swing_hilo(ohlcv, swing_bars=34):
    if len(ohlcv) < swing_bars * 2 + 1:
        return 100.0, 100.0, False, False, None, None

    highs   = [c[2] for c in ohlcv]
    lows    = [c[3] for c in ohlcv]
    n       = len(ohlcv)
    last_sw_low  = None
    last_sw_high = None

    for i in range(swing_bars, n - swing_bars - 1):
        if lows[i]  == min(lows[i  - swing_bars:i + swing_bars + 1]):
            last_sw_low  = lows[i]
        if highs[i] == max(highs[i - swing_bars:i + swing_bars + 1]):
            last_sw_high = highs[i]

    current = ohlcv[-1][4]

    if last_sw_low  is None: last_sw_low  = min(lows[-60:])
    if last_sw_high is None: last_sw_high = max(highs[-60:])

    sw_low_pct  = (current - last_sw_low)   / last_sw_low  * 100
    sw_high_pct = (last_sw_high - current)  / last_sw_high * 100

    near_sw_low  = 0 <= sw_low_pct  <= SWING_ATTENTION_PCT
    near_sw_high = 0 <= sw_high_pct <= SWING_ATTENTION_PCT

    return sw_low_pct, sw_high_pct, near_sw_low, near_sw_high, last_sw_low, last_sw_high


def analyze_elliott(ohlcv, swing_bars=10):
    result = {
        'structure': 'neutral', 'wave_number': None, 'wave_label': '⚪️ Нет данных',
        'fib_retracement': 0.0, 'fib_level': None, 'fib_on_level': False,
        'wave1_low': None, 'wave1_high': None,
        'fib_382': None, 'fib_500': None, 'fib_618': None,
        'ext_1618': None, 'ext_2618': None,
        'bos_bull': False, 'bos_bear': False,
        'score_long': 0, 'score_short': 0,
        'details_long': [], 'details_short': [],
        'alert_block': ''
    }

    if len(ohlcv) < swing_bars * 4 + 1:
        return result

    highs  = [c[2] for c in ohlcv]
    lows   = [c[3] for c in ohlcv]
    closes = [c[4] for c in ohlcv]
    n      = len(ohlcv)

    swing_lows  = []
    swing_highs = []

    for i in range(swing_bars, n - swing_bars):
        if lows[i] == min(lows[i - swing_bars:i + swing_bars + 1]):
            swing_lows.append((i, lows[i]))
        if highs[i] == max(highs[i - swing_bars:i + swing_bars + 1]):
            swing_highs.append((i, highs[i]))

    if len(swing_lows) < 2 or len(swing_highs) < 2:
        return result

    last_lows  = swing_lows[-3:]
    last_highs = swing_highs[-3:]

    current = closes[-1]

    bull_lows  = all(last_lows[i][1]  < last_lows[i+1][1]  for i in range(len(last_lows)-1))
    bull_highs = all(last_highs[i][1] < last_highs[i+1][1] for i in range(len(last_highs)-1))
    bear_lows  = all(last_lows[i][1]  > last_lows[i+1][1]  for i in range(len(last_lows)-1))
    bear_highs = all(last_highs[i][1] > last_highs[i+1][1] for i in range(len(last_highs)-1))

    if bull_lows and bull_highs:
        result['structure'] = 'bullish'
    elif bear_lows and bear_highs:
        result['structure'] = 'bearish'
    elif bull_lows and not bull_highs:
        result['structure'] = 'bullish'
    elif bear_highs and not bear_lows:
        result['structure'] = 'bearish'
    else:
        result['structure'] = 'neutral'

    if len(swing_highs) >= 2:
        prev_high = swing_highs[-2][1]
        result['bos_bull'] = current > prev_high

    if len(swing_lows) >= 2:
        prev_low = swing_lows[-2][1]
        result['bos_bear'] = current < prev_low

    if result['structure'] == 'bullish':
        w1_low  = swing_lows[-2][1]
        w1_high = swing_highs[-1][1]
        impulse = w1_high - w1_low

        if impulse <= 0:
            return result

        result['wave1_low']  = w1_low
        result['wave1_high'] = w1_high

        result['fib_382'] = w1_high - impulse * 0.382
        result['fib_500'] = w1_high - impulse * 0.500
        result['fib_618'] = w1_high - impulse * 0.618

        result['ext_1618'] = w1_low + impulse * 1.618
        result['ext_2618'] = w1_low + impulse * 2.618

        if current < w1_high:
            retrace = (w1_high - current) / impulse * 100
            result['fib_retracement'] = retrace

            tolerance = 0.015
            for lvl_name, lvl_val in [('61.8%', result['fib_618']),
                                       ('50.0%', result['fib_500']),
                                       ('38.2%', result['fib_382'])]:
                if lvl_val and abs(current - lvl_val) / lvl_val <= tolerance:
                    result['fib_level']    = lvl_name
                    result['fib_on_level'] = True
                    break

            if retrace <= 38.2:
                result['wave_number'] = 4
                result['wave_label']  = '〽️ Волна 4 (откат, ждём 5)'
            elif 38.2 < retrace <= 61.8:
                result['wave_number'] = 2
                result['wave_label']  = '〽️ Волна 2 → ожидается Волна 3 ⚡️'
            elif retrace > 61.8:
                if retrace <= 78.6:
                    result['wave_number'] = 2
                    result['wave_label']  = '〽️ Волна 2 глубокая (78.6%)'
                else:
                    result['wave_number'] = None
                    result['wave_label']  = '⚠️ Возможная смена тренда'
        else:
            ext_pct = (current - w1_low) / impulse * 100
            if 100 < ext_pct <= 162:
                result['wave_number'] = 3
                result['wave_label']  = '⚡️ Волна 3 (активная)'
            elif ext_pct > 162:
                result['wave_number'] = 3
                result['wave_label']  = '⚡️ Волна 3 расширенная'
            else:
                result['wave_number'] = 1
                result['wave_label']  = '〽️ Волна 1 (начало)'

    elif result['structure'] == 'bearish':
        w1_high = swing_highs[-2][1]
        w1_low  = swing_lows[-1][1]
        impulse = w1_high - w1_low

        if impulse <= 0:
            return result

        result['wave1_low']  = w1_low
        result['wave1_high'] = w1_high

        result['fib_382'] = w1_low + impulse * 0.382
        result['fib_500'] = w1_low + impulse * 0.500
        result['fib_618'] = w1_low + impulse * 0.618

        result['ext_1618'] = w1_high - impulse * 1.618
        result['ext_2618'] = w1_high - impulse * 2.618

        if current > w1_low:
            retrace = (current - w1_low) / impulse * 100
            result['fib_retracement'] = retrace

            tolerance = 0.015
            for lvl_name, lvl_val in [('61.8%', result['fib_618']),
                                       ('50.0%', result['fib_500']),
                                       ('38.2%', result['fib_382'])]:
                if lvl_val and abs(current - lvl_val) / lvl_val <= tolerance:
                    result['fib_level']    = lvl_name
                    result['fib_on_level'] = True
                    break

            if retrace <= 38.2:
                result['wave_number'] = 4
                result['wave_label']  = '〽️ Волна 4 (отскок, ждём 5↓)'
            elif 38.2 < retrace <= 61.8:
                result['wave_number'] = 2
                result['wave_label']  = '〽️ Волна 2↓ → ожидается Волна 3↓ ⚡️'
            elif retrace > 61.8:
                if retrace <= 78.6:
                    result['wave_number'] = 2
                    result['wave_label']  = '〽️ Волна 2↓ глубокая (78.6%)'
                else:
                    result['wave_number'] = None
                    result['wave_label']  = '⚠️ Возможный разворот вверх'
        else:
            ext_pct = (w1_high - current) / impulse * 100
            if 100 < ext_pct <= 162:
                result['wave_number'] = 3
                result['wave_label']  = '⚡️ Волна 3↓ (активная)'
            elif ext_pct > 162:
                result['wave_number'] = 3
                result['wave_label']  = '⚡️ Волна 3↓ расширенная'
            else:
                result['wave_number'] = 1
                result['wave_label']  = '〽️ Волна 1↓ (начало)'

    sc_l = 0; sc_s = 0; det_l = []; det_s = []

    if result['structure'] == 'bullish':
        if result['wave_number'] == 2 and result['fib_on_level']:
            sc_l += 3
            det_l.append(f"⚡️ W2→W3 Фибо {result['fib_level']}")
        elif result['wave_number'] == 2:
            sc_l += 2
            det_l.append("〽️ Волна 2 (откат)")
        elif result['wave_number'] == 4 and result['fib_on_level']:
            sc_l += 2
            det_l.append(f"〽️ W4 Фибо {result['fib_level']}")
        elif result['wave_number'] == 4:
            sc_l += 1
            det_l.append("〽️ Волна 4 (откат)")
        if result['bos_bull']:
            sc_l += 1
            det_l.append("💥 BOS пробой")

    if result['structure'] == 'bearish':
        if result['wave_number'] == 2 and result['fib_on_level']:
            sc_s += 3
            det_s.append(f"⚡️ W2↓→W3↓ Фибо {result['fib_level']}")
        elif result['wave_number'] == 2:
            sc_s += 2
            det_s.append("〽️ Волна 2↓ (отскок)")
        elif result['wave_number'] == 4 and result['fib_on_level']:
            sc_s += 2
            det_s.append(f"〽️ W4↓ Фибо {result['fib_level']}")
        elif result['wave_number'] == 4:
            sc_s += 1
            det_s.append("〽️ Волна 4↓ (отскок)")
        if result['bos_bear']:
            sc_s += 1
            det_s.append("💥 BOS пробой вниз")

    if (result['structure'] == 'bullish'
            and result['wave1_low'] is not None
            and closes[-1] < result['wave1_low'] * 0.99):
        result['wave_label']  = '⚠️ ABC коррекция (структура сломана)'
        result['wave_number'] = None
        sc_l = 0
        det_l = ['⚠️ ABC — ждём завершения коррекции']

    if (result['structure'] == 'bearish'
            and result['wave1_high'] is not None
            and closes[-1] > result['wave1_high'] * 1.01):
        result['wave_label']  = '⚠️ ABC коррекция вверх (структура сломана)'
        result['wave_number'] = None
        sc_s = 0
        det_s = ['⚠️ ABC — ждём завершения коррекции']

    result['score_long']   = sc_l
    result['score_short']  = sc_s
    result['details_long'] = det_l
    result['details_short']= det_s

    lines = []
    lines.append(f"〽️ Эллиотт: <b>{result['wave_label']}</b>")

    if result['wave1_low'] and result['wave1_high']:
        imp = result['wave1_high'] - result['wave1_low']
        imp_pct = imp / result['wave1_low'] * 100
        lines.append(
            f"   Импульс: {result['wave1_low']:.4g} → "
            f"{result['wave1_high']:.4g} (+{imp_pct:.1f}%)"
        )

    if result['fib_retracement'] > 0:
        fib_str = f"Откат: {result['fib_retracement']:.1f}%"
        if result['fib_on_level']:
            fib_str += f" 📐 на уровне {result['fib_level']} ✅"
        lines.append(f"   {fib_str}")

    if result['structure'] == 'bullish' and result['fib_382']:
        lines.append(
            f"📐 Фибо: 38.2%={result['fib_382']:.4g} | "
            f"50%={result['fib_500']:.4g} | "
            f"61.8%={result['fib_618']:.4g}"
        )
        if result['ext_1618']:
            lines.append(
                f"🎯 Цели W3: ×1.618={result['ext_1618']:.4g} "
                f"(+{(result['ext_1618']/result['wave1_high']-1)*100:.1f}%) | "
                f"×2.618={result['ext_2618']:.4g} "
                f"(+{(result['ext_2618']/result['wave1_high']-1)*100:.1f}%)"
            )
    elif result['structure'] == 'bearish' and result['fib_382']:
        lines.append(
            f"📐 Фибо: 38.2%={result['fib_382']:.4g} | "
            f"50%={result['fib_500']:.4g} | "
            f"61.8%={result['fib_618']:.4g}"
        )
        if result['ext_1618']:
            lines.append(
                f"🎯 Цели W3↓: ×1.618={result['ext_1618']:.4g} "
                f"(-{(1-result['ext_1618']/result['wave1_low'])*100:.1f}%) | "
                f"×2.618={result['ext_2618']:.4g} "
                f"(-{(1-result['ext_2618']/result['wave1_low'])*100:.1f}%)"
            )

    if result['bos_bull']: lines.append("💥 BOS: пробой предыдущего хая")
    if result['bos_bear']: lines.append("💥 BOS: пробой предыдущего лоя")

    result['alert_block'] = '\n'.join(lines)
    return result


def detect_wyckoff_phase(ohlcv, swing_low, swing_high, cvd_level):
    if len(ohlcv) < 60:
        return 'ranging', True, True, '⚪️ Диапазон', 0

    closes  = [c[4] for c in ohlcv]
    volumes = [c[5] for c in ohlcv]
    current = closes[-1]

    price_range  = swing_high - swing_low
    if price_range <= 0: price_range = swing_high * 0.01
    position_pct = (current - swing_low) / price_range * 100

    lower_third = swing_low + price_range * 0.33
    upper_third = swing_low + price_range * 0.67

    vol_at_lows  = np.mean([volumes[i] for i in range(len(ohlcv))
                             if closes[i] <= lower_third] or [0])
    vol_at_highs = np.mean([volumes[i] for i in range(len(ohlcv))
                             if closes[i] >= upper_third] or [0])
    vol_avg = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)

    recent_ranges  = [ohlcv[i][2] - ohlcv[i][3] for i in range(-10, 0)]
    earlier_ranges = [ohlcv[i][2] - ohlcv[i][3] for i in range(-30, -10)]
    vol_compression = (np.mean(recent_ranges) / np.mean(earlier_ranges)) \
                      if np.mean(earlier_ranges) > 0 else 1.0

    ma20_start  = np.mean(closes[-25:-20]) if len(closes) >= 25 else closes[0]
    ma20_end    = np.mean(closes[-5:])
    trend_20    = (ma20_end - ma20_start) / ma20_start * 100

    recent_lows  = [c[3] for c in ohlcv[-5:]]
    spring_detected = (min(recent_lows) < swing_low * 1.005
                       and current > swing_low * 1.01
                       and cvd_level in ('bull', 'bull_div'))
    if spring_detected:
        return 'accumulation', True, False, '🌱 Spring (Wyckoff)', 3

    recent_highs = [c[2] for c in ohlcv[-5:]]
    utad_detected = (max(recent_highs) > swing_high * 0.995
                     and current < swing_high * 0.99
                     and cvd_level in ('bear', 'bear_div'))
    if utad_detected:
        return 'distribution', False, True, '🔝 UTAD (Wyckoff)', 3

    if (position_pct <= 25
            and vol_at_lows > vol_avg * 1.2
            and cvd_level in ('bull', 'bull_div')
            and vol_compression < 0.9):
        strength = 0
        if position_pct <= 15:        strength += 1
        if vol_at_lows > vol_avg * 2: strength += 1
        if cvd_level == 'bull_div':   strength += 1
        return 'accumulation', True, False, '🟢 Накопление (Wyckoff)', min(strength, 3)

    if (position_pct >= 75
            and vol_at_highs > vol_avg * 1.2
            and cvd_level in ('bear', 'bear_div')
            and vol_compression < 0.9):
        strength = 0
        if position_pct >= 85:         strength += 1
        if vol_at_highs > vol_avg * 2: strength += 1
        if cvd_level == 'bear_div':    strength += 1
        return 'distribution', False, True, '🔴 Распределение (Wyckoff)', min(strength, 3)

    if trend_20 > 3.0 and position_pct > 50 and cvd_level in ('bull', 'bull_div'):
        return 'markup', True, False, '📈 Разгон (Markup)', 1

    if trend_20 < -3.0 and position_pct < 50 and cvd_level in ('bear', 'bear_div'):
        return 'markdown', False, True, '📉 Снижение (Markdown)', 1

    return 'ranging', True, True, '⚪️ Диапазон', 0


def check_post_move_filter(ohlcv, lookback=12):
    if len(ohlcv) < lookback + 1:
        return False, False, 0.0, 0

    max_down = 0.0; max_up = 0.0
    down_idx = 0;   up_idx = 0

    for i in range(len(ohlcv) - lookback, len(ohlcv)):
        candle = ohlcv[i]
        o, h, l, c = candle[1], candle[2], candle[3], candle[4]
        md = (o - l) / o * 100 if o > 0 else 0
        mu = (h - o) / o * 100 if o > 0 else 0
        if i > 0:
            pc = ohlcv[i-1][4]
            md = max(md, (pc - c) / pc * 100 if pc > 0 else 0)
            mu = max(mu, (c - pc) / pc * 100 if pc > 0 else 0)
        if md > max_down: max_down = md; down_idx = i
        if mu > max_up:   max_up   = mu; up_idx   = i

    last_idx = len(ohlcv) - 1
    after_dump = (max_down >= 15.0 and 1 <= (last_idx - down_idx) <= 6)
    after_pump = (max_up   >= 15.0 and 1 <= (last_idx - up_idx)   <= 6)

    move_pct      = max_down if after_dump else (max_up if after_pump else 0.0)
    candles_since = (last_idx - down_idx) if after_dump else \
                    ((last_idx - up_idx)  if after_pump else 0)

    return after_dump, after_pump, move_pct, candles_since


def calc_cvd_level(closed):
    if len(closed) < 10:
        return 'neutral', 0.0
    deltas = []
    for c in closed:
        h, l, cl, v = c[2], c[3], c[4], c[5]
        ratio = (cl - l) / (h - l) if h != l else 0.5
        deltas.append((ratio - 0.5) * 2 * v)

    cumulative = np.cumsum(deltas)
    total_cvd  = cumulative[-1]
    lookback   = 10
    closes     = [x[4] for x in closed]
    if len(closes) < lookback + 1:
        return 'neutral', total_cvd

    price_new_low  = closes[-1] < min(closes[-lookback:-1])
    price_new_high = closes[-1] > max(closes[-lookback:-1])
    cvd_new_low    = cumulative[-1] < min(cumulative[-lookback:-1])
    cvd_new_high   = cumulative[-1] > max(cumulative[-lookback:-1])

    if price_new_low  and not cvd_new_low:  return 'bull_div', total_cvd
    if price_new_high and not cvd_new_high: return 'bear_div', total_cvd
    if total_cvd > 0:                       return 'bull',     total_cvd
    return 'bear', total_cvd


def get_cvd_divergence(ohlcv, mode='long'):
    closes     = [x[4] for x in ohlcv]
    lookback   = 10
    cumulative = 0.0
    cvd_proxy  = []
    for candle in ohlcv:
        o, h, l, c, v = candle[1], candle[2], candle[3], candle[4], candle[5]
        ratio      = (c - l) / (h - l) if h != l else 0.5
        cumulative += (ratio - 0.5) * 2 * v
        cvd_proxy.append(cumulative)
    if len(closes) < lookback + 1:
        return False
    if mode == 'long':
        return (closes[-1] < min(closes[-lookback:-1])
                and cvd_proxy[-1] > min(cvd_proxy[-lookback:-1]))
    else:
        return (closes[-1] > max(closes[-lookback:-1])
                and cvd_proxy[-1] < max(cvd_proxy[-lookback:-1]))


def calc_rsi_divergence(closed, rsi_period=14, lookback=10):
    closes = [x[4] for x in closed]
    if len(closes) < rsi_period + lookback + 2:
        return False, False

    def rsi_at(sl):
        if len(sl) < rsi_period + 1: return 50.0
        d  = np.diff(sl)
        g  = np.where(d > 0, d, 0.0)
        ls = np.where(d < 0, -d, 0.0)
        ag = np.mean(g[:rsi_period]); al = np.mean(ls[:rsi_period])
        for i in range(rsi_period, len(g)):
            ag = (ag * (rsi_period-1) + g[i])  / rsi_period
            al = (al * (rsi_period-1) + ls[i]) / rsi_period
        if al == 0: return 100.0
        return 100.0 - (100.0 / (1.0 + ag / al))

    rsi_values = []
    for i in range(lookback + 1):
        idx = len(closes) - lookback - 1 + i
        rsi_values.append(rsi_at(closes[max(0, idx - rsi_period - 1): idx + 1]))

    pw   = closes[-(lookback+1):]
    bull = pw[-1] < min(pw[:-1]) and rsi_values[-1] >= min(rsi_values[:-1])
    bear = pw[-1] > max(pw[:-1]) and rsi_values[-1] <= max(rsi_values[:-1])
    return bull, bear


def get_oi_data_from_ticker(symbol, tickers, price_change_pct):
    try:
        ticker   = tickers.get(symbol)
        if not ticker: return 0.0, 'neutral', '⚪️ OI: нет данных'
        info     = ticker.get('info', {})
        hold_vol = float(info.get('holdVol', 0) or 0)
        if hold_vol == 0: return 0.0, 'neutral', '⚪️ OI: нет данных'

        prev_vol         = oi_cache.get(symbol, 0)
        oi_cache[symbol] = hold_vol

        if prev_vol == 0:
            return 0.0, 'neutral', f"⚪️ OI: {hold_vol:.0f} (первое измерение)"

        oi_chg = (hold_vol - prev_vol) / prev_vol * 100
        if abs(oi_chg) < 2.0:
            return oi_chg, 'neutral', f"⚪️ OI: {oi_chg:+.1f}% (нейтраль)"

        if   oi_chg > 0 and price_change_pct >= 0:
            return oi_chg, 'bull',       f"🟢 OI: +{oi_chg:.1f}% (деньги в лонг)"
        elif oi_chg > 0 and price_change_pct < 0:
            return oi_chg, 'bear',       f"🔴 OI: +{oi_chg:.1f}% (деньги в шорт)"
        elif oi_chg < 0 and price_change_pct >= 0:
            return oi_chg, 'squeeze_up', f"⚠️ OI: {oi_chg:.1f}% (шорт-сквиз)"
        else:
            return oi_chg, 'squeeze_dn', f"⚠️ OI: {oi_chg:.1f}% (лонг-ликвидации)"
    except Exception as e:
        logging.debug(f"OI error {symbol}: {e}")
        return 0.0, 'neutral', '⚪️ OI: нет данных'


def get_funding_signal(symbol, funding_rates_cache=None):
    """
    Возвращает funding rate для символа в виде (rate_pct, signal, label).
    Если передан funding_rates_cache — берёт оттуда (быстро, без запроса).
    Иначе делает отдельный запрос (медленно).
    """
    rate = None
    if funding_rates_cache is not None:
        # Быстрый путь: берём из кэша который загрузили один раз в начале итерации
        if symbol in funding_rates_cache:
            rate = funding_rates_cache[symbol] * 100  # 0.0001 → 0.01%
    if rate is None:
        # v7.2: fallback через safe_api_call (учитывает global ban)
        fr = safe_api_call(exchange.fetch_funding_rate, symbol)
        if fr is None:
            return 0.0, 'neutral', '⚪️ Фандинг: нет данных'
        try:
            rate = float(fr.get('fundingRate', 0) or 0) * 100
        except:
            return 0.0, 'neutral', '⚪️ Фандинг: нет данных'
    if rate < -0.02:   return rate, 'bull',    f"🟢 Фандинг: {rate:.3f}%"
    elif rate > 0.05:  return rate, 'bear',    f"🔴 Фандинг: {rate:.3f}%"
    else:              return rate, 'neutral',  f"⚪️ Фандинг: {rate:.3f}%"


def get_pivot_levels(ohlcv, tolerance=0.005):
    highs = [x[2] for x in ohlcv]; lows = [x[3] for x in ohlcv]
    pl = []; ph = []
    for i in range(2, len(ohlcv) - 2):
        if lows[i]  < lows[i-1]  and lows[i]  < lows[i-2]  and lows[i]  < lows[i+1]  and lows[i]  < lows[i+2]:  pl.append(lows[i])
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]: ph.append(highs[i])

    def cluster(pts, tol):
        if not pts: return []
        pts = sorted(pts); clusters = []; group = [pts[0]]
        for p in pts[1:]:
            if (p - group[0]) / group[0] <= tol: group.append(p)
            else: clusters.append(np.mean(group)); group = [p]
        clusters.append(np.mean(group))
        return [c for c in clusters if sum(1 for p in pts if abs(p-c)/c <= tol) >= 2]

    cp = ohlcv[-1][4]
    return (sorted([s for s in cluster(pl, tolerance) if s < cp], reverse=True),
            sorted([r for r in cluster(ph, tolerance) if r > cp]))


def calculate_atr(ohlcv, period=14):
    if len(ohlcv) < period + 1: return None
    trs = [max(ohlcv[i][2]-ohlcv[i][3],
               abs(ohlcv[i][2]-ohlcv[i-1][4]),
               abs(ohlcv[i][3]-ohlcv[i-1][4]))
           for i in range(1, len(ohlcv))]
    atr = np.mean(trs[:period])
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def dynamic_atr_multipliers(btc_vol):
    if btc_vol > 3.0:   sk, tk = 2.0, 3.0
    elif btc_vol > 1.5: sk, tk = 1.5, 2.5
    else:               sk, tk = 1.2, 2.0
    return sk, tk, f"1:{tk/sk:.1f}"


def calculate_mfi(ohlcv, period=14):
    if len(ohlcv) < period + 1: return 50.0
    tp_prev = None; pos_mf = []; neg_mf = []
    for c in ohlcv[-(period+1):]:
        h, l, cl, v = c[2], c[3], c[4], c[5]
        tp = (h + l + cl) / 3; mf = tp * v
        if tp_prev is not None:
            if tp > tp_prev:   pos_mf.append(mf); neg_mf.append(0.0)
            elif tp < tp_prev: neg_mf.append(mf); pos_mf.append(0.0)
            else:              pos_mf.append(0.0); neg_mf.append(0.0)
        tp_prev = tp
    pmf = sum(pos_mf); nmf = sum(neg_mf)
    if nmf == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + pmf / nmf))


def calculate_rsi_wilder(closes, period=14):
    if len(closes) < period + 1: return 50.0
    d  = np.diff(closes)
    g  = np.where(d > 0, d, 0.0); ls = np.where(d < 0, -d, 0.0)
    ag = np.mean(g[:period]);      al = np.mean(ls[:period])
    for i in range(period, len(g)):
        ag = (ag * (period-1) + g[i])  / period
        al = (al * (period-1) + ls[i]) / period
    if al == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + ag / al))


def update_td_counters(ohlcv, mode='long'):
    closed = ohlcv[:-1]
    closes = [x[4] for x in closed]; lows = [x[3] for x in closed]; highs = [x[2] for x in closed]
    n = len(closes)
    if n < 14: return False, False, False

    s_count = 0; in_c = False; c_count = 0
    setup_high = None; setup_low = None; setup_bars = []
    m9_signal = False; m9_perfect = False; m13_signal = False
    last_idx  = n - 1

    for i in range(4, n):
        c = closes[i]; c4 = closes[i-4]
        sc = (c < c4) if mode == 'long' else (c > c4)

        if not in_c:
            if sc:
                s_count += 1; setup_bars.append(i)
                if s_count == 9:
                    if len(setup_bars) >= 9:
                        i6,i7,i8,i9 = setup_bars[-4],setup_bars[-3],setup_bars[-2],setup_bars[-1]
                        if mode == 'long':
                            perfect = ((lows[i8] < lows[i6] and lows[i8] < lows[i7]) or
                                       (lows[i9] < lows[i6] and lows[i9] < lows[i7]))
                        else:
                            perfect = ((highs[i8] > highs[i6] and highs[i8] > highs[i7]) or
                                       (highs[i9] > highs[i6] and highs[i9] > highs[i7]))
                    else: perfect = False
                    if i == last_idx: m9_signal = True; m9_perfect = perfect
                    setup_high = max(highs[b] for b in setup_bars)
                    setup_low  = min(lows[b]  for b in setup_bars)
                    in_c = True; c_count = 0; s_count = 0; setup_bars = []
            else: s_count = 0; setup_bars = []
        else:
            if mode == 'long' and setup_high and c > setup_high:
                in_c = False; c_count = 0; setup_high = None; setup_low = None
                s_count = 1 if sc else 0; setup_bars = [i] if sc else []; continue
            elif mode == 'short' and setup_low and c < setup_low:
                in_c = False; c_count = 0; setup_high = None; setup_low = None
                s_count = 1 if sc else 0; setup_bars = [i] if sc else []; continue
            if i >= 2:
                cd = (c <= lows[i-2]) if mode == 'long' else (c >= highs[i-2])
                if cd:
                    c_count += 1
                    if c_count == 13:
                        if i == last_idx: m13_signal = True
                        in_c = False; c_count = 0; setup_high = None; setup_low = None
            if in_c and c_count > 30:
                in_c = False; c_count = 0; setup_high = None; setup_low = None

    return m9_signal, m9_perfect, m13_signal


def check_hammer(ohlcv, mode='long'):
    if len(ohlcv) < 2: return False
    o, h, l, c = ohlcv[-2][1], ohlcv[-2][2], ohlcv[-2][3], ohlcv[-2][4]
    body = abs(c - o); fr = h - l if h != l else 1e-9
    if mode == 'long':
        return (min(o,c) - l) / fr > 0.6 and body / fr < 0.3
    else:
        return (h - max(o,c)) / fr > 0.6 and body / fr < 0.3


def _clean_ticker_for_chart(name: str) -> str:
    """
    Очистка тикера для ссылок на TradingView и CoinGlass.
    На Binance Futures нет STOCK тикеров (NVDASTOCK, MSFT-STOCK и т.п.),
    которые были на MEXC. Функция оставлена как заглушка для совместимости —
    просто возвращает имя как есть.
    Если когда-нибудь Binance добавит экзотические тикеры — здесь можно
    вернуть нужную чистку.
    """
    return name


def build_tv_link(symbol):
    # symbol: 'BTC/USDT:USDT' → 'BTCUSDT.P' на BINANCE
    base = symbol.split('/')[0]
    tv = f"{base}USDT.P"
    return f"🔗 <a href='https://www.tradingview.com/chart/?symbol=BINANCE:{tv}'>TradingView</a>"


def build_coinglass_link(symbol):
    coin = _clean_ticker_for_chart(symbol.split("/")[0])
    return f"📊 <a href='https://www.coinglass.com/tv/Binance_{coin}USDT'>CoinGlass СуперГрафик</a>"


def send_msg(text):
    """
    Отправляет сообщение в Telegram.
    Возвращает True если доставлено, False при ошибке.
    При 429 (flood control) делает до 2 retry с указанной паузой.
    После успешной отправки делает паузу 0.5с — Telegram limit ~1 msg/sec.
    """
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    for attempt in range(3):  # до 3 попыток
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10)
            if r.ok:
                time.sleep(0.5)  # защита от flood control при пачке алертов
                return True
            # 429 — flood control, ждём столько сколько просит Telegram + 2 секунды
            if r.status_code == 429:
                try:
                    retry_after = r.json().get('parameters', {}).get('retry_after', 30)
                except Exception:
                    retry_after = 30
                logging.warning(f"TG 429 flood control, waiting {retry_after}s (attempt {attempt+1}/3)")
                if attempt < 2:
                    time.sleep(retry_after + 2)
                    continue
            # Другие ошибки — логируем и не повторяем
            logging.error(f"TG error: {r.status_code} {r.text[:200]}")
            return False
        except Exception as e:
            logging.error(f"TG send error: {e}")
            if attempt < 2:
                time.sleep(5)
                continue
            return False
    return False


def get_market_context():
    """Контекст рынка по BTC/ETH 4H. Использует safe_api_call для обработки 418."""
    for attempt in range(3):
        try:
            btc = safe_api_call(exchange.fetch_ohlcv, 'BTC/USDT:USDT', '4h', limit=5)
            if btc is None:
                # Бан или сетевая ошибка — пробуем ещё раз
                logging.warning(f"market_context attempt {attempt+1}: safe_api_call returned None")
                time.sleep(5)
                continue
            btc_ch = ((btc[-1][4] - btc[-2][4]) / btc[-2][4]) * 100
            eth = safe_api_call(exchange.fetch_ohlcv, 'ETH/USDT:USDT', '4h', limit=5)
            if eth is None:
                logging.warning(f"market_context attempt {attempt+1}: eth fetch returned None")
                time.sleep(5)
                continue
            eth_ch = ((eth[-1][4] - eth[-2][4]) / eth[-2][4]) * 100
            btc_moves = [abs((btc[i][4]-btc[i-1][4])/btc[i-1][4])*100 for i in range(1,5)]
            return {"btc_trend": "🟢" if btc_ch > -0.3 else "🔴",
                    "btc_ch": btc_ch, "btc_p": btc[-1][4],
                    "alt_power": "🚀" if eth_ch-btc_ch > 0.5 else "⚓️",
                    "alt_ch": eth_ch - btc_ch,
                    "btc_vol": np.mean(btc_moves)}
        except Exception as e:
            logging.warning(f"market_context attempt {attempt+1}: {e}"); time.sleep(5)
    return {"btc_trend":"⚪️","btc_ch":0,"btc_p":0,"alt_power":"⚪️","alt_ch":0,"btc_vol":1.0}


def fetch_ohlcv_with_retry(symbol: str, timeframe: str, limit: int,
                             max_retries: int = 2):
    """
    Запрос свечей с поддержкой global circuit breaker (Binance v7.2).

    Алгоритм:
    1. Перед каждой попыткой проверяем глобальный флаг IP_BAN_UNTIL через wait_if_banned():
       - бан активен < 180с → ждём, потом пробуем
       - бан активен ≥ 180с → возвращаем None немедленно (skip монеты)
    2. При получении 418/RateLimitExceeded — парсим точное время разбана из ответа Binance
       ("banned until <ms>") и устанавливаем глобальный флаг для всех потоков.
    3. После каждого успешного запроса — обновляем USED_WEIGHT_1M из X-MBX-USED-WEIGHT-1m.
       Если weight ≥ 1800 — превентивный sleep до новой минуты.
    4. Возвращаем ohlcv или None.

    Это решает цепную реакцию: один 418 → все остальные fetch_ohlcv в этой итерации
    видят флаг и не идут в API, не сжигают retry-попытки впустую.
    """
    for attempt in range(max_retries + 1):
        # Проверяем глобальный флаг бана перед каждой попыткой
        if not wait_if_banned():
            return None

        try:
            result = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            update_used_weight_from_headers()
            return result

        except ccxt.RateLimitExceeded as e:
            # 418/429 от Binance
            err_str = str(e)
            ban_ts = _parse_banned_until_ms(err_str)
            if ban_ts:
                # Точное время из ответа — выставляем глобальный флаг
                set_ip_ban_until(ban_ts)
                logging.warning(
                    f"retry {symbol}: 418 banned until "
                    f"{datetime.fromtimestamp(ban_ts).strftime('%H:%M:%S')} "
                    f"(attempt {attempt+1}/{max_retries+1})"
                )
            elif '418' in err_str or 'banned' in err_str.lower():
                # 418 без timestamp в тексте — ставим +60 сек
                set_ip_ban_until(time.time() + 60)
                logging.warning(
                    f"retry {symbol}: 418 banned (no ts), +60s "
                    f"(attempt {attempt+1}/{max_retries+1})"
                )
            else:
                # 429 без явного 418 — короткая пауза 10с
                set_ip_ban_until(time.time() + 10)
                logging.warning(
                    f"retry {symbol}: 429 rate limit, +10s "
                    f"(attempt {attempt+1}/{max_retries+1})"
                )
            # Следующая попытка вызовет wait_if_banned() в начале цикла

        except ccxt.ExchangeNotAvailable as e:
            # Может прилететь как ExchangeNotAvailable вместо RateLimitExceeded
            err_str = str(e)
            ban_ts = _parse_banned_until_ms(err_str)
            if ban_ts:
                set_ip_ban_until(ban_ts)
            else:
                set_ip_ban_until(time.time() + 30)
            logging.warning(
                f"retry {symbol}: ExchangeNotAvailable "
                f"(attempt {attempt+1}/{max_retries+1})"
            )

        except ccxt.NetworkError as e:
            logging.warning(f"retry {symbol}: NetworkError "
                            f"(attempt {attempt+1}/{max_retries+1}): {e}")
            if attempt < max_retries:
                time.sleep(3)
            else:
                return None

    # Все попытки исчерпаны
    logging.error(f"retry {symbol}: исчерпаны попытки {timeframe} limit={limit}")
    return None


def adaptive_threshold(base, btc_vol, is_priority):
    t = base
    if btc_vol > 3.0:   t += 2
    elif btc_vol < 0.5: t -= 1
    if is_priority:     t -= 1
    return max(t, 3)


def detect_volatility_squeeze(ohlcv, period=5, avg_period=20):
    if len(ohlcv) < avg_period + period + 1:
        return False, 1.0, 0.0, 0, ""

    closes = [c[4] for c in ohlcv]
    highs  = [c[2] for c in ohlcv]
    lows   = [c[3] for c in ohlcv]

    def calc_atr_range(start, end):
        trs = []
        for i in range(start + 1, end):
            h  = highs[i]; l = lows[i]; pc = closes[i-1]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return np.mean(trs) if trs else 0.0

    recent_atr = calc_atr_range(len(ohlcv) - period - 1, len(ohlcv))
    avg_atr    = calc_atr_range(len(ohlcv) - avg_period - 1, len(ohlcv) - period)

    if avg_atr == 0:
        return False, 1.0, 0.0, 0, ""

    squeeze_ratio = recent_atr / avg_atr

    slope_pct = (closes[-1] - closes[-period - 1]) / closes[-period - 1] * 100 \
                if closes[-period - 1] > 0 else 0.0

    bars_count = 0
    for i in range(len(ohlcv) - 1, len(ohlcv) - period - 1, -1):
        if i < 1: break
        bar_range = highs[i] - lows[i]
        if bar_range < avg_atr * 0.7:
            bars_count += 1
        else:
            break

    is_squeeze = squeeze_ratio < 0.6 and bars_count >= 3

    if not is_squeeze:
        return False, squeeze_ratio, slope_pct, bars_count, ""

    # Binance v7.1: упрощённый лейбл без дублей.
    # Раньше было: "Сжатие: 3 св | ATR x0.34 | ↘ наклон вниз (-3.3%) | ⚡ Готов к взрыву"
    # Направление теперь показывается в блоке скоринга направления взрыва,
    # "готов к взрыву" — само собой подразумевается алертом.
    # Оставляем только уникальную информацию: счётчик баров + ATR ratio.
    label = f"🗜 Короткое: {bars_count} св | ATR x{squeeze_ratio:.2f}"

    return True, squeeze_ratio, slope_pct, bars_count, label


def detect_atr_map_squeeze(ohlcv, atr_length=14, baseline_length=50,
                            range_window=20, noise_window=10,
                            containment_window=20,
                            compression_threshold=60, mature_threshold=80):
    if len(ohlcv) < baseline_length + 5:
        return 0, False, False, "", {}

    closes = [c[4] for c in ohlcv]
    opens  = [c[1] for c in ohlcv]
    highs  = [c[2] for c in ohlcv]
    lows   = [c[3] for c in ohlcv]

    def atr_calc(start, end):
        trs = []
        for i in range(max(start, 1), end):
            tr = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i-1]),
                     abs(lows[i]  - closes[i-1]))
            trs.append(tr)
        return np.mean(trs) if trs else 0.0

    n = len(ohlcv)
    current_atr  = atr_calc(n - atr_length, n)
    baseline_atr = atr_calc(n - baseline_length, n - atr_length)

    if baseline_atr == 0:
        return 0, False, False, "", {}

    atr_ratio = current_atr / baseline_atr
    atr_score = max(0, min(100, (1.0 - atr_ratio) * 200))

    recent_ranges  = [highs[i] - lows[i] for i in range(n - range_window, n)]
    long_ranges    = [highs[i] - lows[i] for i in range(n - baseline_length, n - range_window)]
    avg_recent     = np.mean(recent_ranges) if recent_ranges else 0.0
    avg_long       = np.mean(long_ranges) if long_ranges else 0.0

    if avg_long == 0:
        range_score = 0
    else:
        range_ratio = avg_recent / avg_long
        range_score = max(0, min(100, (1.0 - range_ratio) * 200))

    body_ratios = []
    for i in range(n - noise_window, n):
        full_range = highs[i] - lows[i]
        body       = abs(closes[i] - opens[i])
        if full_range > 0:
            body_ratios.append(body / full_range)
    avg_body_ratio = np.mean(body_ratios) if body_ratios else 1.0
    noise_score = max(0, min(100, (1.0 - avg_body_ratio) * 150))

    contained_count = 0
    for i in range(n - containment_window, n):
        if i < 1: continue
        if highs[i] <= highs[i-1] and lows[i] >= lows[i-1]:
            contained_count += 1
    containment_score = (contained_count / containment_window) * 100

    total_score = (atr_score * 0.30 +
                   range_score * 0.30 +
                   noise_score * 0.20 +
                   containment_score * 0.20)

    is_forming = total_score >= compression_threshold
    is_mature  = total_score >= mature_threshold

    components = {
        'atr':        round(atr_score, 1),
        'range':      round(range_score, 1),
        'noise':      round(noise_score, 1),
        'containment':round(containment_score, 1),
        'total':      round(total_score, 1),
        'atr_ratio':  round(atr_ratio, 2),
    }

    if not is_forming:
        return total_score, False, False, "", components

    label_lines = [
        f"📊 ATR Map: {total_score:.0f}/100 "
        f"({'🟠 ЗРЕЛОЕ' if is_mature else '🟡 формируется'})",
        f"   ATR x{atr_ratio:.2f} | Range {range_score:.0f} | "
        f"Noise {noise_score:.0f} | Cont {containment_score:.0f}"
    ]
    label = "\n".join(label_lines)

    return total_score, is_forming, is_mature, label, components


# ═══════════════════════════════════════════════════════════════════════
# BINANCE-СПЕЦИФИЧНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════════

def fetch_all_funding_rates(exchange):
    """
    Загружает funding rate для ВСЕХ USDT-perp одним запросом.
    На Binance это endpoint /fapi/v1/premiumIndex — возвращает все пары.
    Weight ~5, время ~0.5 сек.

    v7.2: использует safe_api_call → если IP в бане, ждёт или возвращает {}.

    Возвращает: {symbol: funding_rate (float, например 0.0001 = 0.01%)}
    """
    rates = safe_api_call(exchange.fetch_funding_rates)
    if rates is None:
        logging.warning("fetch_funding_rates: safe_api_call returned None (ban or error)")
        return {}
    try:
        return {s: (r.get('fundingRate') or 0.0) for s, r in rates.items()}
    except Exception as e:
        logging.warning(f"fetch_funding_rates parse failed: {e}")
        return {}


# Snapshot OI в памяти. Раз в итерацию записываем текущий OI.
# Цикл итерации ~6-7 мин → 24h = ~220 точек. Храним последние 240.
# При рестарте Render всё теряется — первые 24h после деплоя OI динамики нет.
from collections import deque
oi_history: dict = {}  # symbol → deque[(timestamp, oi_value)]


def record_oi_snapshot(symbol: str, oi_value: float):
    """Записывает текущий OI в историю. Хранит ~24h данных."""
    if not oi_value or oi_value <= 0:
        return
    if symbol not in oi_history:
        oi_history[symbol] = deque(maxlen=240)
    oi_history[symbol].append((time.time(), oi_value))


def get_oi_24h_change(symbol: str, current_oi: float):
    """
    Возвращает изменение OI за 24h в процентах, или None если истории мало.
    Использует точку ~24h назад (или ближайшую если её нет).
    """
    if symbol not in oi_history or current_oi <= 0:
        return None
    hist = oi_history[symbol]
    if len(hist) < 2:
        return None

    now = time.time()
    target_time = now - 24 * 3600  # 24h назад

    # Ищем самую ранную запись которая ≥ 23 часа назад
    oldest_eligible = None
    for ts, oi_val in hist:
        if ts <= target_time + 3600:  # не позже чем 23h назад
            oldest_eligible = (ts, oi_val)
            break

    if not oldest_eligible:
        return None

    _, old_oi = oldest_eligible
    if old_oi <= 0:
        return None
    return (current_oi - old_oi) / old_oi * 100


def get_oi_from_ticker(symbol: str, tickers: dict, current_price: float):
    """
    Извлекает OI (Open Interest) из ticker_24hr Binance.
    Binance Futures отдаёт `openInterest` в info — это количество контрактов.
    Для USDM-perp 1 контракт = 1 единица base-актива (например 1 BTC).
    Чтобы получить notional ($), умножаем на текущую цену.

    Возвращает notional OI в долларах, или 0 при ошибке.
    """
    t = tickers.get(symbol, {})
    info = t.get('info', {}) or {}
    try:
        oi_contracts = float(info.get('openInterest') or 0)
    except (ValueError, TypeError):
        oi_contracts = 0.0
    if oi_contracts <= 0 or current_price <= 0:
        return 0.0
    return oi_contracts * current_price


def score_breakout_direction(
    ema_trend: str,
    cvd_state: str,
    range_position: float,
    btc_4h_change: float,
    funding_rate: float,
    oi_24h_change,           # None если истории нет
    current_price: float,
    ema_value: float
):
    """
    Скоринг направления взрыва из сжатия. 6 факторов, каждый +2/+1/-1/-2.
    Возвращает: (direction, label, score_up, score_down, breakdown_dict)
    
    direction: 'UP' | 'DOWN' | 'SIDEWAYS'
    label: текст для алерта
    breakdown_dict: {factor: score} для отображения
    """
    score_up = 0
    score_down = 0
    breakdown = {}

    # 1. EMA тренд
    if ema_trend == 'bull_strong':
        breakdown['EMA'] = +2
        score_up += 2
    elif ema_trend == 'bull_weak':
        breakdown['EMA'] = +1
        score_up += 1
    elif ema_trend == 'bear_strong':
        breakdown['EMA'] = -2
        score_down += 2
    elif ema_trend == 'bear_weak':
        breakdown['EMA'] = -1
        score_down += 1
    else:
        breakdown['EMA'] = 0

    # 2. CVD
    if cvd_state == 'bull':
        breakdown['CVD'] = +2
        score_up += 2
    elif cvd_state == 'bull_div':
        breakdown['CVD'] = +1
        score_up += 1
    elif cvd_state == 'bear':
        breakdown['CVD'] = -2
        score_down += 2
    elif cvd_state == 'bear_div':
        breakdown['CVD'] = -1
        score_down += 1
    else:
        breakdown['CVD'] = 0

    # 3. Положение цены в диапазоне (0.0..1.0, где 0 - дно, 1 - вершина)
    if range_position >= 0.75:
        breakdown['Range'] = +1
        score_up += 1
    elif range_position >= 0.50:
        breakdown['Range'] = 0
    elif range_position >= 0.25:
        breakdown['Range'] = 0
    else:
        breakdown['Range'] = -1
        score_down += 1

    # 4. BTC 4h контекст
    if btc_4h_change >= 1.5:
        breakdown['BTC'] = +2
        score_up += 2
    elif btc_4h_change >= 0.5:
        breakdown['BTC'] = +1
        score_up += 1
    elif btc_4h_change <= -1.5:
        breakdown['BTC'] = -2
        score_down += 2
    elif btc_4h_change <= -0.5:
        breakdown['BTC'] = -1
        score_down += 1
    else:
        breakdown['BTC'] = 0

    # 5. Funding rate (контртренд!)
    # положительный funding → лонги платят → перегрев лонгов → потенциал ВНИЗ (-1 к UP)
    # отрицательный funding → шорты платят → перегрев шортов → потенциал ВВЕРХ (+1 к UP)
    if funding_rate <= -0.0005:    # ≤ -0.05%
        breakdown['Fund'] = +2
        score_up += 2
    elif funding_rate <= -0.0001:   # ≤ -0.01%
        breakdown['Fund'] = +1
        score_up += 1
    elif funding_rate >= 0.0005:    # ≥ +0.05%
        breakdown['Fund'] = -2
        score_down += 2
    elif funding_rate >= 0.0001:    # ≥ +0.01%
        breakdown['Fund'] = -1
        score_down += 1
    else:
        breakdown['Fund'] = 0

    # 6. OI 24h динамика
    # Если истории нет — фактор пропускается (None)
    if oi_24h_change is None:
        breakdown['OI'] = None
    else:
        # Растёт OI + цена растёт = бычий приток (+1 UP)
        # Растёт OI + цена стоит = накопление лонгов? (+1 UP)
        # Падает OI + цена стоит = закрытие шортов (-1 UP по нашей логике контртренд: лонги выходят, ВНИЗ? зависит от ситуации)
        # Проще: чисто рост OI ≥10% → +1 UP, падение OI ≥10% → -1 UP
        if oi_24h_change >= 15:
            breakdown['OI'] = +2
            score_up += 2
        elif oi_24h_change >= 5:
            breakdown['OI'] = +1
            score_up += 1
        elif oi_24h_change <= -15:
            breakdown['OI'] = -2
            score_down += 2
        elif oi_24h_change <= -5:
            breakdown['OI'] = -1
            score_down += 1
        else:
            breakdown['OI'] = 0

    # Решение по дельте score
    delta = score_up - score_down

    if delta >= 6:
        direction = 'UP'
        label = "ВВЕРХ ↑ (уверенно) 🔥"
    elif delta >= 3:
        direction = 'UP'
        label = "ВВЕРХ ↗ (вероятно)"
    elif delta <= -6:
        direction = 'DOWN'
        label = "ВНИЗ ↓ (уверенно) 🔥"
    elif delta <= -3:
        direction = 'DOWN'
        label = "ВНИЗ ↘ (вероятно)"
    else:
        direction = 'SIDEWAYS'
        label = "⚖️ Боковик — направление неясно"

    return direction, label, score_up, score_down, breakdown


def format_breakdown(breakdown: dict, score_up: int, score_down: int):
    """Форматирует строку разбора скоринга для алерта.
    Binance v7.1: показываем 5 факторов (без OI). OI временно отключён —
    нужен отдельный запрос к Binance API, что добавляет нагрузку.
    Вернём OI отдельным модулем позже.
    """
    parts = []
    for key in ['EMA', 'CVD', 'Range', 'BTC', 'Fund']:  # OI убран
        val = breakdown.get(key)
        if val is None:
            parts.append(f"{key} —")
        elif val > 0:
            parts.append(f"{key} +{val}")
        else:
            parts.append(f"{key} {val}")
    return " | ".join(parts) + f" = {score_up} vs {score_down}"


def format_volume_block(vol_24h: float, vol_avg_7d: float):
    """
    Форматирует объёмный блок одной строкой:
    📦 V24h: $3.0M | Avg7d: $1.2M | 💎 2.5x

    Множитель vol_24h / vol_avg_7d даёт эмодзи:
    ≥3.0: 💎  | 2.0-3.0: ❗  | 1.3-2.0: 🔼  | 0.8-1.3: ➖  | <0.8: 🔻
    """
    def fmt_money(v):
        if v >= 1e9:
            return f"${v/1e9:.1f}B"
        elif v >= 1e6:
            return f"${v/1e6:.1f}M"
        elif v >= 1e3:
            return f"${v/1e3:.0f}K"
        else:
            return f"${v:.0f}"

    if vol_avg_7d <= 0:
        return f"📦 V24h: {fmt_money(vol_24h)} | Avg7d: —"

    ratio = vol_24h / vol_avg_7d
    if ratio >= 3.0:
        emoji = "💎"
    elif ratio >= 2.0:
        emoji = "❗"
    elif ratio >= 1.3:
        emoji = "🔼"
    elif ratio >= 0.8:
        emoji = "➖"
    else:
        emoji = "🔻"

    return (f"📦 V24h: {fmt_money(vol_24h)} | "
            f"Avg7d: {fmt_money(vol_avg_7d)} | "
            f"{emoji} {ratio:.2f}x")


def calc_mature_bars(score_history: list, threshold: int = 60):
    """
    Считает сколько последних подряд закрытых баров имели ATR Map score ≥ threshold.
    score_history — список score'ов от старых к новым.
    """
    count = 0
    for s in reversed(score_history):
        if s >= threshold:
            count += 1
        else:
            break
    return count


def pluralize_bars(n: int) -> str:
    """
    Правильная русская грамматика для счётчика баров:
        1 бар | 2 бара | 5 баров | 11 баров | 22 бара | 25 баров
    """
    n = abs(int(n))
    if n % 100 in (11, 12, 13, 14):
        return f"{n} баров"
    last = n % 10
    if last == 1:
        return f"{n} бар"
    if last in (2, 3, 4):
        return f"{n} бара"
    return f"{n} баров"


def get_range_position(highs_recent, lows_recent, current_price):
    """
    Положение текущей цены в диапазоне последних N баров.
    Возвращает 0..1, где 0 = на минимуме, 1 = на максимуме.
    """
    if not highs_recent or not lows_recent:
        return 0.5
    rng_hi = max(highs_recent)
    rng_lo = min(lows_recent)
    if rng_hi <= rng_lo:
        return 0.5
    return max(0.0, min(1.0, (current_price - rng_lo) / (rng_hi - rng_lo)))


def get_pattern_type(is_atr_map_forming: bool, is_short_squeeze: bool):
    """
    Определяет тип паттерна для алерта СЖАТИЕ:
    - "пробой из боковика" (двойное подтверждение) — atr_map + is_sq
    - "классическое сжатие" — только atr_map
    - "остановка отката" — только is_sq
    """
    if is_atr_map_forming and is_short_squeeze:
        return "💎 Паттерн: пробой из боковика (двойное подтверждение)"
    elif is_atr_map_forming:
        return "🌀 Паттерн: классическое сжатие в боковике"
    elif is_short_squeeze:
        return "🏃 Паттерн: остановка отката (продолжение тренда)"
    else:
        return ""


# ═══════════════════════════════════════════════════════════════════════
# ОСНОВНОЙ ЦИКЛ
# ═══════════════════════════════════════════════════════════════════════
def analyst_loop():
    sent_signals = {}
    sent_attention = {}
    # История ATR Map score для подсчёта mature_bars (сколько баров подряд score≥60).
    # Ключ: symbol → deque последних 12 score'ов (24h при cycle 6-7 мин).
    atr_map_score_history: dict = {}
    logging.info("Аналитик Binance v7.1 запущен (REVERSAL_4H + ВНИМАНИЕ + 2H NEW).")

    try:
        exchange.load_markets()
        logging.info("Рынки Binance загружены.")
    except Exception as e:
        logging.error(f"Ошибка загрузки рынков: {e}")

    markets_reload_ts = time.time()

    while True:
        try:
            if time.time() - markets_reload_ts > 3600:
                try:
                    exchange.load_markets(); markets_reload_ts = time.time()
                except Exception as e:
                    logging.error(f"Перезагрузка рынков: {e}")

            ctx = get_market_context()

            # v7.2: fetch_tickers через safe_api_call (учитывает global ban flag)
            tickers = safe_api_call(exchange.fetch_tickers)
            if tickers is None:
                logging.error("fetch_tickers: ban or error, пауза 60с")
                time.sleep(60)
                continue

            # Funding rate для всех монет одним запросом (~5 weight).
            # Используется в скоринге направления взрыва.
            funding_rates = fetch_all_funding_rates(exchange)
            logging.info(f"Funding rates загружены: {len(funding_rates)} монет")

            # Накопитель для алертов СЖАТИЕ — собираем за итерацию,
            # потом сортируем по ATR Map score DESC и отправляем (сначала сильные).
            compression_candidates = []

            # Только USDT-пары: исключает дубликаты USDC-пар (XAUT/USDC:USDC и т.п.)
            # и фильтрует низколиквидные альтернативные котировки.
            active_swaps = [s for s, m in exchange.markets.items()
                            if m.get('active')
                            and m.get('type') == 'swap'
                            and m.get('quote') == 'USDT']

            vol_data = sorted(
                [{'s': s, 'v': tickers[s].get('quoteVolume', 0)}
                 for s in active_swaps if s in tickers],
                key=lambda x: x['v'], reverse=True)

            top100_4h = [x['s'] for x in vol_data]  # ВСЕ активные USDT-perp Binance (~630)
            symbols   = list(dict.fromkeys(
                [w for w in WATCHLIST if w in tickers] + top100_4h))

            for symbol in symbols:
                try:
                    vol_24h = tickers.get(symbol, {}).get('quoteVolume', 0) or 0
                    if vol_24h < MIN_VOLUME_ATTENTION:
                        continue

                    # 4H блок: limit=99 → weight 1 (Binance v7.2: было 100).
                    # На Binance Futures klines с limit ≥ 100 стоит weight 2,
                    # с limit ≤ 99 — weight 1. Экономия ~600 weight/итерацию.
                    # 99 свечей 4H = 16.5 дней истории — достаточно для ATR Map
                    # (baseline 50) и REVERSAL (lookback 10) с запасом 30 свечей.
                    # Логика ATR Map / REVERSAL не задета — параметры неизменны.
                    ohlcv = fetch_ohlcv_with_retry(symbol, '4h', limit=99, max_retries=2)
                    if ohlcv is None or len(ohlcv) < 80:
                        continue

                    is_wl         = symbol in WATCHLIST
                    current_price = ohlcv[-1][4]
                    closed        = ohlcv[:-1]
                    closes_closed = [x[4] for x in closed]

                    last_closed_ts = closed[-1][0]
                    candle_id      = last_closed_ts // 14_400_000
                    la_key    = f"{symbol}_{candle_id}_la"
                    sa_key    = f"{symbol}_{candle_id}_sa"
                    rev_l_key = f"{symbol}_{candle_id}_rev_l"
                    rev_s_key = f"{symbol}_{candle_id}_rev_s"

                    all_done = (rev_l_key in sent_signals and rev_s_key in sent_signals
                                and la_key in sent_attention and sa_key in sent_attention)
                    if all_done:
                        continue

                    price_ch = ((closed[-1][4] - closed[-2][4]) / closed[-2][4] * 100) \
                               if len(closed) >= 2 else 0.0

                    rsi = calculate_rsi_wilder(closes_closed)
                    mfi = calculate_mfi(closed)
                    atr = calculate_atr(closed)

                    c_high = closed[-1][2]; c_low = closed[-1][3]; c_close = closed[-1][4]
                    buy_pressure = (c_close - c_low) / (c_high - c_low) if c_high != c_low else 0.5
                    imb = (buy_pressure - 0.5) * 200

                    vol_score, vol_label, v_rel, v_zscore, avg_buy_pct, vol_detail = \
                        get_volume_info(closed, vol_24h)

                    ssma_val, ssma_trend, ssma_slope = calculate_ssma(closed, period=24)
                    long_gate  = ssma_allows_long(ssma_val, ssma_trend, ssma_slope, current_price)
                    short_gate = ssma_allows_short(ssma_val, ssma_trend, ssma_slope, current_price)

                    cur_candle    = ohlcv[-1]
                    intrabar_high = cur_candle[2]
                    intrabar_low  = cur_candle[3]
                    intrabar_open = cur_candle[1]

                    intrabar_pullback = (intrabar_high - current_price) / intrabar_high * 100 \
                                        if intrabar_high > 0 else 0.0
                    intrabar_bounce   = (current_price - intrabar_low) / intrabar_low * 100 \
                                        if intrabar_low > 0 else 0.0

                    v_avg_cur = np.mean([x[5] for x in closed[-20:]]) if len(closed) >= 20 else 1.0
                    cur_vol_rel = cur_candle[5] / v_avg_cur if v_avg_cur > 0 else 1.0

                    intrabar_top_reversal = (
                        intrabar_pullback >= 1.5
                        and cur_vol_rel >= 1.2
                        and current_price < intrabar_high * 0.985
                        and short_gate
                    )
                    intrabar_bottom_reversal = (
                        intrabar_bounce >= 1.5
                        and cur_vol_rel >= 1.2
                        and current_price > intrabar_low * 1.015
                        and long_gate
                    )

                    ssma_label = ""
                    if ssma_val:
                        icon = "📈" if 'bull' in ssma_trend else "📉"
                        ssma_label = f"{icon} SSMA: {ssma_val:.4g} ({ssma_slope:+.2f}%/св)"

                    bb_signal, bb_dist, bb_upper, bb_lower, bb_basis = \
                        calculate_bb_outside_atr(closed)

                    sw_low_pct, sw_high_pct, near_sw_low, near_sw_high, sw_low, sw_high = \
                        calculate_swing_hilo(closed, swing_bars=20)

                    cvd_level, cvd_total = calc_cvd_level(closed)
                    cvd_div_l = get_cvd_divergence(closed, 'long')
                    cvd_div_s = get_cvd_divergence(closed, 'short')

                    rsi_div_bull, rsi_div_bear = calc_rsi_divergence(closed)

                    hammer_l = check_hammer(ohlcv, 'long')
                    hammer_s = check_hammer(ohlcv, 'short')

                    m9_l, m9_perfect_l, m13_l = update_td_counters(ohlcv, 'long')
                    m9_s, m9_perfect_s, m13_s = update_td_counters(ohlcv, 'short')

                    supports, resistances = get_pivot_levels(closed)
                    sup = supports[0]    if supports    else min(x[3] for x in closed[-60:])
                    res = resistances[0] if resistances else max(x[2] for x in closed[-60:])

                    stop_k, take_k, rr_str = dynamic_atr_multipliers(ctx['btc_vol'])
                    if atr:
                        stop_l = current_price - stop_k * atr
                        target_l = current_price + take_k * atr
                        stop_s = current_price + stop_k * atr
                        target_s = current_price - take_k * atr
                    else:
                        stop_l = stop_s = target_l = target_s = None

                    wyckoff_phase, wyckoff_long_ok, wyckoff_short_ok, wyckoff_label, wyckoff_strength = \
                        detect_wyckoff_phase(closed, sw_low or sup, sw_high or res, cvd_level)

                    after_dump, after_pump, big_move_pct, candles_since_move = \
                        check_post_move_filter(ohlcv, lookback=12)

                    if after_dump and not wyckoff_long_ok:
                        wyckoff_long_ok  = True; wyckoff_short_ok = False
                        wyckoff_label    = f"🌱 После дампа -{big_move_pct:.0f}%"
                    if after_pump and not wyckoff_short_ok:
                        wyckoff_short_ok = True; wyckoff_long_ok  = False
                        wyckoff_label    = f"🔝 После памп +{big_move_pct:.0f}%"

                    ew     = analyze_elliott(closed, swing_bars=10)
                    ew_big = analyze_elliott(closed, swing_bars=20)
                    is_sq_4h, sq_ratio_4h, sq_slope_4h, sq_bars_4h, sq_label_4h = \
                        detect_volatility_squeeze(closed, period=5, avg_period=20)

                    if ew_big['structure'] != 'neutral' and ew_big['wave_label'] != ew['wave_label']:
                        ew_big_label = f"🌍 Глобально: <b>{ew_big['wave_label']}</b>"
                        if ew_big['ext_1618']:
                            ew_big_label += (
                                f"\n   Цели W3 (глоб): "
                                f"×1.618={ew_big['ext_1618']:.4g} | "
                                f"×2.618={ew_big['ext_2618']:.4g}"
                            )
                    else:
                        ew_big_label = ""

                    strong_reversal_l = (
                        (m9_l or m13_l)
                        and bb_signal == 'oversold'
                        and v_rel >= 2.0
                        and near_sw_low
                    )
                    strong_reversal_l_intrabar = (
                        (m9_l or m13_l)
                        and intrabar_bounce >= 2.0
                        and cur_vol_rel >= 1.5
                        and near_sw_low
                    )
                    if strong_reversal_l or strong_reversal_l_intrabar:
                        long_gate = True

                    strong_reversal_s = (
                        (m9_s or m13_s)
                        and bb_signal == 'overbought'
                        and v_rel >= 2.0
                        and near_sw_high
                    )
                    strong_reversal_s_intrabar = (
                        (m9_s or m13_s)
                        and intrabar_pullback >= 2.0
                        and cur_vol_rel >= 1.5
                        and near_sw_high
                    )
                    if strong_reversal_s or strong_reversal_s_intrabar:
                        short_gate = True

                    has_any = (cvd_div_l or cvd_div_s or hammer_l or hammer_s or
                               m9_l or m9_s or m13_l or m13_s or
                               rsi_div_bull or rsi_div_bear or vol_score >= 2 or
                               near_sw_low or near_sw_high or bb_signal != 'neutral' or
                               ew['score_long'] >= 2 or ew['score_short'] >= 2)

                    oi_chg=0.0; oi_signal='neutral'; oi_label='⚪️ OI: нет данных'
                    fr_val=0.0; fr_signal='neutral'; fr_label='⚪️ Фандинг: нет данных'

                    if has_any:
                        oi_chg, oi_signal, oi_label = get_oi_data_from_ticker(symbol, tickers, price_ch)
                        fr_val, fr_signal, fr_label  = get_funding_signal(symbol, funding_rates)

                    wl_badge = " ⭐️" if is_wl else ""
                    cvd_emoji = {"bull":"🟢","bull_div":"🟢✨","bear":"🔴","bear_div":"🔴✨"}.get(cvd_level,"⚪️")

                    def calc_score(mode):
                        score = 0; details = []

                        if mode == 'long':
                            if wyckoff_phase == 'accumulation' and wyckoff_strength >= 2:
                                score += 2; details.append("🟢 Накопление")
                            elif wyckoff_phase == 'accumulation':
                                score += 1; details.append("🟢 Накопление")
                        else:
                            if wyckoff_phase == 'distribution' and wyckoff_strength >= 2:
                                score += 2; details.append("🔴 Распределение")
                            elif wyckoff_phase == 'distribution':
                                score += 1; details.append("🔴 Распределение")

                        if mode == 'long' and bb_signal == 'oversold':
                            score += 2; details.append(f"📉 BB ATR Перепродан ({bb_dist:.1f}%)")
                        if mode == 'short' and bb_signal == 'overbought':
                            score += 2; details.append(f"📈 BB ATR Перекуплен ({bb_dist:.1f}%)")

                        if mode == 'long' and near_sw_low:
                            score += 2; details.append(f"🏔️ Swing Low (+{sw_low_pct:.1f}%)")
                        if mode == 'short' and near_sw_high:
                            score += 2; details.append(f"🏔️ Swing High (-{sw_high_pct:.1f}%)")

                        if mode == 'long' and ssma_trend == 'bull_strong':
                            score += 1; details.append(f"📈 SSMA Бык ({ssma_slope:+.2f}%)")
                        if mode == 'short' and ssma_trend == 'bear_strong':
                            score += 1; details.append(f"📉 SSMA Медведь ({ssma_slope:+.2f}%)")

                        if mode == 'long':
                            if oi_signal == 'bull':   score += 2; details.append(f"💹 OI +{oi_chg:.1f}%")
                            elif oi_signal == 'bear': score -= 1
                        else:
                            if oi_signal == 'bear':   score += 2; details.append(f"💹 OI +{oi_chg:.1f}%")
                            elif oi_signal == 'bull': score -= 1

                        if mode == 'long' and cvd_div_l:
                            score += 2; details.append("🔥 CVD Дивер")
                        if mode == 'short' and cvd_div_s:
                            score += 2; details.append("🔥 CVD Дивер")

                        if mode == 'long'  and rsi_div_bull: score += 2; details.append("📉 RSI Дивер")
                        if mode == 'short' and rsi_div_bear: score += 2; details.append("📈 RSI Дивер")

                        if mode == 'long':
                            if m9_l:
                                pts = 3 if m9_perfect_l else 2
                                score += pts; details.append("⏱ M9✨" if m9_perfect_l else "⏱ M9")
                            if m13_l: score += 3; details.append("⏱ M13")
                        else:
                            if m9_s:
                                pts = 3 if m9_perfect_s else 2
                                score += pts; details.append("⏱ M9✨" if m9_perfect_s else "⏱ M9")
                            if m13_s: score += 3; details.append("⏱ M13")

                        if mode == 'long'  and cvd_level in ('bull','bull_div'): score += 1; details.append("📍 CVD")
                        if mode == 'short' and cvd_level in ('bear','bear_div'): score += 1; details.append("📍 CVD")

                        if mode == 'long'  and current_price <= sup * 1.015: score += 2; details.append("🧱 Pivot Sup")
                        if mode == 'short' and current_price >= res * 0.985: score += 2; details.append("🧱 Pivot Res")

                        if vol_score > 0: score += vol_score; details.append(vol_label)

                        if mode == 'long'  and fr_signal == 'bull': score += 1; details.append(f"💸 FR {fr_val:.3f}%")
                        if mode == 'short' and fr_signal == 'bear': score += 1; details.append(f"💸 FR {fr_val:.3f}%")

                        if mode == 'long'  and hammer_l: score += 1; details.append("⚓️ Фитиль")
                        if mode == 'short' and hammer_s: score += 1; details.append("🏹 Фитиль↑")

                        if mode == 'long'  and rsi < 30:  score += 1; details.append(f"📉 RSI {rsi:.0f}")
                        if mode == 'long'  and mfi < 20:  score += 1; details.append(f"💰 MFI {mfi:.0f}")
                        if mode == 'short' and rsi > 70:  score += 1; details.append(f"📈 RSI {rsi:.0f}")
                        if mode == 'short' and mfi > 80:  score += 1; details.append(f"💰 MFI {mfi:.0f}")

                        if mode == 'long'  and ew['score_long'] > 0:
                            score += ew['score_long']; details.extend(ew['details_long'])
                        if mode == 'short' and ew['score_short'] > 0:
                            score += ew['score_short']; details.extend(ew['details_short'])

                        return max(score, 0), details

                    ib_key_l = f"{symbol}_{candle_id}_ibl"
                    ib_key_s = f"{symbol}_{candle_id}_ibs"

                    if (ib_key_l not in sent_attention
                            and intrabar_bottom_reversal
                            and near_sw_low
                            and vol_24h >= MIN_VOLUME_ATTENTION):
                        ib_score, _ = calc_score('long')
                        if ib_score >= SCORE_ATTENTION:
                            msg = (
                                f"⚡️ <b>РАННИЙ ЛОНГ 4H ({ib_score}/10){wl_badge}</b>\n"
                                f"Монета: <b>{symbol}</b> | 🕯 Внутри свечи\n"
                                f"Цена: <code>{current_price:.6g}</code>\n"
                                f"Отскок от лоя: +{intrabar_bounce:.1f}%"
                                f" | Объём: x{cur_vol_rel:.1f}\n"
                                f"Swing Low: +{sw_low_pct:.1f}%\n"
                                f"───────────────────\n"
                                f"🌊 Wyckoff: <b>{wyckoff_label}</b>\n"
                                f"{ssma_label}\n"
                                + (ew['alert_block'] + "\n" if ew['structure'] != 'neutral' else "")
                                + (ew_big_label + "\n" if ew_big_label else "")
                                + (
                                f"───────────────────\n"
                                f"📊 RSI: {rsi:.1f} | MFI: {mfi:.1f}\n"
                                f"{vol_detail}\n"
                                f"{oi_label}\n"
                                f"⚠️ Свеча не закрыта — ждите подтверждения\n"
                                f"───────────────────\n"
                                f"👑 BTC: {ctx['btc_trend']} {ctx['btc_ch']:.1f}%\n"
                                f"{build_tv_link(symbol)}\n{build_coinglass_link(symbol)}"
                                )
                            )
                            if send_msg(msg):
                                sent_attention[ib_key_l] = time.time()
                                bot_status["attention_sent"] += 1
                                logging.info(f"РАННИЙ ЛОНГ: {symbol} bounce={intrabar_bounce:.1f}% vol=x{cur_vol_rel:.1f}")

                    if (ib_key_s not in sent_attention
                            and intrabar_top_reversal
                            and near_sw_high
                            and vol_24h >= MIN_VOLUME_ATTENTION):
                        ib_score, _ = calc_score('short')
                        if ib_score >= SCORE_ATTENTION:
                            msg = (
                                f"⚡️ <b>РАННИЙ ШОРТ 4H ({ib_score}/10){wl_badge}</b>\n"
                                f"Монета: <b>{symbol}</b> | 🕯 Внутри свечи\n"
                                f"Цена: <code>{current_price:.6g}</code>\n"
                                f"Откат от хая: -{intrabar_pullback:.1f}%"
                                f" | Объём: x{cur_vol_rel:.1f}\n"
                                f"Swing High: -{sw_high_pct:.1f}%\n"
                                f"───────────────────\n"
                                f"🌊 Wyckoff: <b>{wyckoff_label}</b>\n"
                                f"{ssma_label}\n"
                                + (ew['alert_block'] + "\n" if ew['structure'] != 'neutral' else "")
                                + (ew_big_label + "\n" if ew_big_label else "")
                                + (
                                f"───────────────────\n"
                                f"📊 RSI: {rsi:.1f} | MFI: {mfi:.1f}\n"
                                f"{vol_detail}\n"
                                f"{oi_label}\n"
                                f"⚠️ Свеча не закрыта — ждите подтверждения\n"
                                f"───────────────────\n"
                                f"👑 BTC: {ctx['btc_trend']} {ctx['btc_ch']:.1f}%\n"
                                f"{build_tv_link(symbol)}\n{build_coinglass_link(symbol)}"
                                )
                            )
                            if send_msg(msg):
                                sent_attention[ib_key_s] = time.time()
                                bot_status["attention_sent"] += 1
                                logging.info(f"РАННИЙ ШОРТ: {symbol} pullback={intrabar_pullback:.1f}% vol=x{cur_vol_rel:.1f}")

                    if (la_key not in sent_attention
                            and near_sw_low
                            and long_gate
                            and vol_24h >= MIN_VOLUME_ATTENTION):

                        a_score, a_details = calc_score('long')
                        rr_ok_a, tgt_pct_a, stp_pct_a, rr_a = check_rr(
                            current_price, target_l, stop_l, 'long',
                            MIN_TARGET_PCT_ATTENTION, MAX_STOP_PCT)

                        if a_score >= SCORE_ATTENTION and rr_ok_a:
                            msg = (
                                f"🔔 <b>ВНИМАНИЕ ЛОНГ 4H ({a_score}/10){wl_badge}</b>\n"
                                f"Монета: <b>{symbol}</b>\n"
                                f"Цена: <code>{current_price:.6g}</code> | "
                                f"Swing Low: +{sw_low_pct:.1f}%\n"
                                f"Сигналы: {', '.join(a_details)}\n"
                                f"───────────────────\n"
                                f"🌊 Wyckoff: <b>{wyckoff_label}</b>\n"
                                f"{ssma_label}\n"
                                f"{ew['alert_block'] + chr(10) if ew['structure'] != 'neutral' else ''}"
                                f"{ew_big_label + chr(10) if ew_big_label else ''}"
                                f"{sq_label_4h + chr(10) if is_sq_4h else ''}"
                                f"───────────────────\n"
                                f"📊 RSI: {rsi:.1f} | MFI: {mfi:.1f}\n"
                                f"{vol_detail}\n"
                                f"{cvd_emoji} CVD: <b>{cvd_level}</b>\n"
                                f"{oi_label}\n"
                                f"───────────────────\n"
                                f"🎯 Цель: <code>{target_l:.6g}</code> "
                                f"(+{tgt_pct_a:.1f}%) | R/R {rr_a:.1f}\n"
                                f"🛑 Стоп: <code>{stop_l:.6g}</code> "
                                f"(-{stp_pct_a:.1f}%)\n"
                                f"───────────────────\n"
                                f"👑 BTC: {ctx['btc_trend']} {ctx['btc_ch']:.1f}%\n"
                                f"{build_tv_link(symbol)}\n{build_coinglass_link(symbol)}"
                            )
                            if send_msg(msg):
                                sent_attention[la_key] = time.time()
                                bot_status["attention_sent"] += 1
                                logging.info(f"ВНИМАНИЕ ЛОНГ: {symbol} score={a_score} sw_low={sw_low_pct:.1f}%")

                    if (sa_key not in sent_attention
                            and near_sw_high
                            and short_gate
                            and vol_24h >= MIN_VOLUME_ATTENTION):

                        a_score, a_details = calc_score('short')
                        rr_ok_a, tgt_pct_a, stp_pct_a, rr_a = check_rr(
                            current_price, target_s, stop_s, 'short',
                            MIN_TARGET_PCT_ATTENTION, MAX_STOP_PCT)

                        if a_score >= SCORE_ATTENTION and rr_ok_a:
                            msg = (
                                f"🔔 <b>ВНИМАНИЕ ШОРТ 4H ({a_score}/10){wl_badge}</b>\n"
                                f"Монета: <b>{symbol}</b>\n"
                                f"Цена: <code>{current_price:.6g}</code> | "
                                f"Swing High: -{sw_high_pct:.1f}%\n"
                                f"Сигналы: {', '.join(a_details)}\n"
                                f"───────────────────\n"
                                f"🌊 Wyckoff: <b>{wyckoff_label}</b>\n"
                                f"{ssma_label}\n"
                                f"{ew['alert_block'] + chr(10) if ew['structure'] != 'neutral' else ''}"
                                f"{ew_big_label + chr(10) if ew_big_label else ''}"
                                f"───────────────────\n"
                                f"📊 RSI: {rsi:.1f} | MFI: {mfi:.1f}\n"
                                f"{vol_detail}\n"
                                f"{cvd_emoji} CVD: <b>{cvd_level}</b>\n"
                                f"{oi_label}\n"
                                f"───────────────────\n"
                                f"🎯 Цель: <code>{target_s:.6g}</code> "
                                f"(-{tgt_pct_a:.1f}%) | R/R {rr_a:.1f}\n"
                                f"🛑 Стоп: <code>{stop_s:.6g}</code> "
                                f"(+{stp_pct_a:.1f}%)\n"
                                f"───────────────────\n"
                                f"👑 BTC: {ctx['btc_trend']} {ctx['btc_ch']:.1f}%\n"
                                f"{build_tv_link(symbol)}\n{build_coinglass_link(symbol)}"
                            )
                            if send_msg(msg):
                                sent_attention[sa_key] = time.time()
                                bot_status["attention_sent"] += 1
                                logging.info(f"ВНИМАНИЕ ШОРТ: {symbol} score={a_score} sw_high={sw_high_pct:.1f}%")

                    # ══════════════════════════════════════════════════
                    # REVERSAL 4H — НОВЫЙ блок (заменяет старые "СИГНАЛ ЛОНГ/ШОРТ 4H")
                    # 5 жёстких слотов: weekly extreme, exhaustion, 1H trigger,
                    # достижимая цель, не ловля ножа. Логика — в reversal_4h.py.
                    # ══════════════════════════════════════════════════
                    need_rev_long  = rev_l_key not in sent_signals
                    need_rev_short = rev_s_key not in sent_signals

                    if (need_rev_long or need_rev_short) and vol_24h >= MIN_VOLUME_SIGNAL:
                        # v7.2: 1H для reversal через safe_api_call (global ban flag)
                        ohlcv_1h_rev = safe_api_call(exchange.fetch_ohlcv,
                                                      symbol, '1h', limit=30)
                        if ohlcv_1h_rev is None:
                            logging.debug(f"1H {symbol} (reversal): None (ban or error)")

                        if ohlcv_1h_rev and len(ohlcv_1h_rev) >= 25:
                            # ─── REVERSAL ЛОНГ ───
                            if need_rev_long:
                                try:
                                    sig = scan_reversal_4h_long(
                                        symbol=symbol,
                                        ohlcv_4h=ohlcv,
                                        ohlcv_1h=ohlcv_1h_rev,
                                        vol_24h=vol_24h,
                                        btc_ch_4h=ctx['btc_ch'],
                                    )
                                except Exception as e:
                                    logging.error(f"reversal_long {symbol}: {e}")
                                    sig = None
                                if sig:
                                    if send_msg(sig['message']):
                                        sent_signals[rev_l_key] = time.time()
                                        bot_status["signals_sent"] += 1
                                        bot_status["reversal_sent"] += 1
                                        logging.info(
                                            f"REVERSAL ЛОНГ: {symbol} entry={sig['entry']:.6g} "
                                            f"tgt={sig['target_pct']:+.2f}% rr={sig['rr']:.2f}"
                                        )

                            # ─── REVERSAL ШОРТ ───
                            if need_rev_short:
                                try:
                                    sig = scan_reversal_4h_short(
                                        symbol=symbol,
                                        ohlcv_4h=ohlcv,
                                        ohlcv_1h=ohlcv_1h_rev,
                                        vol_24h=vol_24h,
                                        btc_ch_4h=ctx['btc_ch'],
                                    )
                                except Exception as e:
                                    logging.error(f"reversal_short {symbol}: {e}")
                                    sig = None
                                if sig:
                                    if send_msg(sig['message']):
                                        sent_signals[rev_s_key] = time.time()
                                        bot_status["signals_sent"] += 1
                                        bot_status["reversal_sent"] += 1
                                        logging.info(
                                            f"REVERSAL ШОРТ: {symbol} entry={sig['entry']:.6g} "
                                            f"tgt={sig['target_pct']:+.2f}% rr={sig['rr']:.2f}"
                                        )

                    time.sleep(0.15)

                except ccxt.RateLimitExceeded:
                    logging.warning(f"Rate limit {symbol}, пауза 30с"); time.sleep(30)
                except ccxt.NetworkError as e:
                    logging.error(f"Network {symbol}: {e}")
                except Exception as e:
                    logging.error(f"Ошибка {symbol}: {e}")

            # ══════════════════════════════════════════════════
            # 2H СКАН
            # ══════════════════════════════════════════════════
            # СЖАТИЕ 2H: ВСЕ активные USDT-perp Binance (~630), динамический список.
            # На Binance минимальный объём ~$1M (даже мелкие монеты ликвидны),
            # делистинги/листинги обрабатываются автоматически.
            # Если завтра добавят/уберут пары — список обновится сам.
            #
            # На MEXC был топ-450 чтобы отрезать "мёртвые" пары с $5-50K объёмом,
            # на Binance такого мусора нет — берём всё.
            all_perps_2h = [x['s'] for x in vol_data]

            for symbol in all_perps_2h:
                try:
                    vol_24h_2h = tickers.get(symbol, {}).get('quoteVolume', 0) or 0

                    # ──────────────────────────────────────────────────
                    # ПРЯМЫЕ 2H СВЕЧИ Binance (без склейки 1H→2H)
                    # На Binance fetch_ohlcv('2h') возвращает нативные 2H свечи,
                    # построенные по UTC календарю. Последняя свеча — формирующаяся.
                    # limit=80 даёт 160 часов = ~6.6 дней истории — достаточно
                    # для ATR Map (baseline 50) и EMA20.
                    # Binance v7.1 фикс: используем helper с retry на 418/429/Network.
                    # ──────────────────────────────────────────────────
                    ohlcv_2h = fetch_ohlcv_with_retry(symbol, '2h', limit=80, max_retries=2)
                    if ohlcv_2h is None or len(ohlcv_2h) < 40:
                        continue

                    closed_2h   = ohlcv_2h[:-1]
                    closes_2h   = [x[4] for x in closed_2h]
                    current_2h  = ohlcv_2h[-1][4]

                    last_ts_2h  = closed_2h[-1][0]
                    cid_2h      = last_ts_2h // 7_200_000

                    ib2_key_l = f"{symbol}_{cid_2h}_2h_ibl"
                    ib2_key_s = f"{symbol}_{cid_2h}_2h_ibs"
                    sq2_key   = f"{symbol}_{cid_2h}_2h_sq"

                    if (ib2_key_l in sent_attention
                            and ib2_key_s in sent_attention
                            and sq2_key in sent_attention):
                        continue

                    rsi_2h    = calculate_rsi_wilder(closes_2h)
                    v_hist_2h = [x[5] for x in closed_2h[-21:-1]]
                    v_avg_2h  = np.mean(v_hist_2h) if v_hist_2h else 1.0

                    cur_2h         = ohlcv_2h[-1]
                    ib_high_2h     = cur_2h[2]
                    ib_low_2h      = cur_2h[3]
                    ib_bounce_2h   = (current_2h - ib_low_2h)  / ib_low_2h  * 100 if ib_low_2h  > 0 else 0.0
                    ib_pullback_2h = (ib_high_2h - current_2h) / ib_high_2h * 100 if ib_high_2h > 0 else 0.0
                    cur_vol_2h     = cur_2h[5] / v_avg_2h if v_avg_2h > 0 else 1.0

                    ssma_2h, ssma_trend_2h, ssma_slope_2h = calculate_ssma(closed_2h, period=24)
                    long_gate_2h  = ssma_allows_long(ssma_2h, ssma_trend_2h, ssma_slope_2h, current_2h)
                    short_gate_2h = ssma_allows_short(ssma_2h, ssma_trend_2h, ssma_slope_2h, current_2h)

                    # ── ИЗМЕНЕНИЕ 3: swing_bars=5 вместо 10 ──────────────────
                    # Было: swing_bars=10 → окно 20 свечей = 40 часов
                    # Стало: swing_bars=5 → окно 10 свечей = 20 часов
                    # Уровни более локальные → near_sl/sh срабатывает раньше
                    # → ранний лонг/шорт приходит до движения, не в момент
                    sw_lp_2h, sw_hp_2h, near_sl_2h, near_sh_2h, sw_l_2h, sw_h_2h = \
                        calculate_swing_hilo(ohlcv_2h, swing_bars=SWING_BARS_2H)

                    cvd_level_2h, _ = calc_cvd_level(closed_2h)
                    cvd_emoji_2h = {"bull":"🟢","bull_div":"🟢✨",
                                    "bear":"🔴","bear_div":"🔴✨"}.get(cvd_level_2h, "⚪️")

                    is_sq, sq_ratio, sq_slope, sq_bars, sq_label = \
                        detect_volatility_squeeze(closed_2h, period=3, avg_period=50)

                    atr_map_score, atr_map_forming, atr_map_mature, atr_map_label, atr_map_comp = \
                        detect_atr_map_squeeze(closed_2h)

                    atr_map_active = atr_map_forming

                    cur_vol_2h_rel = (ohlcv_2h[-1][5] /
                        (sum(x[5] for x in closed_2h[-20:]) / 20)
                        if closed_2h else 1.0)

                    if len(closed_2h) >= 3:
                        price_3back = closed_2h[-3][4]
                        recent_move_pct = abs(current_2h - price_3back) / price_3back * 100
                    else:
                        recent_move_pct = 0.0
                    already_moving = recent_move_pct > 3.0

                    is_sq_clean = is_sq and not already_moving and cur_vol_2h_rel < 1.5

                    # Исключение SSMA ворот
                    if (not long_gate_2h and is_sq
                            and cvd_level_2h in ('bull', 'bull_div')
                            and ib_bounce_2h >= 1.5):
                        long_gate_2h = True
                    if (not short_gate_2h and is_sq
                            and cvd_level_2h in ('bear', 'bear_div')
                            and ib_pullback_2h >= 1.5):
                        short_gate_2h = True

                    wl_2h = " ⭐️" if symbol in WATCHLIST else ""
                    ssma_lbl_2h = ""
                    if ssma_2h:
                        icon = "📈" if 'bull' in ssma_trend_2h else "📉"
                        ssma_lbl_2h = f"{icon} SSMA 2H: {ssma_2h:.4g} ({ssma_slope_2h:+.2f}%/св)"

                    tv = build_tv_link(symbol)
                    cg = build_coinglass_link(symbol)
                    btc_line = f"👑 BTC: {ctx['btc_trend']} {ctx['btc_ch']:.1f}%"
                    vol_line = f"📦 Объём 24H: ${vol_24h_2h/1_000_000:.1f}M"

                    # ══════════════════════════════════════════════════
                    # СЖАТИЕ 2H — НОВЫЙ ФОРМАТ для Binance v7.0
                    # ══════════════════════════════════════════════════
                    # Триггеры (любой):
                    #   is_sq → короткое окно (3 свечи)
                    #   atr_map_forming (score≥60) → длинное окно (~50 свечей)
                    #
                    # Новое:
                    #   • EMA20 вместо SSMA для направления (быстрее реагирует)
                    #   • Скоринг направления по 6 факторам:
                    #       EMA, CVD, Range position, BTC 4h, Funding, OI 24h
                    #     (все +2/+1/-1/-2, итоговая дельта определяет направление)
                    #   • Объёмный блок одной строкой: V24h | Avg7d | множитель с эмодзи
                    #   • Funding rate из Binance (один запрос на всех)
                    #   • OI динамика 24h из памяти (24h history)
                    #   • Mature Xb — сколько баров подряд держится score≥60
                    #   • Тип паттерна (двойное подтверждение / классическое / откат)
                    #
                    # Все алерты собираются в compression_candidates,
                    # потом сортируются по ATR Map score DESC и отправляются.
                    # ══════════════════════════════════════════════════

                    # ---- запись текущего score в историю для подсчёта mature_bars
                    # Историю ведём на каждой итерации, а не только когда уже active —
                    # так точнее, прерывание восстановит счётчик
                    if symbol not in atr_map_score_history:
                        atr_map_score_history[symbol] = deque(maxlen=12)
                    atr_map_score_history[symbol].append(atr_map_score)
                    mature_bars = calc_mature_bars(list(atr_map_score_history[symbol]),
                                                    threshold=60)

                    # ---- OI ВРЕМЕННО ОТКЛЮЧЁН (Binance v7.1)
                    # Проблема: fetch_tickers НЕ возвращает openInterest на Binance Futures.
                    # OI доступен только через отдельный endpoint /fapi/v1/openInterest
                    # или historical /futures/data/openInterestHist.
                    # Решение откладывается: вернём OI отдельным модулем для топ-50 монет,
                    # когда стабилизируем основную работу бота. Сейчас скоринг работает
                    # на 5 факторах (EMA, CVD, Range, BTC, Funding) — этого достаточно.
                    # Функции record_oi_snapshot / get_oi_24h_change / get_oi_from_ticker
                    # оставлены в коде (выше) — будут переиспользованы.
                    oi_now_notional = 0.0
                    oi_24h_change   = None  # передаём None в скоринг → фактор OI пропускается

                    # Детальный лог
                    if is_sq or atr_map_active:
                        already_sent = sq2_key in sent_attention
                        vol_filter_pass = (cur_vol_2h < 1.5 or atr_map_active)
                        will_send = (not already_sent and vol_filter_pass)
                        logging.info(
                            f"СЖАТИЕ-DEBUG {symbol}: "
                            f"is_sq={is_sq} atr_map={atr_map_score:.0f}({'+' if atr_map_active else '-'}) "
                            f"mature_bars={mature_bars} "
                            f"cur_vol={cur_vol_2h:.2f} v24h={vol_24h_2h/1e6:.2f}M "
                            f"already_sent={already_sent} vol_pass={vol_filter_pass} "
                            f"→ {'QUEUE' if will_send else 'SKIP'}"
                        )

                    # Условие добавления в очередь — как раньше
                    if (sq2_key not in sent_attention
                            and (is_sq or atr_map_active)
                            and (cur_vol_2h < 1.5 or atr_map_active)):

                        # ── EMA20 (заменяет SSMA в блоке СЖАТИЕ)
                        ema_val, ema_trend, ema_slope = calculate_ema(closed_2h, period=20)

                        # ── Положение цены в диапазоне (последние 20 закрытых баров)
                        if len(closed_2h) >= 20:
                            highs_20 = [c[2] for c in closed_2h[-20:]]
                            lows_20  = [c[3] for c in closed_2h[-20:]]
                        else:
                            highs_20 = [c[2] for c in closed_2h]
                            lows_20  = [c[3] for c in closed_2h]
                        range_pos = get_range_position(highs_20, lows_20, current_2h)

                        # ── Funding rate (по символу из общего словаря)
                        fr = funding_rates.get(symbol, 0.0)

                        # ── BTC 4h контекст уже в ctx
                        btc_4h_change = ctx.get('btc_ch', 0.0)

                        # ── 7-дневный средний объём
                        # 7d = 84 закрытых 2H свечи. У нас обычно есть ~80 — берём что есть.
                        vol_bars_2h = [c[5] * c[4] for c in closed_2h[-84:]]
                        v_avg_7d_per_2h = (sum(vol_bars_2h) / len(vol_bars_2h)
                                            if vol_bars_2h else 0.0)
                        v_avg_7d_24h = v_avg_7d_per_2h * 12  # 12 свечей по 2H = 24h

                        # ── Скоринг направления
                        direction, dir_label, score_up, score_down, breakdown = \
                            score_breakout_direction(
                                ema_trend=ema_trend,
                                cvd_state=cvd_level_2h,
                                range_position=range_pos,
                                btc_4h_change=btc_4h_change,
                                funding_rate=fr,
                                oi_24h_change=oi_24h_change,
                                current_price=current_2h,
                                ema_value=ema_val or 0
                            )

                        # ── Текущий объём 2H свечи (relative) для строки
                        v_avg_recent_2h = (sum(c[5] for c in closed_2h[-20:]) / 20
                                            if len(closed_2h) >= 20 else 1.0)
                        cur_vol_2h_x = (ohlcv_2h[-1][5] / v_avg_recent_2h
                                        if v_avg_recent_2h > 0 else 1.0)

                        # ── Финальный приоритет для сортировки
                        # Используем atr_map_score (или 0 если только is_sq)
                        sort_score = atr_map_score if atr_map_active else 0

                        # Собираем кандидата
                        compression_candidates.append({
                            'symbol':            symbol,
                            'sq2_key':           sq2_key,
                            'current_2h':        current_2h,
                            'sort_score':        sort_score,
                            'is_sq':             is_sq,
                            'is_sq_clean':       is_sq_clean,
                            'sq_label':          sq_label,
                            'sq_ratio':          sq_ratio,
                            'atr_map_active':    atr_map_active,
                            'atr_map_score':     atr_map_score,
                            'atr_map_mature':    atr_map_mature,
                            'atr_map_comp':      atr_map_comp,
                            'mature_bars':       mature_bars,
                            'ema_val':           ema_val,
                            'ema_trend':         ema_trend,
                            'ema_slope':         ema_slope,
                            'cvd_level':         cvd_level_2h,
                            'cvd_emoji':         cvd_emoji_2h,
                            'rsi_2h':            rsi_2h,
                            'cur_vol_2h_x':      cur_vol_2h_x,
                            'vol_24h':           vol_24h_2h,
                            'v_avg_7d_24h':      v_avg_7d_24h,
                            'funding_rate':      fr,
                            'oi_now':            oi_now_notional,
                            'oi_24h_change':     oi_24h_change,
                            'direction':         direction,
                            'dir_label':         dir_label,
                            'score_up':          score_up,
                            'score_down':        score_down,
                            'breakdown':         breakdown,
                            'btc_line':          btc_line,
                            'tv':                tv,
                            'cg':                cg,
                            'wl_2h':             wl_2h,
                        })

                    # ══════════════════════════════════════════════════
                    # РАННИЙ ЛОНГ 2H
                    # ИЗМЕНЕНИЕ 3: swing_bars=5 → near_sl_2h срабатывает
                    # раньше, на более локальных уровнях
                    # Объём остаётся MIN_VOLUME_ATTENTION = $1M
                    #
                    # ФИЛЬТР RSI: если RSI 2H > 65 — перекуплено, лонг опасен.
                    # 3-bar move фильтр НЕ применяется: разворот от капитуляционного
                    # лоя даёт большой move (+7-10%), но это валидный сигнал.
                    # ══════════════════════════════════════════════════
                    if (ib2_key_l not in sent_attention
                            and ib_bounce_2h >= 1.0
                            and cur_vol_2h >= 1.2
                            and long_gate_2h
                            and near_sl_2h
                            and vol_24h_2h >= MIN_VOLUME_ATTENTION
                            and rsi_2h <= 65):
                        parts = [
                            f"⚡️ <b>РАННИЙ ЛОНГ 2H{wl_2h}</b>",
                            f"Монета: <b>{symbol}</b> | 🕯 Внутри 2H свечи",
                            f"Цена: <code>{current_2h:.6g}</code>",
                            f"Отскок от лоя: +{ib_bounce_2h:.1f}% | Объём: x{cur_vol_2h:.1f}",
                            f"Swing Low 2H: +{sw_lp_2h:.1f}%",
                            "───────────────────",
                            ssma_lbl_2h,
                            f"{cvd_emoji_2h} CVD 2H: <b>{cvd_level_2h}</b>",
                            f"📊 RSI 2H: {rsi_2h:.1f}",
                            vol_line,
                        ]
                        if is_sq:
                            parts.append(sq_label)
                        parts += [
                            "───────────────────",
                            "⚠️ Свеча 2H не закрыта — ждите подтверждения",
                            btc_line, tv,
                            cg
                        ]
                        if send_msg("\n".join(parts)):
                            sent_attention[ib2_key_l] = time.time()
                            bot_status["early_2h_sent"] += 1
                            logging.info(f"РАННИЙ ЛОНГ 2H: {symbol} bounce={ib_bounce_2h:.1f}% swing_bars={SWING_BARS_2H}")

                    # ══════════════════════════════════════════════════
                    # РАННИЙ ШОРТ 2H
                    #
                    # ФИЛЬТР RSI: если RSI 2H < 35 — перепродано, шорт опасен.
                    # 3-bar move фильтр НЕ применяется: разворот от пика после
                    # сильного памп-движения даёт большой move (-7-10%), но это
                    # валидный сигнал на шорт.
                    # ══════════════════════════════════════════════════
                    if (ib2_key_s not in sent_attention
                            and ib_pullback_2h >= 1.0
                            and cur_vol_2h >= 1.2
                            and short_gate_2h
                            and near_sh_2h
                            and vol_24h_2h >= MIN_VOLUME_ATTENTION
                            and rsi_2h >= 35):
                        parts = [
                            f"⚡️ <b>РАННИЙ ШОРТ 2H{wl_2h}</b>",
                            f"Монета: <b>{symbol}</b> | 🕯 Внутри 2H свечи",
                            f"Цена: <code>{current_2h:.6g}</code>",
                            f"Откат от хая: -{ib_pullback_2h:.1f}% | Объём: x{cur_vol_2h:.1f}",
                            f"Swing High 2H: -{sw_hp_2h:.1f}%",
                            "───────────────────",
                            ssma_lbl_2h,
                            f"{cvd_emoji_2h} CVD 2H: <b>{cvd_level_2h}</b>",
                            f"📊 RSI 2H: {rsi_2h:.1f}",
                            vol_line,
                        ]
                        if is_sq:
                            parts.append(sq_label)
                        parts += [
                            "───────────────────",
                            "⚠️ Свеча 2H не закрыта — ждите подтверждения",
                            btc_line, tv,
                            cg
                        ]
                        if send_msg("\n".join(parts)):
                            sent_attention[ib2_key_s] = time.time()
                            bot_status["early_2h_sent"] += 1
                            logging.info(f"РАННИЙ ШОРТ 2H: {symbol} pullback={ib_pullback_2h:.1f}% swing_bars={SWING_BARS_2H}")

                except ccxt.RateLimitExceeded:
                    logging.warning(f"Rate limit 2H {symbol}, пауза 30с"); time.sleep(30)
                except ccxt.NetworkError as e:
                    logging.error(f"Network 2H {symbol}: {e}")
                except Exception as e:
                    logging.error(f"Ошибка 2H {symbol}: {e}")

            # ══════════════════════════════════════════════════
            # ОТПРАВКА АЛЕРТОВ СЖАТИЕ
            # Сортируем по ATR Map score DESC — сначала зрелые,
            # потом активные, потом is_sq без ATR Map.
            # Защита от Telegram 429 уже в send_msg (retry с retry_after).
            # ══════════════════════════════════════════════════
            compression_candidates.sort(key=lambda x: x['sort_score'], reverse=True)
            logging.info(f"СЖАТИЕ-OUT: кандидатов на отправку = {len(compression_candidates)}")

            for cand in compression_candidates:
                # Двойная проверка дедупликации (если за итерацию пришёл дубль)
                if cand['sq2_key'] in sent_attention:
                    continue

                # ── Строим блок ATR Map / Короткое
                sq_block_lines = []
                if cand['atr_map_active']:
                    maturity = "🟠 ЗРЕЛОЕ" if cand['atr_map_mature'] else "🟡 АКТИВНОЕ"
                    sq_block_lines.append(
                        f"📊 ATR Map: {cand['atr_map_score']:.0f}/100 {maturity} "
                        f"({pluralize_bars(cand['mature_bars'])})\n"
                        f"   ATR x{cand['atr_map_comp']['atr_ratio']:.2f} | "
                        f"Range {cand['atr_map_comp']['range']:.0f} | "
                        f"Noise {cand['atr_map_comp']['noise']:.0f} | "
                        f"Cont {cand['atr_map_comp']['containment']:.0f}"
                    )
                else:
                    # is_sq без ATR Map — показываем что ATR Map ниже порога
                    sq_block_lines.append(
                        f"📊 ATR Map: {cand['atr_map_score']:.0f}/100 ⚪ ниже порога"
                    )
                if cand['is_sq_clean']:
                    sq_block_lines.append(cand['sq_label'])

                # ── Тип паттерна
                pattern_line = get_pattern_type(cand['atr_map_active'], cand['is_sq_clean'])

                # ── Направление взрыва
                breakdown_line = format_breakdown(cand['breakdown'],
                                                   cand['score_up'],
                                                   cand['score_down'])

                # ── EMA блок
                if cand['ema_val']:
                    ema_icon = "📈" if 'bull' in cand['ema_trend'] else (
                                "📉" if 'bear' in cand['ema_trend'] else "➡️")
                    ema_line = (f"{ema_icon} EMA 2H: {cand['ema_val']:.4g} "
                                f"({cand['ema_slope']:+.2f}%/св)")
                else:
                    ema_line = "EMA 2H: —"

                # ── CVD
                cvd_line = f"{cand['cvd_emoji']} CVD 2H: <b>{cand['cvd_level']}</b>"

                # ── RSI
                rsi_line = f"📊 RSI 2H: {cand['rsi_2h']:.1f}"

                # ── Объём текущей 2H
                # ── Vol текущей 2H с эмодзи (Binance v7.1)
                # 🔇 <0.30x — полное затишье (идеал для входа после сжатия)
                # ⚪ 0.30-1.50x — норма
                # ⚡ 1.50-2.00x — просыпается
                # 🚨 >2.00x — взрыв уже идёт (возможно поздно входить)
                cv = cand['cur_vol_2h_x']
                if cv < 0.30:
                    cv_emoji = "🔇"
                elif cv < 1.50:
                    cv_emoji = "⚪"
                elif cv < 2.00:
                    cv_emoji = "⚡"
                else:
                    cv_emoji = "🚨"
                cur_vol_line = f"{cv_emoji} Vol текущей 2H: {cv:.2f}x"

                # ── Объёмный блок
                vol_block_line = format_volume_block(cand['vol_24h'], cand['v_avg_7d_24h'])

                # ── Funding
                fund_pct = cand['funding_rate'] * 100  # rate (0.0001) → проценты (0.01%)
                fund_line = f"💸 Funding: {fund_pct:+.3f}%"

                # OI временно отключён (Binance v7.1) — вернём отдельным модулем для топ-50

                # ── Собираем сообщение
                msg_parts = [
                    f"🗜 <b>СЖАТИЕ 2H{cand['wl_2h']}</b>",
                    f"Монета: <b>{cand['symbol']}</b>",
                    f"Цена: <code>{cand['current_2h']:.6g}</code>",
                    "━━━━━━━━━━━━━━━━",
                    *sq_block_lines,
                ]
                if pattern_line:
                    msg_parts.append(pattern_line)
                msg_parts += [
                    "",
                    f"⚡️ Вероятный взрыв: {cand['dir_label']}",
                    f"   {breakdown_line}",
                    "━━━━━━━━━━━━━━━━",
                    ema_line,
                    cvd_line,
                    rsi_line,
                    cur_vol_line,
                    "",
                    vol_block_line,
                    fund_line,
                    "",
                    cand['btc_line'],
                    cand['tv'],
                    cand['cg'],
                ]
                msg = "\n".join(msg_parts)

                if send_msg(msg):
                    sent_attention[cand['sq2_key']] = time.time()
                    bot_status["early_2h_sent"] += 1
                    logging.info(
                        f"СЖАТИЕ 2H SENT: {cand['symbol']} "
                        f"atr_map={cand['atr_map_score']:.0f} mature_b={cand['mature_bars']} "
                        f"dir={cand['direction']} {cand['score_up']}vs{cand['score_down']}"
                    )

            now = time.time()
            sent_signals   = {k: v for k, v in sent_signals.items()   if now - v < 86400}
            sent_attention = {k: v for k, v in sent_attention.items() if now - v < 86400}
            bot_status["iterations"]    += 1
            bot_status["last_iteration"] = datetime.now().strftime('%H:%M:%S')
            logging.info(f"Итерация. Символов 4H: {len(symbols)} | 2H: {len(all_perps_2h)} | "
                         f"Reversal: {bot_status['reversal_sent']} | "
                         f"Внимание: {bot_status['attention_sent']} | "
                         f"Ранних 2H: {bot_status['early_2h_sent']} | "
                         f"BTC vol: {ctx['btc_vol']:.2f}%")
            time.sleep(60)  # Binance v7.1: пауза 60с (итерация ~6-7 мин)

        except ccxt.NetworkError as e:
            logging.error(f"Глобальная сеть: {e}"); bot_status["errors"] += 1; time.sleep(60)
        except Exception as e:
            logging.error(f"Критическая ошибка: {e}"); bot_status["errors"] += 1; time.sleep(60)


@app.route('/health')
def health():
    uptime = str(datetime.now() - datetime.fromisoformat(bot_status["started_at"])).split('.')[0]
    # v7.2: показываем состояние circuit breaker и weight
    now = time.time()
    if IP_BAN_UNTIL > now:
        ban_str = f"🚫 BAN до {datetime.fromtimestamp(IP_BAN_UNTIL).strftime('%H:%M:%S')} (ещё {IP_BAN_UNTIL - now:.0f}с)"
    else:
        ban_str = "✅ нет"
    return (f"✅ OK | Binance v7.2\n"
            f"Uptime: {uptime}\n"
            f"Итераций: {bot_status['iterations']}\n"
            f"Ошибок: {bot_status['errors']}\n"
            f"IP бан: {ban_str}\n"
            f"X-MBX-USED-WEIGHT-1m: {USED_WEIGHT_1M} / {WEIGHT_LIMIT_BINANCE}\n"
            f"Reversal 4H 🚨: {bot_status['reversal_sent']}\n"
            f"Внимание 🔔: {bot_status['attention_sent']}\n"
            f"Ранних 2H: {bot_status['early_2h_sent']}\n"
            f"Последняя итерация: {bot_status['last_iteration']}")


def keepalive_loop():
    time.sleep(30)
    port         = int(os.environ.get("PORT", 10000))
    external_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    local_url    = f"http://localhost:{port}/health"
    while True:
        for url in ([f"{external_url}/health"] if external_url else []) + [local_url]:
            try:
                r = requests.get(url, timeout=15)
                logging.info(f"Keepalive [{url}]: {r.status_code}"); break
            except Exception as e:
                logging.warning(f"Keepalive [{url}]: {e}")
        time.sleep(240)


def watchdog():
    time.sleep(60)
    while True:
        global analyst_thread
        if not analyst_thread.is_alive():
            logging.error("analyst_loop упал, перезапуск...")
            bot_status["errors"] += 1
            analyst_thread = threading.Thread(target=analyst_loop, daemon=True, name="analyst")
            analyst_thread.start()
        time.sleep(60)


analyst_thread = threading.Thread(target=analyst_loop, daemon=True, name="analyst")
analyst_thread.start()
threading.Thread(target=keepalive_loop, daemon=True, name="keepalive").start()
threading.Thread(target=watchdog,       daemon=True, name="watchdog").start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
