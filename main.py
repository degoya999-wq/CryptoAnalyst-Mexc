import ccxt
import ccxt.pro as ccxtpro
import asyncio
import requests
import time
import os
import logging
import gc
import resource
from datetime import datetime
from flask import Flask
import threading
import numpy as np
import re as _re
from collections import deque


# ═══════════════════════════════════════════════════════════════════════
# MEMORY DIAGNOSTICS (добавлено в v7.3.5, актуально в v7.3.7)
# Render Free даёт 512 МБ. Логируем RSS в ключевых точках,
# чтобы понять где именно превышается лимит.
# resource.getrusage даёт ru_maxrss в КБ на Linux (на macOS в байтах,
# но мы на Render = Linux, так что просто /1024 для МБ).
# ═══════════════════════════════════════════════════════════════════════
def _log_memory(tag: str):
    """Логирует текущее потребление памяти процессом.
    tag — короткая метка точки в коде."""
    try:
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_mb = rss_kb / 1024
        # /proc/self/status даёт текущий RSS (не peak) — полезнее для понимания
        # реальной картины. peak = ru_maxrss, current = VmRSS из /proc/.
        current_rss_mb = None
        try:
            with open('/proc/self/status', 'r') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        current_rss_mb = int(line.split()[1]) / 1024
                        break
        except Exception:
            pass
        if current_rss_mb is not None:
            logging.info(
                f"🧠 MEM[{tag}]: current={current_rss_mb:.1f}MB, peak={rss_mb:.1f}MB "
                f"(limit=512MB, free={512 - current_rss_mb:.1f}MB)"
            )
        else:
            logging.info(f"🧠 MEM[{tag}]: peak={rss_mb:.1f}MB (limit=512MB)")
    except Exception as e:
        logging.warning(f"🧠 MEM[{tag}]: не смог измерить — {e}")


# ═══════════════════════════════════════════════════════════════════════
# v7.3.9.1: MALLOC_TRIM — возврат памяти из glibc-пулов обратно в ОС.
# Возвращён из v7.3.8.1 (в v7.3.9 был утерян — оставался только gc.collect,
# который чистит Python-объекты, но НЕ отдаёт RSS операционной системе).
# На Render 512МБ это критично: gc.collect освобождает объекты в арены glibc,
# но RSS, который видит Render, остаётся высоким → OOM-рестарт каждые ~2 часа.
# malloc_trim(0) форсит glibc вернуть свободные арены в ОС → RSS реально падает.
# Проверено: на glibc Linux (Render = Linux) malloc_trim(0) возвращает 1.
# При недоступности (не-glibc, напр. musl/Alpine) — безопасный no-op.
# ═══════════════════════════════════════════════════════════════════════
import ctypes as _ctypes
import ctypes.util as _ctypes_util

try:
    _libc = _ctypes.CDLL(_ctypes_util.find_library("c"))
    _libc.malloc_trim.argtypes = [_ctypes.c_size_t]
    _libc.malloc_trim.restype = _ctypes.c_int

    def _malloc_trim():
        """Возвращает свободную память glibc-аллокатора обратно в ОС.
        Вызывать после gc.collect() в конце итерации analyst_loop."""
        try:
            return _libc.malloc_trim(0)
        except Exception:
            return -1
    logging.info("✅ v7.3.9.1: malloc_trim доступен (glibc) — память будет возвращаться в ОС")
except Exception as _e:
    def _malloc_trim():
        """Заглушка для сред без glibc (musl/Alpine и пр.)."""
        return -1
    logging.warning(f"⚠️ v7.3.9.1: malloc_trim недоступен ({_e}) — RSS-trim работать не будет")

# ─────────────────────────────────────────────
# Binance v7.3.7: WS-TICKERS вместо REST fetch_tickers (главный фикс банов)
# - Проблема: shared IP Render Free попадает в watchlist Binance после первого
#   бана. Дальше любой weight-расход = моментальный бан (даже у соседей).
# - Главный источник нашего weight: fetch_tickers (weight=40) каждые ~8 минут
#   из analyst_loop = 280+ weight/час. Плюс fetch_tickers в refresh_active_symbols
#   раз в час. Итого мы — заметный пользователь на shared IP.
# - Фикс v7.3.7: подписываемся на WebSocket-стрим !ticker@arr через ccxt.pro
#   watch_tickers() — это 0 REST weight, обновления всех ~579 пар каждую секунду.
#   Binance в 418-ответах прямо рекомендует: "use websocket for live updates
#   to avoid bans".
# - Что меняется:
#   1. ws_tickers_cache — глобальный dict с тикерами, формат совместим с
#      output fetch_tickers (поле quoteVolume и др.).
#   2. ws_tickers_exchange — ОТДЕЛЬНЫЙ ccxt.pro инстанс (не из ws_exchanges[tf]).
#      Markets шарятся через set_markets() из v7.3.6 — нулевой оверхед памяти.
#   3. _ws_watch_tickers() — корутина, четвёртая параллельная задача в _ws_main.
#   4. _get_tickers_ws_or_rest() — единая точка получения тикеров:
#      WS-кэш если свежий → fallback REST если нет (первые секунды после старта).
#   5. analyst_loop и refresh_active_symbols используют новый хелпер.
# - Что НЕ меняется: бизнес-логика (СЖАТИЕ, is_sq, ATR Map, ранние 2H,
#   ВНИМАНИЕ 4H, REVERSAL 4H, 1H триггеры), funding_rates (оставлены, weight=5),
#   все фиксы v7.3.6 (set_markets, OHLCVLimit), v7.3.5 (memory diag), v7.3.4.
# - Ожидаемый эффект: weight/час падает с 280+ до ~40 (только funding).
#   Бот перестаёт быть заметным потребителем — баны не должны прилетать,
#   даже от соседского трафика на shared IP.
#
# Binance v7.3.6: SHARE MARKETS между exchange'ами (экономия ~90MB)
# - Диагностика v7.3.5 показала: warmup растёт линейно (~3MB на 50 монет, итого
#   ~30MB на 579 монет). Это здоровое поведение. НО после ws_main_start RSS
#   подскочил на +167MB (308→476MB) за одну минуту — это и есть причина OOM.
# - Причина скачка: 3 ws_exchanges (ccxt.pro) при старте каждый делает свой
#   load_markets() и держит свою копию рынков Binance Futures (~30MB × 3).
#   Плюс REST exchange — итого 4 копии одного и того же словаря.
# - Фикс: после _safe_load_markets() в startup_sequence вызываем
#   ws_ex.set_markets(shared_markets, shared_currencies) для каждого из трёх
#   ws_exchanges. Они используют ОДИН dict-объект (не копию), не делают свой
#   load_markets() при первом обращении. Экономия ожидаемая: ~90MB.
# - set_markets — публичный метод ccxt.Exchange, безопасный. Заполняет
#   markets, markets_by_id, symbols, ids, currencies одним вызовом.
# - При ошибке fallback на старое поведение (ws_exchanges грузят markets сами).
#
# Binance v7.3.5: MEMORY DIAGNOSTICS
# - v7.3.4 пофиксил OOM от ccxt.pro buffer + WS 1008, но OOM 512MB всё равно
#   прилетает во время warmup. Источник раньше был замаскирован WS-утечкой.
# - Этот патч НЕ меняет логику, только добавляет:
#   1. _log_memory(tag) — печатает current/peak RSS в МБ из /proc/self/status
#      и resource.getrusage. Видим точное потребление в реалтайме.
#   2. Замеры в start_bot, warmup_start, каждые 50 монет warmup, warmup_done,
#      after_gc, ws_main_start, watchdog раз в минуту.
#   3. gc.collect() после warmup — освобождает промежуточные JSON-объекты
#      ccxt (1158 REST-ответов могли накопиться без явного gc).
# - По этим логам поймём:
#   * где именно пробивается лимит 512МБ (warmup или WS-инициализация);
#   * есть ли утечка в стабильной фазе (RSS растёт от тика к тику)
#     или это honest usage (стабильное потребление).
#
# Binance v7.3.4: ФИКС OOM 512MB + WebSocket code 1008
# - Баг v7.3.3: при 579 монетах × 3 ТФ все 1737 streams шли через ОДИН ws_exchange,
#   из-за чего Binance слал code 1008 (policy violation, лимит ~200 subs/conn).
#   Плюс ccxt.pro по умолчанию буферит до 1000 свечей на символ — на 579×3 это
#   ~85MB сырых данных + ~3× оверхед Python dict/list → OOM kill на Render (512MB).
# - Фикс:
#   1. ws_exchanges = {tf: ccxtpro.binance(...)} — три отдельных WS-соединения,
#      по одному на 4h/2h/1h. 579/3 ≈ 193 stream на соединение — влезает в лимит.
#   2. options.OHLCVLimit = 100 — режет внутренний буфер ccxt.pro в 10 раз.
#      Главный фикс по памяти.
#   3. asyncio.gather теперь читает results явно — убирает
#      "Future exception was never retrieved" warning + утечку future-объектов.
#   4. start_bot() обёртка для всех фоновых потоков, вызывается из __main__.
#      Чистая структура, идемпотентна (защита от двойного запуска).
#
# Binance v7.3.3: критичный фикс WebSocket bulk subscription
# - Баг v7.3.2: подписка на 579 символов × 3 таймфрейма (1737 streams) одновременно
#   триггерила anti-DDoS Binance → 418 бан 4+ часов на всех WS соединениях.
#   Дальше watcher переподключался каждые 5с и продлевал бан до бесконечности.
# - Фикс:
#   1. Batching: подписка по WS_BATCH_SIZE=50 монет с паузой 1.5с между батчами.
#      Полная подписка одного таймфрейма займёт ~20с вместо мгновенного flood.
#   2. Разнесение по времени старта watcher'ов: 4h сразу, 2h через 30с, 1h через 60с.
#   3. Ban-aware retry: при получении 418 в WS — выставляем глобальный флаг
#      и ВСЕ батчи ждут его снятия (а не повторно подключаются).
#   4. Exponential backoff: при ошибках задержка растёт 5s → 10s → 20s → ... → 300s.
#   5. /health показывает batch_progress.
#
# Binance v7.3.2: критичный фикс _safe_load_markets зависания
# - Баг v7.3.1: load_markets() мог зависать на TCP без exception при бане
#   shared IP. Старый процесс на Render продолжал отвечать на /health
#   из памяти, новый молча висел в load_markets без логов.
# - Фикс: heartbeat-лог на каждой попытке retry (видим что живой).
# - Принудительный timeout=20s на load_markets (избегаем TCP зависания).
# - При длинном бане ждём максимум 60с за раз, потом повторяем — для heartbeat.
#
# Binance v7.3.1: ФИКС startup_sequence для холодного старта в бане
# - Баг v7.3: load_markets() в startup вызывался напрямую без обёртки.
#   При холодном старте если shared IP в 418-бане — DDoSProtection
#   пробрасывалось наружу startup_sequence → warmup НИКОГДА не запускался,
#   бот висел "ждёт прогрев: 0/0" пока бан не снят (часами).
# - Фикс: _safe_load_markets с бесконечными retry на 418/Network.
#   startup_sequence теперь выживает любые баны при старте — ждёт
#   точное время разбана из ответа Binance, потом продолжает прогрев.
# - refresh_active_symbols тоже использует _safe_load_markets.
#
# Binance v7.3: МИГРАЦИЯ НА WEBSOCKET (гибрид REST + WS)
# - Свечи 4H/2H/1H приходят через ccxt.pro websocket (watch_ohlcv_for_symbols).
#   Никаких массовых REST-запросов больше нет.
# - REST остаётся только для:
#     * fetch_tickers (weight 40) — раз в итерацию для vol_24h и активных пар
#     * fetch_funding_rates (weight ~5) — раз в итерацию
#     * Прогрев истории при старте (5-7 минут разовая нагрузка)
# - Общий weight упал с ~2050 до ~45 за итерацию (в 45 раз меньше).
#   Шанс ловить 418 от Binance практически нулевой даже на shared IP.
# - ws_loop запускается в отдельном asyncio-event-loop в потоке.
#   analyst_loop остаётся синхронным, читает из глобальных candles_storage.
# - Раз в час обновляется список активных монет — для подхвата новых листингов.
# - Защита от 418 (circuit breaker из v7.2) ОСТАВЛЕНА для REST-вызовов
#   tickers/funding, на случай если shared-IP всё-таки получит чужой бан.
# - Логика ATR Map / is_sq / scoring / порогов / сообщений — НЕ ИЗМЕНЕНА.
#
# Binance v7.2: глобальный circuit breaker IP_BAN_UNTIL для REST-банов 418
# Binance v7.1: rateLimit 200мс, limit 100, helper fetch_ohlcv_with_retry
# Binance v7.0: миграция с MEXC, блок СЖАТИЕ с EMA + скоринг направления
# v6.2 (MEXC): REVERSAL_4H + ATR Map mature/forming
# ─────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════
# v7.3.9.2: модуль REVERSAL_4H УДАЛЁН целиком (работает на MEXC-боте).
# Все REV_* константы и _rev_*/scan_reversal_4h_* функции убраны.
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
    'rateLimit': 280,                       # 280мс (Binance v7.3 — для REST оставшихся вызовов)
                                             # REST теперь только fetch_tickers + fetch_funding_rates
                                             # один раз в итерацию. Свечи идут через WS.
    'timeout': 15000,
    'options': {
        'defaultType': 'future',         # USDM Futures
        'adjustForTimeDifference': True,
    }
})

# WebSocket exchanges (ccxt.pro) — v7.3.4: ОТДЕЛЬНОЕ соединение на каждый таймфрейм.
# Причина: при 579 монетах × 3 ТФ = 1737 streams на одно соединение Binance шлёт
# код 1008 (policy violation) — лимит ~200 subscriptions per connection.
# Разнесение по трём WS-соединениям убирает 1008 + равномернее распределяет память.
#
# OHLCVLimit=100 — КРИТИЧНО для памяти на Render (512MB лимит):
# по умолчанию ccxt.pro хранит до 1000 свечей × N символов в self.ohlcvs.
# 1000×579×3 = ~1.7M свечей × ~50 байт = ~85MB только ccxt-буфер + python overhead.
# С limit=100 буфер уменьшается в 10 раз и OOM на 512MB больше не должен прилетать.
#
# enableRateLimit для WS не нужен — там лимиты по соединениям, не по weight.
ws_exchanges = {
    tf: ccxtpro.binance({
        'enableRateLimit': False,
        'options': {
            'defaultType': 'future',
            'OHLCVLimit':  100,    # ограничиваем внутренний буфер свечей ccxt.pro
            'tradesLimit': 100,    # на случай если где-то trades подпишутся
        }
    })
    for tf in ('2h',)   # v7.3.9.3: 4h и 1h убраны — одно WS-соединение на свечи (2h)
}


# ═══════════════════════════════════════════════════════════════════
# v7.3.7: WEBSOCKET TICKERS CACHE (замена fetch_tickers REST)
# ═══════════════════════════════════════════════════════════════════
# Главная цель v7.3.7: убрать fetch_tickers (weight=40) из analyst_loop.
# Раньше каждые ~8 минут делали REST-запрос → 7-8 раз/час × 40 weight = 280+/час.
# Теперь подписываемся на !ticker@arr (Binance Futures) через WebSocket —
# это 0 REST weight, обновления всех ~579 пар каждую секунду.
#
# Cache живёт в памяти, формат СОВМЕСТИМ с output fetch_tickers:
#   ws_tickers_cache[symbol] = {'quoteVolume': float, 'last': float, ...}
# Поэтому downstream-код (analyst_loop, refresh_active_symbols) не меняется,
# просто читает из этого dict вместо вызова exchange.fetch_tickers().
#
# Отдельный ws_exchange (НЕ из ws_exchanges[tf]) чтобы tickers и ohlcv шли
# через разные соединения — иначе при reconnect ohlcv тикеры тоже моргают.
ws_tickers_cache: dict = {}
ws_tickers_lock = threading.Lock()
ws_tickers_status = {'last_update': 0.0, 'symbols_count': 0, 'connected': False}

ws_tickers_exchange = ccxtpro.binance({
    'enableRateLimit': False,
    'options': {
        'defaultType': 'future',
    }
})


# ═══════════════════════════════════════════════════════════════════
# WEBSOCKET CANDLES STORAGE (Binance v7.3)
# ═══════════════════════════════════════════════════════════════════
# Глобальные хранилища свечей в памяти. Заполняются:
#   1) При старте — через REST (фаза прогрева, 5-7 минут).
#   2) После прогрева — через WebSocket (ccxt.pro watch_ohlcv_for_symbols).
#
# Структура: candles_storage[timeframe][symbol] = list[ohlcv_candle]
# Каждая ohlcv_candle = [timestamp_ms, open, high, low, close, volume]
#
# Защита: candles_lock — единый Lock для read/write всех хранилищ.
# В analyst_loop делаем КОПИЮ списка свечей внутри lock, дальше работаем без lock.
# ═══════════════════════════════════════════════════════════════════

# Лимиты истории (соответствуют v7.1/v7.2 — не меняем логику анализа)
# v7.3.9.3: CANDLES_LIMIT_4H и CANDLES_LIMIT_1H удалены (4h/1h отключены)
CANDLES_LIMIT_2H = 80    # для ATR Map baseline 50 + EMA20 + запас

candles_storage = {
    # v7.3.9.3: '4h' и '1h' убраны — остался только 2h (ядро Сжатие ATR MAP / is_sq)
    '2h': {},   # {symbol: deque(maxlen=CANDLES_LIMIT_2H)}
}
candles_lock = threading.Lock()
candles_freshness = {}   # {(symbol, timeframe): last_update_ts} — для watchdog

# Состояние прогрева
warmup_state = {
    'phase':       'idle',     # idle | warming | done
    'done':        0,
    'total':       0,
    'started_at':  None,
    'finished_at': None,
    'errors':      0,
}

# Список монет (обновляется раз в час)
active_symbols_lock = threading.Lock()
active_symbols: list = []
active_symbols_updated_at = 0.0
ACTIVE_SYMBOLS_REFRESH_SEC = 3600   # раз в час


# ═══════════════════════════════════════════════════════════════════
# GLOBAL CIRCUIT BREAKER (Binance v7.2, для REST вызовов)
# ═══════════════════════════════════════════════════════════════════
# Оставлен для fetch_tickers/fetch_funding_rates на случай, если shared IP
# поймает чужой бан. Свечи идут через WS — на них это не влияет.
# ═══════════════════════════════════════════════════════════════════
IP_BAN_UNTIL = 0.0
_IP_BAN_LOCK = threading.Lock()
BAN_WAIT_THRESHOLD_SEC = 180

USED_WEIGHT_1M       = 0
WEIGHT_LIMIT_BINANCE = 2400
WEIGHT_SOFT_THRESHOLD = 1800
_WEIGHT_LOCK = threading.Lock()


def _parse_banned_until_ms(err_str: str):
    """Парсит timestamp из сообщения Binance "banned until <ms>"."""
    m = _re.search(r'banned until (\d{13})', err_str)
    if m:
        return int(m.group(1)) / 1000.0
    return None


def set_ip_ban_until(ban_ts_sec: float):
    """Атомарно обновляет глобальный флаг бана. Берёт максимум."""
    global IP_BAN_UNTIL
    with _IP_BAN_LOCK:
        if ban_ts_sec > IP_BAN_UNTIL:
            IP_BAN_UNTIL = ban_ts_sec
            logging.warning(
                f"🚫 IP_BAN_UNTIL установлен на "
                f"{datetime.fromtimestamp(ban_ts_sec).strftime('%H:%M:%S')} "
                f"(через {ban_ts_sec - time.time():.1f} сек)"
            )


def wait_if_banned() -> bool:
    """Проверка флага бана перед REST запросом. True = можно идти."""
    global IP_BAN_UNTIL
    with _IP_BAN_LOCK:
        ban_until = IP_BAN_UNTIL
    if ban_until <= 0:
        return True
    now = time.time()
    wait = ban_until - now
    if wait <= 0:
        with _IP_BAN_LOCK:
            if IP_BAN_UNTIL <= time.time():
                IP_BAN_UNTIL = 0.0
        return True
    if wait < BAN_WAIT_THRESHOLD_SEC:
        logging.info(f"⏸ Global ban active — sleeping {wait + 2:.1f}s")
        time.sleep(wait + 2)
        with _IP_BAN_LOCK:
            if IP_BAN_UNTIL <= time.time():
                IP_BAN_UNTIL = 0.0
        return True
    else:
        logging.warning(f"⏭ Global ban {wait:.0f}s ≥ {BAN_WAIT_THRESHOLD_SEC}s — skip")
        return False


def update_used_weight_from_headers():
    """После каждого REST запроса — обновляем USED_WEIGHT_1M."""
    global USED_WEIGHT_1M
    try:
        headers = getattr(exchange, 'last_response_headers', None) or {}
        weight_str = (headers.get('x-mbx-used-weight-1m')
                      or headers.get('X-MBX-USED-WEIGHT-1m')
                      or headers.get('X-MBX-USED-WEIGHT-1M'))
        if weight_str:
            with _WEIGHT_LOCK:
                USED_WEIGHT_1M = int(weight_str)
            if USED_WEIGHT_1M >= WEIGHT_SOFT_THRESHOLD:
                now = time.time()
                seconds_to_next_minute = 60 - (now % 60) + 1
                logging.warning(
                    f"⚠️ X-MBX-USED-WEIGHT-1m={USED_WEIGHT_1M} ≥ {WEIGHT_SOFT_THRESHOLD}, "
                    f"sleep {seconds_to_next_minute:.1f}s до новой минуты"
                )
                time.sleep(seconds_to_next_minute)
    except Exception as e:
        logging.debug(f"update_used_weight: {e}")


def safe_api_call(fn, *args, **kwargs):
    """Универсальная обёртка для REST вызовов с защитой 418/429/Network.
    Проверяет текст ошибки на 'banned until' до классификации по типу,
    т.к. ccxt может бросать 418 любым из NetworkError/RateLimitExceeded/etc.
    """
    if not wait_if_banned():
        return None
    try:
        result = fn(*args, **kwargs)
        update_used_weight_from_headers()
        return result
    except Exception as e:
        err_str = str(e)
        ban_ts = _parse_banned_until_ms(err_str)
        if ban_ts:
            set_ip_ban_until(ban_ts)
            logging.warning(
                f"🚫 418 IP banned until "
                f"{datetime.fromtimestamp(ban_ts).strftime('%H:%M:%S')}: {type(e).__name__}"
            )
            return None
        if '418' in err_str or 'banned' in err_str.lower() or "I'm a teapot" in err_str:
            set_ip_ban_until(time.time() + 60)
            logging.warning(f"🚫 418 IP banned (no ts), +60s: {type(e).__name__}")
            return None
        if isinstance(e, ccxt.RateLimitExceeded):
            set_ip_ban_until(time.time() + 10)
            logging.warning(f"429 rate limit, +10s: {err_str[:200]}")
            return None
        if isinstance(e, (ccxt.ExchangeNotAvailable, ccxt.DDoSProtection)):
            set_ip_ban_until(time.time() + 30)
            logging.warning(f"{type(e).__name__}, +30s: {err_str[:200]}")
            return None
        if isinstance(e, ccxt.NetworkError):
            logging.warning(f"NetworkError (real): {err_str[:200]}")
            time.sleep(3)
            return None
        logging.error(f"safe_api_call unexpected {type(e).__name__}: {err_str[:200]}")
        return None


# ═══════════════════════════════════════════════════════════════════
# WS CANDLES API (для analyst_loop)
# ═══════════════════════════════════════════════════════════════════

def get_candles(symbol: str, timeframe: str):
    """Возвращает копию списка свечей из хранилища или None если данных нет.
    Безопасно для конкурентного чтения (создаём копию под lock)."""
    with candles_lock:
        d = candles_storage.get(timeframe, {})
        candles = d.get(symbol)
        if candles is None:
            return None
        # Создаём копию как обычный list (deque → list для совместимости с существующим кодом)
        return list(candles)


def set_candles(symbol: str, timeframe: str, ohlcv_list):
    """Полная замена списка свечей (для прогрева). Хранится как deque с maxlen."""
    # v7.3.9.3: остался только 2h (4h и 1h убраны)
    if timeframe == '2h':
        maxlen = CANDLES_LIMIT_2H
    else:
        maxlen = 100
    with candles_lock:
        candles_storage[timeframe][symbol] = deque(ohlcv_list, maxlen=maxlen)
        candles_freshness[(symbol, timeframe)] = time.time()


def update_candle(symbol: str, timeframe: str, candle):
    """Обновление одной свечи от WebSocket. Логика:
       - если timestamp совпадает с последней в хранилище → заменяем последнюю
         (формирующаяся свеча обновляется тиками)
       - если timestamp новее → добавляем (deque автоматически выкинет самую старую)
       - если хранилища нет → игнорируем (значит ещё не прогрели)
    """
    with candles_lock:
        d = candles_storage.get(timeframe, {})
        if symbol not in d:
            return  # ещё не прогрели — пропускаем
        dq = d[symbol]
        if not dq:
            dq.append(candle)
        else:
            last_ts = dq[-1][0]
            new_ts = candle[0]
            if new_ts == last_ts:
                # обновление формирующейся свечи
                dq[-1] = candle
            elif new_ts > last_ts:
                # новая закрытая свеча
                dq.append(candle)
            # else: пришла устаревшая свеча, игнорируем
        candles_freshness[(symbol, timeframe)] = time.time()


def candles_stats():
    """Для /health: сколько монет прогрето по каждому таймфрейму."""
    with candles_lock:
        return {tf: len(d) for tf, d in candles_storage.items()}

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
    "early_2h_sent":    0,           # СЖАТИЕ 2H — отправленные алерты
    "reversal_sent":    0,
    "compression_candidates_last":  0,
    "compression_total_iterations": 0,
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
        try:
            fr   = exchange.fetch_funding_rate(symbol)
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
    """v7.3.9.2: контекст рынка по BTC/ETH на 2H (раньше 4H — 4H отключён).
    BTC-контекст в 2H-сигналах информативный + питает btc_vol для
    dynamic_atr_multipliers/adaptive_threshold/score_breakout_direction.
    Берём из candles_storage['2h'] если есть, иначе fallback на REST."""
    # Сначала пробуем взять из хранилища (после прогрева так и должно быть)
    btc = get_candles('BTC/USDT:USDT', '2h')
    eth = get_candles('ETH/USDT:USDT', '2h')
    if btc and eth and len(btc) >= 5 and len(eth) >= 5:
        try:
            btc_ch = ((btc[-1][4] - btc[-2][4]) / btc[-2][4]) * 100
            eth_ch = ((eth[-1][4] - eth[-2][4]) / eth[-2][4]) * 100
            btc_moves = [abs((btc[i][4]-btc[i-1][4])/btc[i-1][4])*100 for i in range(-4, 0)]
            return {"btc_trend": "🟢" if btc_ch > -0.3 else "🔴",
                    "btc_ch": btc_ch, "btc_p": btc[-1][4],
                    "alt_power": "🚀" if eth_ch-btc_ch > 0.5 else "⚓️",
                    "alt_ch": eth_ch - btc_ch,
                    "btc_vol": np.mean(btc_moves)}
        except Exception as e:
            logging.warning(f"market_context from storage failed: {e}")

    # Fallback на REST (до прогрева или если в хранилище пусто)
    for attempt in range(3):
        try:
            btc_rest = safe_api_call(exchange.fetch_ohlcv, 'BTC/USDT:USDT', '2h', limit=5)
            if btc_rest is None:
                time.sleep(5); continue
            btc_ch = ((btc_rest[-1][4] - btc_rest[-2][4]) / btc_rest[-2][4]) * 100
            eth_rest = safe_api_call(exchange.fetch_ohlcv, 'ETH/USDT:USDT', '2h', limit=5)
            if eth_rest is None:
                time.sleep(5); continue
            eth_ch = ((eth_rest[-1][4] - eth_rest[-2][4]) / eth_rest[-2][4]) * 100
            btc_moves = [abs((btc_rest[i][4]-btc_rest[i-1][4])/btc_rest[i-1][4])*100 for i in range(1,5)]
            return {"btc_trend": "🟢" if btc_ch > -0.3 else "🔴",
                    "btc_ch": btc_ch, "btc_p": btc_rest[-1][4],
                    "alt_power": "🚀" if eth_ch-btc_ch > 0.5 else "⚓️",
                    "alt_ch": eth_ch - btc_ch,
                    "btc_vol": np.mean(btc_moves)}
        except Exception as e:
            logging.warning(f"market_context attempt {attempt+1}: {e}"); time.sleep(5)
    return {"btc_trend":"⚪️","btc_ch":0,"btc_p":0,"alt_power":"⚪️","alt_ch":0,"btc_vol":1.0}


def fetch_ohlcv_with_retry(symbol: str, timeframe: str, limit: int,
                             max_retries: int = 2):
    """
    REST helper для разовой загрузки свечей (прогрев истории, fallback).
    v7.3: ОСНОВНЫЕ свечи теперь приходят через WebSocket в candles_storage.
    Эта функция используется ТОЛЬКО для:
      - прогрева при старте бота (~1200 запросов один раз)
      - обновления свечей в watchdog (если WS заглох)
    """
    for attempt in range(max_retries + 1):
        if not wait_if_banned():
            return None
        try:
            result = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            update_used_weight_from_headers()
            return result
        except Exception as e:
            err_str = str(e)
            ban_ts = _parse_banned_until_ms(err_str)
            if ban_ts:
                set_ip_ban_until(ban_ts)
                logging.warning(
                    f"retry {symbol}: 🚫 418 banned until "
                    f"{datetime.fromtimestamp(ban_ts).strftime('%H:%M:%S')} "
                    f"(attempt {attempt+1}/{max_retries+1})"
                )
                continue
            if '418' in err_str or 'banned' in err_str.lower() or "I'm a teapot" in err_str:
                set_ip_ban_until(time.time() + 60)
                logging.warning(f"retry {symbol}: 🚫 418 (no ts), +60s")
                continue
            if isinstance(e, ccxt.RateLimitExceeded):
                set_ip_ban_until(time.time() + 10)
                continue
            if isinstance(e, (ccxt.ExchangeNotAvailable, ccxt.DDoSProtection)):
                set_ip_ban_until(time.time() + 30)
                continue
            if isinstance(e, ccxt.NetworkError):
                logging.warning(f"retry {symbol}: NetworkError: {err_str[:200]}")
                if attempt < max_retries:
                    time.sleep(3)
                continue
            logging.error(f"retry {symbol}: unexpected {type(e).__name__}: {err_str[:200]}")
            return None
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


# ═══════════════════════════════════════════════════════════════════════
# v7.3.9: FUNDING RATES CACHE (кэш на 30 минут вместо REST каждые 7 мин)
# Было: fetch_funding_rates каждую итерацию (~8-9 раз/час × weight 5 = ~40-45/час).
# Стало: REST раз в 30 минут = 2 раза/час × weight 5 = ~10/час.
# На shared IP Render после первого бана IP помечается — любой weight триггерит
# повторный бан. Снижение частоты в 4-5 раз убирает этот риск.
# ═══════════════════════════════════════════════════════════════════════
_funding_cache: dict = {}
_funding_cache_ts: float = 0.0
_funding_cache_lock = threading.Lock()
FUNDING_CACHE_SEC = 1800  # 30 минут


def fetch_all_funding_rates(exchange):
    """
    v7.3.9: Кэшированный funding rate — REST не чаще раза в 30 минут.
    Возвращает кэш если он свежий, иначе делает REST-запрос и обновляет кэш.
    При бане (None от safe_api_call) возвращает последний известный кэш
    (лучше устаревшие данные, чем пустой dict и потеря сигналов).
    """
    global _funding_cache, _funding_cache_ts
    now = time.time()

    with _funding_cache_lock:
        age = now - _funding_cache_ts
        if _funding_cache and age < FUNDING_CACHE_SEC:
            logging.debug(f"funding_rates: кэш ({age:.0f}s < {FUNDING_CACHE_SEC}s), REST пропускаем")
            return dict(_funding_cache)

    # Кэш устарел или пуст — идём в REST
    logging.info(f"funding_rates: кэш устарел ({age:.0f}s), REST запрос...")
    rates = safe_api_call(exchange.fetch_funding_rates)
    if rates is None:
        logging.warning("fetch_funding_rates: safe_api_call вернул None (ban?), возвращаем старый кэш")
        with _funding_cache_lock:
            return dict(_funding_cache)  # старый кэш лучше пустого
    try:
        parsed = {s: (r.get('fundingRate') or 0.0) for s, r in rates.items()}
        with _funding_cache_lock:
            _funding_cache = parsed
            _funding_cache_ts = time.time()
            logging.info(f"funding_rates: кэш обновлён ({len(parsed)} монет)")
        return dict(parsed)
    except Exception as e:
        logging.warning(f"fetch_funding_rates parse failed: {e}")
        with _funding_cache_lock:
            return dict(_funding_cache)


def _get_tickers_ws_or_rest(max_age_sec: int = 30):
    """v7.3.7: возвращает dict тикеров — приоритет WS-кэш, fallback REST.

    Логика:
      1) Если ws_tickers_cache наполнен и обновлялся не дольше max_age_sec назад
         → возвращаем КОПИЮ кэша (формат совместим с fetch_tickers).
      2) Иначе (WS ещё не подключился ИЛИ давно молчит)
         → один REST-вызов fetch_tickers (weight=40, через safe_api_call).

    Это снимает 95%+ REST-нагрузки fetch_tickers, оставляя REST только для
    fallback в первые секунды после деплоя (пока ws_tickers не подцепится)
    или если WS-соединение надолго разорвалось.

    Возвращает: dict[symbol → ticker] или None если совсем ничего нет.
    """
    now = time.time()
    with ws_tickers_lock:
        cache_size = len(ws_tickers_cache)
        cache_age  = now - ws_tickers_status['last_update']
        # WS-кэш свежий и непустой — используем
        if cache_size >= 100 and cache_age <= max_age_sec:
            # Возвращаем shallow copy, чтобы вызывающий код мог свободно
            # итерировать без блокировки лока на всё время аналитики.
            return dict(ws_tickers_cache)

    # Fallback на REST
    if cache_size > 0:
        logging.warning(
            f"_get_tickers: WS-cache устарел ({cache_age:.0f}s > {max_age_sec}s), "
            f"fallback на REST fetch_tickers"
        )
    else:
        logging.info("_get_tickers: WS-cache пуст, первый вызов через REST")
    return safe_api_call(exchange.fetch_tickers)


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
# WEBSOCKET INFRASTRUCTURE (Binance v7.3)
# ═══════════════════════════════════════════════════════════════════════

def refresh_active_symbols(initial: bool = False):
    """Получает актуальный список активных USDT-perp монет.
    Сортирует по объёму (для приоритезации в прогреве).
    Вызывается раз в час из watchdog + при старте.
    v7.3.1: load_markets через _safe_load_markets — переживает 418."""
    global active_symbols, active_symbols_updated_at
    try:
        # v7.3.1: безопасный load_markets с retry на 418
        # При initial=True уже вызван в startup_sequence, тут можно пропустить
        if not initial:
            _safe_load_markets()
        # v7.3.7: tickers из WS-cache (или REST fallback при initial=True,
        # когда WS ещё не подключился — нам нужны volume сразу для warmup).
        if initial:
            tickers = safe_api_call(exchange.fetch_tickers)
        else:
            tickers = _get_tickers_ws_or_rest(max_age_sec=60)
        if tickers is None:
            logging.warning("refresh_active_symbols: tickers недоступны (ban?), пропуск")
            return False
        markets = exchange.markets
        active_swaps = [s for s, m in markets.items()
                        if m.get('active')
                        and m.get('type') == 'swap'
                        and m.get('quote') == 'USDT']
        # Сортируем по объёму DESC
        vol_pairs = sorted(
            [(s, tickers.get(s, {}).get('quoteVolume', 0) or 0)
             for s in active_swaps if s in tickers],
            key=lambda x: x[1], reverse=True
        )
        new_list = [s for s, _v in vol_pairs]
        # Гарантируем что WATCHLIST в начале (даже если объёмы низкие)
        wl = [w for w in WATCHLIST if w in tickers]
        merged = list(dict.fromkeys(wl + new_list))

        with active_symbols_lock:
            old_set = set(active_symbols)
            new_set = set(merged)
            added = new_set - old_set
            removed = old_set - new_set
            active_symbols = merged
            active_symbols_updated_at = time.time()

        # v7.3.9: чистим candles_storage и candles_freshness от удалённых символов.
        # Без этого делистинги и переименования копятся в памяти бесконечно.
        # Делаем только при hourly refresh (not initial) — при initial данных ещё нет.
        if not initial and removed:
            with candles_lock:
                for sym in removed:
                    for tf in ('2h',):   # v7.3.9.3: 4h и 1h убраны из storage
                        candles_storage[tf].pop(sym, None)
                        candles_freshness.pop((sym, tf), None)
            logging.info(f"🧹 candles_storage: удалено {len(removed)} символов ({list(removed)[:5]}{'...' if len(removed)>5 else ''})")

        if initial:
            logging.info(f"📋 Active symbols: {len(merged)} (initial)")
        else:
            logging.info(
                f"📋 Active symbols refreshed: {len(merged)} total "
                f"(+{len(added)} added, -{len(removed)} removed)"
            )
            if added:
                logging.info(f"   Added: {list(added)[:10]}{'...' if len(added)>10 else ''}")
            if removed:
                logging.info(f"   Removed: {list(removed)[:10]}{'...' if len(removed)>10 else ''}")
        return True
    except Exception as e:
        logging.error(f"refresh_active_symbols failed: {e}")
        return False


def warmup_history():
    """Разовая загрузка истории свечей для всех активных монет.
    Запускается ОДИН РАЗ при старте. Занимает ~5-7 минут.
    После прогрева — данные приходят через WebSocket в фоне.
    """
    global warmup_state
    warmup_state['phase'] = 'warming'
    warmup_state['started_at'] = time.time()
    warmup_state['errors'] = 0

    with active_symbols_lock:
        syms = list(active_symbols)
    total = len(syms) * 1  # v7.3.9.2: только 2H (4H убран)
    warmup_state['total'] = total
    warmup_state['done'] = 0

    logging.info(f"🔄 WARMUP START: {len(syms)} монет × 1 таймфрейм (2H) = {total} запросов")
    _log_memory("warmup_start")

    for i, sym in enumerate(syms):
        # v7.3.9: явная пауза между символами чтобы не делать burst при рестарте.
        # rateLimit=280ms у exchange должен это делать, но явная пауза надёжнее —
        # особенно если shared IP уже в watchlist Binance после предыдущего бана.
        # 0.35s × 579 монет × 2 ТФ ≈ +405 сек к warmup (~7 мин итого) — приемлемо.
        if i > 0:
            time.sleep(0.35)

        # v7.3.9.2: 4H-загрузка убрана (4H отключён)

        # 2H
        ohlcv_2h = fetch_ohlcv_with_retry(sym, '2h', limit=CANDLES_LIMIT_2H, max_retries=2)
        if ohlcv_2h and len(ohlcv_2h) >= 40:
            set_candles(sym, '2h', ohlcv_2h)
        else:
            warmup_state['errors'] += 1
        warmup_state['done'] += 1

        # Прогресс лог каждые 50 монет
        if (i + 1) % 50 == 0:
            elapsed = time.time() - warmup_state['started_at']
            pct = warmup_state['done'] / total * 100
            eta = elapsed / (i + 1) * (len(syms) - i - 1)
            logging.info(
                f"🔄 WARMUP: {i+1}/{len(syms)} монет ({pct:.0f}%), "
                f"elapsed={elapsed:.0f}s, eta={eta:.0f}s, errors={warmup_state['errors']}"
            )
            _log_memory(f"warmup_{i+1}")

    warmup_state['phase'] = 'done'
    warmup_state['finished_at'] = time.time()
    elapsed = warmup_state['finished_at'] - warmup_state['started_at']
    stats = candles_stats()
    logging.info(
        f"✅ WARMUP DONE: {warmup_state['done']}/{total} запросов за {elapsed:.0f}s, "
        f"errors={warmup_state['errors']}, "
        f"candles_2h={stats.get('2h',0)}"
    )
    _log_memory("warmup_done")
    # v7.3.5: принудительно освободить промежуточные JSON-объекты ccxt,
    # которые могли накопиться за 1158 REST-запросов.
    collected = gc.collect()
    trimmed = _malloc_trim()  # v7.3.9.1: вернуть warmup-пик RSS в ОС
    logging.info(f"🧹 gc.collect после warmup: освобождено {collected} объектов, malloc_trim={trimmed}")
    _log_memory("after_gc")


# ─────────────────────────────────────────────────────────────────────
# WebSocket loop — работает в отдельном asyncio event loop в потоке
# v7.3.3: batching + ban-aware backoff
# ─────────────────────────────────────────────────────────────────────
ws_status = {
    'connected':       False,
    'last_message_at': 0.0,
    'reconnects':      0,
    'messages_total':  0,
    'errors':          0,
    'batch_progress':  '',   # текстовое описание прогресса подписки
}

# Размер одного батча подписки WS (Binance принимает до 200 streams в одном
# combined stream URL без проблем, но при первом коннекте лучше начинать
# с меньших порций — чтобы не триггерить anti-DDoS).
WS_BATCH_SIZE      = 50
WS_BATCH_PAUSE_SEC = 1.5   # пауза между батчами подписки
WS_RECONNECT_MIN_SEC = 30  # минимальная пауза между переподключениями watcher'а
WS_RECONNECT_MAX_SEC = 600 # максимум — 10 минут (если совсем плохо)


async def _ws_async_wait_ban():
    """Async-аналог wait_if_banned. Если флаг бана активен — ждём его снятия + 5 секунд."""
    while True:
        with _IP_BAN_LOCK:
            ban_until = IP_BAN_UNTIL
        if ban_until <= 0:
            return
        now = time.time()
        wait = ban_until - now
        if wait <= 0:
            with _IP_BAN_LOCK:
                if IP_BAN_UNTIL <= time.time():
                    globals()['IP_BAN_UNTIL'] = 0.0
            return
        # Ждём максимум 60 секунд за раз, чтобы регулярно проверять флаг и логировать
        chunk = min(wait + 5, 60)
        logging.info(f"⏸ WS waiting global ban: {wait:.0f}s remaining, sleep {chunk:.0f}s")
        await asyncio.sleep(chunk)


def _ws_parse_ban_from_error(err_str: str) -> bool:
    """Парсит 418 из текста ошибки WebSocket. Возвращает True если выставил флаг."""
    ban_ts = _parse_banned_until_ms(err_str)
    if ban_ts:
        set_ip_ban_until(ban_ts)
        logging.warning(
            f"🚫 WS error → 418 banned until "
            f"{datetime.fromtimestamp(ban_ts).strftime('%H:%M:%S')}"
        )
        return True
    if '418' in err_str or 'banned' in err_str.lower() or 'teapot' in err_str.lower():
        set_ip_ban_until(time.time() + 120)
        logging.warning(f"🚫 WS error → 418 (no ts), +120s")
        return True
    if '429' in err_str or 'too many' in err_str.lower():
        set_ip_ban_until(time.time() + 60)
        logging.warning(f"🚫 WS error → 429, +60s")
        return True
    return False


async def _ws_watch_timeframe(ws_ex, timeframe: str):
    """v7.3.3: watcher одного таймфрейма с батчевой подпиской и ban-aware retry.
    Стратегия:
      1. Берём актуальный список монет.
      2. Делим на батчи по WS_BATCH_SIZE.
      3. Для КАЖДОГО батча создаём отдельный поток подписки watch_ohlcv_for_symbols.
      4. При получении 418 на любом батче — все батчи ждут глобальный флаг бана.
      5. Exponential backoff при повторных ошибках.
    """
    reconnect_delay = WS_RECONNECT_MIN_SEC

    while True:
        try:
            # Проверяем глобальный бан перед каждым полным циклом
            await _ws_async_wait_ban()

            with active_symbols_lock:
                syms = list(active_symbols)

            if not syms:
                logging.warning(f"ws_watch[{timeframe}]: empty symbol list, sleep 5s")
                await asyncio.sleep(5)
                continue

            # Разбиваем на батчи
            batches = [syms[i:i + WS_BATCH_SIZE] for i in range(0, len(syms), WS_BATCH_SIZE)]
            total_batches = len(batches)
            logging.info(
                f"📡 ws_watch[{timeframe}]: {len(syms)} symbols → {total_batches} batches × {WS_BATCH_SIZE}"
            )

            # Создаём задачи для каждого батча — каждый batch будет отдельной WS-подпиской
            batch_tasks = []
            for batch_idx, batch_syms in enumerate(batches):
                # Пауза между запуском батчей — чтобы Binance не воспринял как flood
                if batch_idx > 0:
                    await asyncio.sleep(WS_BATCH_PAUSE_SEC)
                # Перед каждым батчом проверяем флаг бана (вдруг другой batch уже его выставил)
                await _ws_async_wait_ban()
                logging.info(
                    f"📡 ws_watch[{timeframe}] batch {batch_idx+1}/{total_batches}: "
                    f"подписываюсь на {len(batch_syms)} символов"
                )
                ws_status['batch_progress'] = (
                    f"{timeframe} batch {batch_idx+1}/{total_batches}"
                )
                task = asyncio.create_task(
                    _ws_subscribe_batch(ws_ex, batch_syms, timeframe, batch_idx)
                )
                batch_tasks.append(task)

            # При успехе все задачи висят в бесконечной watch_ohlcv_for_symbols.
            # gather с return_exceptions — собираем все исключения, не валим один на другом.
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            # v7.3.4: ЯВНО читаем все exceptions, иначе asyncio пишет
            # "Future exception was never retrieved" — это и ворнинг в логах, и
            # незакрытые future-объекты в памяти.
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    logging.debug(
                        f"ws_watch[{timeframe}] batch#{idx} task ended: "
                        f"{type(r).__name__}: {str(r)[:150]}"
                    )
            # Если все задачи завершились (что не должно быть при норме) —
            # значит соединения порвались, повторяем
            logging.warning(f"ws_watch[{timeframe}]: все батчи завершились, переподключение")
            ws_status['connected'] = False
            ws_status['reconnects'] += 1
            # Backoff
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, WS_RECONNECT_MAX_SEC)

        except asyncio.CancelledError:
            logging.info(f"ws_watch[{timeframe}]: cancelled")
            raise
        except Exception as e:
            ws_status['errors'] += 1
            ws_status['connected'] = False
            err_str = str(e)
            logging.error(
                f"ws_watch[{timeframe}] outer error: {type(e).__name__}: {err_str[:200]}"
            )
            if _ws_parse_ban_from_error(err_str):
                # 418 распознан — ждём снятия флага
                await _ws_async_wait_ban()
                reconnect_delay = WS_RECONNECT_MIN_SEC  # после бана можно агрессивнее
            else:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, WS_RECONNECT_MAX_SEC)


async def _ws_subscribe_batch(ws_ex, batch_syms, timeframe: str, batch_idx: int):
    """Подписка одного батча на watch_ohlcv_for_symbols.
    Крутится в бесконечном цикле, при ошибке возвращает контроль наверх
    через бросок исключения — outer-уровень сделает gather и переподключит ВСЁ.
    """
    sym_tf_pairs = [[s, timeframe] for s in batch_syms]
    consecutive_errors = 0
    while True:
        try:
            await _ws_async_wait_ban()
            ohlcv_update = await ws_ex.watch_ohlcv_for_symbols(sym_tf_pairs)
            consecutive_errors = 0  # сброс при успехе
            if isinstance(ohlcv_update, dict):
                for sym, tf_dict in ohlcv_update.items():
                    if isinstance(tf_dict, dict):
                        for tf, candles in tf_dict.items():
                            if candles and len(candles) > 0:
                                last_candle = candles[-1]
                                update_candle(sym, tf, last_candle)
                                ws_status['messages_total'] += 1
                                ws_status['last_message_at'] = time.time()
                                ws_status['connected'] = True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            consecutive_errors += 1
            err_str = str(e)
            err_type = type(e).__name__
            # Каждые 10 ошибок логируем (чтобы не флудить)
            if consecutive_errors <= 3 or consecutive_errors % 10 == 0:
                logging.warning(
                    f"ws_batch[{timeframe}#{batch_idx}] error #{consecutive_errors}: "
                    f"{err_type}: {err_str[:200]}"
                )
            ban_detected = _ws_parse_ban_from_error(err_str)
            if ban_detected:
                await _ws_async_wait_ban()
                # после ожидания флага продолжаем тот же батч
                continue
            # Не-бан ошибка → exponential backoff в рамках батча
            backoff = min(5 * (2 ** min(consecutive_errors - 1, 6)), 300)
            await asyncio.sleep(backoff)


async def _ws_watch_tickers():
    """v7.3.7: подписка на WebSocket-стрим всех тикеров Binance Futures.
    Заменяет fetch_tickers REST (weight=40) — Binance в своих 418-ответах
    прямо рекомендует: 'use websocket for live updates to avoid bans'.

    Поток !ticker@arr отдаёт обновления для ВСЕХ ~579 пар каждую секунду,
    одно WebSocket-соединение, 0 REST weight.

    Формат каждого тика в self.tickers (после ccxt-нормализации):
      {'symbol': 'BTC/USDT:USDT', 'quoteVolume': 1234567.89, 'last': 60000, ...}
    — совместим с output exchange.fetch_tickers(), поэтому downstream-код
    в analyst_loop читает наш cache в том же формате.
    """
    reconnect_delay = WS_RECONNECT_MIN_SEC
    consecutive_errors = 0
    while True:
        try:
            await _ws_async_wait_ban()
            logging.info("📡 ws_tickers: подписка на !ticker@arr для всех Futures-пар")
            ws_tickers_status['connected'] = True
            while True:
                # watch_tickers(None) подписывается на !ticker@arr — весь рынок.
                # Возвращает dict[symbol -> ticker] с уже накопленными обновлениями.
                tickers_update = await ws_tickers_exchange.watch_tickers()
                if tickers_update:
                    with ws_tickers_lock:
                        # Мерджим: новые обновления накладываются на старые.
                        ws_tickers_cache.update(tickers_update)
                        # v7.3.9: чистим символы которых нет в active_symbols —
                        # без этого кэш бесконечно растёт при делистингах/реструктуризации.
                        # Делаем редко (раз в ~500 тиков ≈ ~8 мин) чтобы не тратить lock каждый тик.
                        ws_tickers_status['symbols_count'] = len(ws_tickers_cache)
                        ws_tickers_status['last_update']   = time.time()
                        if ws_tickers_status['symbols_count'] > 0 and \
                                ws_tickers_status.get('_tick_counter', 0) % 500 == 0:
                            with active_symbols_lock:
                                active_set = set(active_symbols)
                            stale = [k for k in list(ws_tickers_cache.keys()) if k not in active_set]
                            for k in stale:
                                ws_tickers_cache.pop(k, None)
                            if stale:
                                logging.info(f"🧹 ws_tickers_cache: удалено {len(stale)} устаревших символов")
                        ws_tickers_status['_tick_counter'] = \
                            ws_tickers_status.get('_tick_counter', 0) + 1
                consecutive_errors = 0  # успех — сбрасываем счётчик
                reconnect_delay = WS_RECONNECT_MIN_SEC

        except asyncio.CancelledError:
            logging.info("ws_tickers: cancelled")
            ws_tickers_status['connected'] = False
            raise
        except Exception as e:
            consecutive_errors += 1
            ws_tickers_status['connected'] = False
            err_str = str(e)
            logging.error(
                f"ws_tickers error #{consecutive_errors}: "
                f"{type(e).__name__}: {err_str[:200]}"
            )
            if _ws_parse_ban_from_error(err_str):
                await _ws_async_wait_ban()
                reconnect_delay = WS_RECONNECT_MIN_SEC
                continue
            # Backoff: 5s → 10s → 20s → ... → 300s max
            backoff = min(5 * (2 ** min(consecutive_errors - 1, 6)), 300)
            logging.warning(f"ws_tickers: переподключение через {backoff}s")
            await asyncio.sleep(backoff)


async def _ws_main():
    """Главная корутина — запускает watch для всех 3-х таймфреймов параллельно
    + watch_tickers для замены fetch_tickers REST.
    v7.3.4: каждый таймфрейм на отдельном ws_exchange (см. словарь ws_exchanges).
    v7.3.7: + _ws_watch_tickers на отдельном ws_tickers_exchange."""
    logging.info("📡 WS main: starting 2h timeframe watcher + tickers watcher")
    _log_memory("ws_main_start")
    # Дополнительная пауза перед стартом 2h и 1h watchers — чтобы 4h успел
    # подписаться без флуда. Это разносит начальный peak во времени.
    # tickers стартует первым (без задержки) — без них analyst_loop стоит.
    await asyncio.gather(
        _ws_watch_tickers(),
        _ws_watch_timeframe(ws_exchanges['2h'], '2h'),
        return_exceptions=True,
    )


async def _ws_watch_with_delay(ws_ex, timeframe: str, delay: int):
    """Запускает watcher после задержки. Используется для разнесения старта 2h/1h
    относительно 4h — чтобы при первом подключении не было all-or-nothing flood."""
    logging.info(f"⏳ ws_watch[{timeframe}]: задержка старта {delay}s")
    await asyncio.sleep(delay)
    await _ws_watch_timeframe(ws_ex, timeframe)


def ws_loop():
    """Точка входа для WS-потока. Создаёт asyncio event loop и крутит _ws_main."""
    while True:
        try:
            # Ждём окончания прогрева перед запуском WS
            while warmup_state['phase'] != 'done':
                time.sleep(2)
            logging.info("📡 ws_loop: warmup done, starting asyncio event loop")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_ws_main())
        except Exception as e:
            logging.error(f"ws_loop crashed: {type(e).__name__}: {e}")
            ws_status['connected'] = False
            time.sleep(10)


# ═══════════════════════════════════════════════════════════════════════
# ОСНОВНОЙ ЦИКЛ
# ═══════════════════════════════════════════════════════════════════════
def analyst_loop():
    sent_signals = {}
    sent_attention = {}
    # История ATR Map score для подсчёта mature_bars (сколько баров подряд score≥60).
    # Ключ: symbol → deque последних 12 score'ов (24h при cycle 6-7 мин).
    atr_map_score_history: dict = {}
    logging.info("Аналитик Binance v7.3.9.3 (malloc_trim + 4H/1H removed, 2H-only) запущен.")

    # v7.3: ждём окончания прогрева истории
    while warmup_state['phase'] != 'done':
        logging.info(
            f"⏳ analyst_loop ждёт прогрев: "
            f"{warmup_state.get('done', 0)}/{warmup_state.get('total', 0)}"
        )
        time.sleep(5)
    logging.info("✅ Прогрев завершён, analyst_loop стартует основной цикл")

    while True:
        try:
            ctx = get_market_context()

            # v7.3.7: tickers из WebSocket-кэша вместо REST fetch_tickers (weight=40 → 0).
            # Fallback на REST только если WS ещё не подключился или давно молчит.
            tickers = _get_tickers_ws_or_rest()
            if tickers is None:
                logging.error("_get_tickers: WS пуст и REST вернул None (ban?), пауза 60с")
                time.sleep(60)
                continue

            # Funding rate для всех монет одним запросом (~5 weight).
            funding_rates = fetch_all_funding_rates(exchange)
            logging.info(f"Funding rates загружены: {len(funding_rates)} монет")

            # Накопитель для алертов СЖАТИЕ
            compression_candidates = []

            # v7.3: берём список монет из глобального active_symbols
            # (обновляется раз в час из watchdog)
            with active_symbols_lock:
                symbols = list(active_symbols)

            # vol_data нужен дальше для 2H блока — формируем из tickers
            vol_data = sorted(
                [{'s': s, 'v': tickers[s].get('quoteVolume', 0) or 0}
                 for s in symbols if s in tickers],
                key=lambda x: x['v'], reverse=True)

            # ══════════════════════════════════════════════════
            # v7.3.9.2: 4H-БЛОК УДАЛЁН (REVERSAL 4H + ВНИМАНИЕ 4H + РАННИЙ 4H).
            # 4H полностью отключён — этот функционал работает на отдельном
            # MEXC-боте. Здесь оставлен только 2H-скан (ATR MAP / is_sq / ранние 2H).
            # ══════════════════════════════════════════════════
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
                    # 80 свечей = 160 часов = ~6.6 дней истории — достаточно
                    # для ATR Map (baseline 50) и EMA20.
                    # v7.3: свечи берём из candles_storage (заполняется WebSocket).
                    # ──────────────────────────────────────────────────
                    ohlcv_2h = get_candles(symbol, '2h')
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
            # v7.3: счётчики для /health
            bot_status["compression_candidates_last"] = len(compression_candidates)
            bot_status["compression_total_iterations"] += len(compression_candidates)

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

            # v7.3.9.1: gc.collect() + malloc_trim() КАЖДУЮ итерацию.
            # analyst_loop создаёт ~1000+ временных list/dict за итерацию
            # (list(deque) для каждого символа × 3 ТФ + numpy arrays).
            # gc.collect() освобождает Python-объекты, но память остаётся в
            # пулах glibc → RSS не падает. malloc_trim(0) форсит glibc вернуть
            # свободные арены в ОС → RSS реально снижается. Без trim (как было
            # в v7.3.7 и v7.3.9) RSS монотонно растёт → OOM-рестарт каждые ~2ч.
            # Связка отрабатывает за ~10-50мс — на 60с-итерацию пренебрежимо.
            collected = gc.collect()
            trimmed = _malloc_trim()
            # Подробный memory-лог раз в 10 итераций (чтобы не засорять логи).
            if bot_status["iterations"] % 10 == 0:
                _log_memory(f"gc_iter_{bot_status['iterations']}")
                logging.info(
                    f"🧹 gc.collect (iter {bot_status['iterations']}): "
                    f"освобождено {collected} объектов, malloc_trim={trimmed}"
                )

            logging.info(f"Итерация. Символов 2H: {len(all_perps_2h)} | "
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
    now = time.time()

    # IP ban статус
    if IP_BAN_UNTIL > now:
        ban_str = f"🚫 BAN до {datetime.fromtimestamp(IP_BAN_UNTIL).strftime('%H:%M:%S')} (ещё {IP_BAN_UNTIL - now:.0f}с)"
    else:
        ban_str = "✅ нет"

    # Прогрев статус
    if warmup_state['phase'] == 'warming':
        pct = warmup_state['done'] / warmup_state['total'] * 100 if warmup_state['total'] else 0
        elapsed = now - warmup_state['started_at'] if warmup_state['started_at'] else 0
        if pct > 0 and elapsed > 0:
            eta = elapsed / pct * (100 - pct)
            warmup_str = f"🔄 {warmup_state['done']}/{warmup_state['total']} ({pct:.0f}%), eta {eta:.0f}с"
        else:
            warmup_str = f"🔄 {warmup_state['done']}/{warmup_state['total']} (старт)"
    elif warmup_state['phase'] == 'done':
        warmup_str = f"✅ завершён (errors: {warmup_state['errors']})"
    else:
        warmup_str = "ожидание"

    # WebSocket статус
    ws_last = ws_status['last_message_at']
    ws_age = (now - ws_last) if ws_last > 0 else -1
    if ws_status['connected'] and ws_age >= 0 and ws_age < 120:
        ws_str = f"✅ connected (msg {ws_age:.0f}с назад, total {ws_status['messages_total']})"
    elif ws_status['connected']:
        ws_str = f"⚠️ stale ({ws_age:.0f}с без сообщений)"
    else:
        ws_str = f"❌ disconnected (reconnects: {ws_status['reconnects']}, errors: {ws_status['errors']})"
    # batch progress (v7.3.3)
    if ws_status.get('batch_progress'):
        ws_str += f" | {ws_status['batch_progress']}"

    # Candles в памяти
    cs = candles_stats()
    candles_str = f"2h={cs.get('2h',0)}"

    # v7.3.7: статус WS-tickers
    tickers_age = time.time() - ws_tickers_status['last_update'] if ws_tickers_status['last_update'] else -1
    if ws_tickers_status['connected'] and tickers_age >= 0 and tickers_age < 30:
        tickers_str = f"✅ {ws_tickers_status['symbols_count']} symbols (last upd {tickers_age:.0f}s ago)"
    elif ws_tickers_status['connected']:
        tickers_str = f"⚠️ stale ({tickers_age:.0f}s without updates)"
    else:
        tickers_str = "❌ disconnected"

    return (f"✅ OK | Binance v7.3.9 (funding-cache + gc + cleanup)\n"
            f"Uptime: {uptime}\n"
            f"Итераций: {bot_status['iterations']}\n"
            f"Ошибок: {bot_status['errors']}\n"
            f"───── REST ─────\n"
            f"IP бан: {ban_str}\n"
            f"X-MBX-USED-WEIGHT-1m: {USED_WEIGHT_1M} / {WEIGHT_LIMIT_BINANCE}\n"
            f"───── WebSocket ─────\n"
            f"Прогрев: {warmup_str}\n"
            f"WS свечи: {ws_str}\n"
            f"WS tickers: {tickers_str}\n"
            f"Candles в памяти: {candles_str}\n"
            f"───── Алерты отправлены ─────\n"
            f"🗜 СЖАТИЕ 2H: {bot_status['early_2h_sent']}\n"
            f"───── Сжатие диагностика ─────\n"
            f"Кандидатов в последней итерации: {bot_status['compression_candidates_last']}\n"
            f"Кандидатов за всё время: {bot_status['compression_total_iterations']}\n"
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


def symbols_refresh_loop():
    """v7.3: раз в час обновляет список активных монет (для подхвата новых листингов).
    После обновления WS-подписки автоматически берут новый список при reconnect."""
    while True:
        time.sleep(ACTIVE_SYMBOLS_REFRESH_SEC)
        try:
            ok = refresh_active_symbols(initial=False)
            if ok:
                logging.info("📋 Active symbols refreshed (hourly)")
        except Exception as e:
            logging.error(f"symbols_refresh_loop error: {e}")


def watchdog():
    """v7.3: следит за analyst_loop И ws_loop.
    Если поток упал — перезапускает.
    v7.3.5: + раз в минуту логирует RSS для отслеживания утечек."""
    global analyst_thread, ws_thread
    time.sleep(60)
    tick = 0
    while True:
        tick += 1
        # Analyst
        if not analyst_thread.is_alive():
            logging.error("analyst_loop упал, перезапуск...")
            bot_status["errors"] += 1
            analyst_thread = threading.Thread(target=analyst_loop, daemon=True, name="analyst")
            analyst_thread.start()
        # WS
        if not ws_thread.is_alive():
            logging.error("ws_loop упал, перезапуск...")
            bot_status["errors"] += 1
            ws_thread = threading.Thread(target=ws_loop, daemon=True, name="ws")
            ws_thread.start()
        # v7.3.5: каждую минуту — замер памяти.
        # Если ползёт вверх между тиками — утечка. Если ровный — honest usage.
        _log_memory(f"watchdog_t{tick}")
        time.sleep(60)


# ═══════════════════════════════════════════════════════════════════════
# STARTUP SEQUENCE (Binance v7.3)
# ═══════════════════════════════════════════════════════════════════════
def _safe_load_markets():
    """v7.3.2: load_markets с обработкой 418/Network в бесконечном retry.
    Heartbeat-логи на каждой попытке — видим если зависли."""
    attempt = 0
    while True:
        attempt += 1
        # Heartbeat-лог каждой попытки
        logging.info(f"🔄 _safe_load_markets attempt #{attempt}: проверяю флаг бана")

        # Проверяем флаг бана
        with _IP_BAN_LOCK:
            ban_until_now = IP_BAN_UNTIL
        if ban_until_now > time.time():
            wait = ban_until_now - time.time() + 5
            logging.warning(
                f"🔄 _safe_load_markets #{attempt}: ban до "
                f"{datetime.fromtimestamp(ban_until_now).strftime('%H:%M:%S')}, sleep {wait:.0f}s"
            )
            time.sleep(min(wait, 60))  # макс 60с за раз, чтобы heartbeat шёл
            continue

        try:
            logging.info(f"🔄 _safe_load_markets #{attempt}: вызываю exchange.load_markets()...")
            # Принудительный таймаут — иначе ccxt может зависнуть на TCP без ответа
            old_timeout = exchange.timeout
            exchange.timeout = 20000  # 20 сек
            try:
                exchange.load_markets(reload=True)
            finally:
                exchange.timeout = old_timeout
            update_used_weight_from_headers()
            logging.info(f"✅ _safe_load_markets #{attempt}: УСПЕХ, {len(exchange.markets)} рынков")
            return True
        except Exception as e:
            err_str = str(e)
            err_type = type(e).__name__
            logging.warning(f"⚠️ _safe_load_markets #{attempt}: {err_type}: {err_str[:300]}")

            ban_ts = _parse_banned_until_ms(err_str)
            if ban_ts:
                set_ip_ban_until(ban_ts)
                wait = max(ban_ts - time.time(), 0) + 5
                logging.warning(
                    f"🚫 _safe_load_markets #{attempt}: 418 until "
                    f"{datetime.fromtimestamp(ban_ts).strftime('%H:%M:%S')}, sleep {min(wait, 60):.0f}s"
                )
                time.sleep(min(wait, 60))
                continue
            if '418' in err_str or 'banned' in err_str.lower() or "teapot" in err_str.lower():
                set_ip_ban_until(time.time() + 90)
                logging.warning(f"🚫 _safe_load_markets #{attempt}: 418 (no ts), sleep 60s")
                time.sleep(60)
                continue
            # Любая другая ошибка (Network, timeout, etc) — пауза 30с и retry
            logging.warning(f"⚠️ _safe_load_markets #{attempt}: retry через 30s")
            time.sleep(30)


def startup_sequence():
    """1) load_markets (с ретраями на 418) → 2) refresh_active_symbols (с ретраями) →
    3) прогрев истории → analyst + ws подхватятся сами."""
    while True:
        try:
            logging.info("🚀 Startup v7.3.9: load_markets() (с retry + heartbeat)...")
            _safe_load_markets()
            logging.info(f"   Markets loaded: {len(exchange.markets)} symbols total")

            # ═══════════════════════════════════════════════════════════════
            # v7.3.6: SHARE MARKETS BETWEEN EXCHANGES
            # По логам v7.3.5 видно что после ws_main_start (308.8MB) память
            # подскакивает на +167MB до 475.8MB. Причина: 3 ws_exchanges при
            # первом обращении вызывают свой load_markets() каждый, и каждый
            # держит свою копию словаря рынков Binance Futures (~30MB × 3).
            # Плюс REST exchange — итого 4 копии того же markets dict.
            #
            # Фикс: передаём готовый markets из REST exchange в каждый
            # ws_exchange через публичный метод set_markets(). ccxt.pro после
            # этого не будет грузить рынки повторно — флаг markets_loaded
            # установится автоматически.
            # Ожидаемая экономия: ~90MB (3 копии × ~30MB).
            # ═══════════════════════════════════════════════════════════════
            try:
                shared_markets    = exchange.markets
                shared_currencies = exchange.currencies
                for tf, ws_ex in ws_exchanges.items():
                    # set_markets перезаписывает: markets, markets_by_id,
                    # symbols, ids, currencies. Тот же dict-объект, не копия —
                    # значит реально одна структура в памяти на все 4 инстанса.
                    ws_ex.set_markets(shared_markets, shared_currencies)
                # v7.3.7: и в ws_tickers_exchange тоже — иначе он при старте
                # watch_tickers сам сделает load_markets и съест +30MB.
                ws_tickers_exchange.set_markets(shared_markets, shared_currencies)
                logging.info(
                    f"✅ v7.3.6: markets shared между REST + 2 ws_exchanges + ws_tickers "
                    f"({len(shared_markets)} symbols, экономия ~120MB)"
                )
                _log_memory("after_share_markets")
            except Exception as e:
                # Не критично — если что-то пошло не так, ws_exchanges сами
                # загрузят markets при первом обращении (как было в v7.3.5).
                logging.warning(
                    f"⚠️ v7.3.6: не удалось share markets ({type(e).__name__}: {e}), "
                    f"ws_exchanges загрузят их сами при старте WS"
                )

            logging.info("🚀 Startup: получаем список активных USDT-perp...")
            # refresh_active_symbols использует safe_api_call внутри — он ждёт бан.
            # Если вернул False (длинный бан skip) — повторяем в цикле.
            attempts = 0
            while not refresh_active_symbols(initial=(attempts == 0)):
                attempts += 1
                logging.warning(f"Startup: refresh_active_symbols failed (attempt {attempts}), retry в 30с")
                time.sleep(30)
                if attempts >= 20:
                    logging.error("Startup: 20 attempts failed, restart full sequence")
                    break
            else:
                # refresh_active_symbols вернул True — продолжаем
                logging.info("🚀 Startup: запуск warmup_history (5-7 минут)...")
                warmup_history()
                logging.info("🚀 Startup: warmup done, all systems go")
                return  # успешно завершили

            # если попали сюда — значит refresh_active_symbols 20 раз подряд провалился
            # повторяем весь startup_sequence через минуту
            logging.error("Startup: повторяем полный startup через 60с")
            time.sleep(60)
        except Exception as e:
            logging.error(f"startup_sequence FATAL: {type(e).__name__}: {e}", exc_info=True)
            logging.info("startup_sequence: повтор через 30с")
            time.sleep(30)


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT (v7.3.4)
# ═══════════════════════════════════════════════════════════════════════
analyst_thread: threading.Thread = None  # type: ignore
ws_thread:      threading.Thread = None  # type: ignore

_bot_started = False
_bot_start_lock = threading.Lock()


def start_bot():
    """Запускает все фоновые потоки. Идемпотентна — повторный вызов ничего не делает.
    v7.3.4: вынесено в функцию для чистоты и тестируемости."""
    global _bot_started, analyst_thread, ws_thread
    with _bot_start_lock:
        if _bot_started:
            logging.warning("start_bot: уже запущен, пропускаю")
            return
        _bot_started = True

    # Запускаем startup в отдельном потоке
    _log_memory("start_bot_begin")
    threading.Thread(target=startup_sequence, daemon=True, name="startup").start()

    # Analyst — ждёт окончания warmup внутри себя
    analyst_thread = threading.Thread(target=analyst_loop, daemon=True, name="analyst")
    analyst_thread.start()

    # WebSocket — тоже ждёт warmup внутри ws_loop
    ws_thread = threading.Thread(target=ws_loop, daemon=True, name="ws")
    ws_thread.start()

    # Keepalive + watchdog + hourly symbols refresh
    threading.Thread(target=keepalive_loop,       daemon=True, name="keepalive").start()
    threading.Thread(target=watchdog,             daemon=True, name="watchdog").start()
    threading.Thread(target=symbols_refresh_loop, daemon=True, name="symbols_refresh").start()
    logging.info("🚀 start_bot v7.3.9: все фоновые потоки запущены")


if __name__ == "__main__":
    start_bot()
    port = int(os.environ.get("PORT", 10000))
    # host="0.0.0.0" обязателен для Render и других PaaS
    app.run(host="0.0.0.0", port=port)
