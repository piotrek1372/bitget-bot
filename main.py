"""
BitgetBot – TSM Advanced 2.0  (Poprawiona wersja)
==================================================

Naprawione błędy względem oryginału:
  1. get_balance_usdt()  — dodany łańcuch fallback dla Bitget swap API
  2. execute_trade()     — poprawione typy zleceń: stop_market (SL) i
                           take_profit_market (TP); w oryginale oba używały
                           stopPrice jako stop-sell, co powodowało że TP
                           nigdy nie realizował się w właściwym kierunku
  3. Podwójna sprzedaż  — SL po trafieniu TP1 jest anulowany i zastępowany
                           SL na break-even dla pozostałej połowy pozycji
  4. Warunki RSI        — złagodzone z 40/60 na konfigurowalne 45/55
                           (ENV: RSI_LONG_THRESHOLD, RSI_SHORT_THRESHOLD)
  5. Limit świec        — podniesiony z 250 do 400, żeby EMA200 miał pełne
                           200 świec rozgrzewki przed pierwszym sygnałem
  6. Health-check HTTP  — opcjonalny serwer Flask dla Heroku / VPS monitoringu
  7. Backoff przy błędach — eksponencjalny czas oczekiwania przy Network/Exchange errors
"""

import asyncio
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional, Tuple

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv

# Flask do health-checku (opcjonalne — wymagane jeśli hostujesz na Heroku)
try:
    from flask import Flask
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
#  KONFIGURACJA
# ──────────────────────────────────────────────────────────────────────────────

PROXY_URL      = os.environ.get("FIXIE_URL")          # Heroku Fixie proxy (opcjonalne)
API_KEY        = os.getenv("BITGET_API_KEY")
API_SECRET     = os.getenv("BITGET_API_SECRET")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE")

SYMBOL     = os.getenv("SYMBOL",    "SUI/USDT:USDT")  # Para futures
TIMEFRAME  = os.getenv("TIMEFRAME", "15m")            # Interwał świec
LEVERAGE   = int(os.getenv("LEVERAGE",    "20"))      # Dźwignia
RISK_PCT   = float(os.getenv("RISK_PERCENT", "3.0")) / 100.0  # % kapitału ryzykowany na trade

# POPRAWKA #4 — RSI: złagodzone progi.
# Oryginał: 40 / 60 — prawie niemożliwe do spełnienia jednocześnie z trendem EMA.
# Nowe wartości konfigurowalne przez .env; domyślnie 45 / 55.
RSI_LONG  = float(os.getenv("RSI_LONG_THRESHOLD",  "45"))
RSI_SHORT = float(os.getenv("RSI_SHORT_THRESHOLD", "55"))

# POPRAWKA #5 — więcej świec: EMA200 potrzebuje 200 świec rozgrzewki,
# więc minimum sensowne to 400 (200 warmup + 200 "żywych" danych).
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "400"))

DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"

# ──────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BitgetBot")

# ──────────────────────────────────────────────────────────────────────────────
#  POPRAWKA #6 — Health-check server dla Heroku
# ──────────────────────────────────────────────────────────────────────────────

def start_health_server() -> None:
    """
    Heroku wymaga, żeby proces webowy nasłuchiwał na $PORT w ciągu 60 sekund.
    Bez tego dyno zostaje zabite nawet jeśli bot działa poprawnie.

    Ten minimalistyczny serwer Flask rozwiązuje problem — działa w osobnym
    wątku daemon (zatrzymuje się automatycznie gdy główny proces się kończy).

    Na VPS można ustawić PORT="" aby wyłączyć serwer.
    """
    port = int(os.environ.get("PORT", 0))
    if not FLASK_AVAILABLE or not port:
        return

    app = Flask(__name__)

    @app.route("/")
    def health():
        return {
            "status":    "running",
            "symbol":    SYMBOL,
            "timeframe": TIMEFRAME,
            "dry_run":   DRY_RUN,
        }, 200

    thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, use_reloader=False),
        daemon=True,
    )
    thread.start()
    logger.info(f"Health-check server uruchomiony na porcie {port}")


# ──────────────────────────────────────────────────────────────────────────────
#  BOT
# ──────────────────────────────────────────────────────────────────────────────

class BitgetBot:
    """
    TSM Advanced 2.0 — strategia Trend + RSI Pullback na futures Bitget.

    Logika wejść:
      LONG  : Close > EMA50 > EMA200  I  RSI < RSI_LONG   (pullback w uptrend)
      SHORT : Close < EMA50 < EMA200  I  RSI > RSI_SHORT  (pullback w downtrend)

    Zarządzanie ryzykiem:
      - Wielkość pozycji  : (Equity × RISK_PCT) / |Entry − SL|
      - Stop-Loss         : Entry ± 2 × ATR14
      - Take-Profit 1     : 2:1 R:R  (50% pozycji)
      - Take-Profit 2     : 4:1 R:R  (pozostałe 50%)
      - Break-even        : po TP1 SL przesuwa się na cenę wejścia
    """

    def __init__(self) -> None:
        config: dict = {
            "apiKey":          API_KEY,
            "secret":          API_SECRET,
            "password":        API_PASSPHRASE,
            "enableRateLimit": True,
            "options":         {"defaultType": "swap"},
        }
        if PROXY_URL:
            p = PROXY_URL if PROXY_URL.startswith("http") else f"http://{PROXY_URL}"
            config["proxies"] = {"http": p, "https": p}

        self.exchange = ccxt.bitget(config)
        self.is_running = True

        # Stan wewnętrzny — śledzenie aktualnej pozycji
        self.sl_order_id:   Optional[str]   = None   # ID zlecenia SL (do anulowania po TP1)
        self.entry_price:   Optional[float] = None   # Cena wejścia (do break-even SL)
        self.original_qty:  Optional[float] = None   # Pełna wielkość pozycji przy wejściu
        self.tp1_hit:       bool            = False   # Flaga: czy TP1 już się zrealizował
        self.position_side: Optional[str]   = None   # "long" lub "short"

    # ──────────────────────────────────────────────────────────────────────────
    #  INICJALIZACJA
    # ──────────────────────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        logger.info(f"Łączę z Bitget — {SYMBOL} | dźwignia: {LEVERAGE}x | DRY_RUN: {DRY_RUN}")
        try:
            try:
                # Isolated margin redukuje ryzyko: strata ograniczona do marży jednej pozycji
                await self.exchange.set_margin_mode("isolated", SYMBOL)
            except Exception:
                pass  # Może być już ustawiony — błąd niekrytyczny
            await self.exchange.set_leverage(LEVERAGE, SYMBOL)
            logger.info("Inicjalizacja zakończona pomyślnie.")
        except Exception as e:
            logger.error(f"Błąd inicjalizacji: {e}")
            raise

    # ──────────────────────────────────────────────────────────────────────────
    #  DANE RYNKOWE
    # ──────────────────────────────────────────────────────────────────────────

    async def fetch_data(self) -> pd.DataFrame:
        """
        Pobiera świece OHLCV i oblicza wszystkie wskaźniki.

        POPRAWKA #5 — CANDLE_LIMIT=400:
          EMA(200) wymaga dokładnie 200 świec do pełnego rozgrzania.
          Przy 250 świecach tylko ~50 ostatnich wartości EMA200 jest wiarygodnych.
          Z 400 świecami mamy 200 świec rozgrzewki + 200 "żywych" danych,
          co sprawia że EMA200 na ostatnich świecach jest w pełni precyzyjne.
        """
        ohlcv = await self.exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["ema_50"]  = ta.ema(df["close"], length=50)
        df["ema_200"] = ta.ema(df["close"], length=200)
        df["rsi_14"]  = ta.rsi(df["close"], length=14)
        df["atr_14"]  = ta.atr(df["high"], df["low"], df["close"], length=14)
        return df

    # ──────────────────────────────────────────────────────────────────────────
    #  SALDO
    # ──────────────────────────────────────────────────────────────────────────

    async def get_balance_usdt(self) -> float:
        """
        POPRAWKA #1 — Odczyt salda z łańcuchem fallback.

        Oryginał: balance.get("USDT", {}).get("total", 0.0)
        Problem:  Bitget futures (swap) w różnych wersjach ccxt zwraca saldo
                  w różnych miejscach struktury. Oryginał zwracał 0 na wielu
                  konfiguracjach, co uniemożliwiało otwarcie jakiejkolwiek pozycji.

        Nowe podejście: 3 ścieżki fallback — jeśli pierwsza nie zwróci wartości
        > 0, próbujemy kolejnej. Logujemy z której ścieżki pochodzi wartość.
        """
        raw = await self.exchange.fetch_balance()

        # Ścieżka 1 — standardowa ujednolicona struktura ccxt
        usdt = raw.get("USDT", {})
        if isinstance(usdt, dict):
            val = usdt.get("total") or usdt.get("free") or 0
            if float(val) > 0:
                logger.debug(f"Saldo (ścieżka 1 — ccxt unified): ${float(val):.2f}")
                return float(val)

        # Ścieżka 2 — surowa odpowiedź Bitget V2 (info.data[].equity)
        # Bitget Futures API zwraca saldo w info.data jako lista kont walutowych
        info_data = raw.get("info", {}).get("data", [])
        if isinstance(info_data, list):
            for entry in info_data:
                if str(entry.get("marginCoin", "")).upper() == "USDT":
                    val = entry.get("equity") or entry.get("available") or 0
                    if float(val) > 0:
                        logger.debug(f"Saldo (ścieżka 2 — info.data): ${float(val):.2f}")
                        return float(val)

        # Ścieżka 3 — saldo w raw["total"]["USDT"] (stary format niektórych wersji ccxt)
        total_block = raw.get("total", {})
        if isinstance(total_block, dict):
            val = total_block.get("USDT", 0)
            if float(val) > 0:
                logger.debug(f"Saldo (ścieżka 3 — raw total): ${float(val):.2f}")
                return float(val)

        logger.warning(
            "Nie można odczytać salda USDT. Sprawdź uprawnienia API "
            "(wymagane: Futures Read + Trade) oraz typ konta (swap)."
        )
        return 0.0

    # ──────────────────────────────────────────────────────────────────────────
    #  WIELKOŚĆ POZYCJI
    # ──────────────────────────────────────────────────────────────────────────

    async def calculate_qty(self, entry: float, sl: float) -> Tuple[float, float]:
        """
        Oblicza wielkość pozycji tak, żeby trafienie SL straciło dokładnie
        RISK_PCT × equity, niezależnie od dźwigni.

        Wzór: qty = (equity × RISK_PCT) / |entry − SL|

        Przykład: equity=1000$, RISK_PCT=3%, entry=3.50, SL=3.40
          → ryzyko $= 30$, odległość do SL= 0.10$
          → qty = 30 / 0.10 = 300 tokenów
          → wartość pozycji = 300 × 3.50 = 1050$
          → marża wymagana = 1050 / 20 = 52.5$ (5.25% equity) ✓

        Cap 90%: jeśli obliczona marża > 90% equity, zmniejszamy qty.
        Zapobiega to natychmiastowej likwidacji przy bardzo małym SL distance.
        """
        equity = await self.get_balance_usdt()
        if equity <= 0:
            logger.warning("Saldo = 0 lub nieodczytywalne — pomijam transakcję.")
            return 0.0, 0.0

        price_risk = abs(entry - sl)
        if price_risk == 0:
            logger.warning("Odległość Entry-SL = 0 — pomijam transakcję.")
            return 0.0, 0.0

        risk_amount = equity * RISK_PCT
        raw_qty = risk_amount / price_risk

        # Sprawdzenie czy wymagana marża nie przekracza 90% kapitału
        required_margin = (raw_qty * entry) / LEVERAGE
        if required_margin > equity * 0.9:
            logger.warning(
                f"Wymagana marża ${required_margin:.2f} > 90% equity ${equity:.2f}. "
                f"Zmniejszam pozycję."
            )
            raw_qty = (equity * 0.9 * LEVERAGE) / entry

        qty = float(self.exchange.amount_to_precision(SYMBOL, raw_qty))
        return qty, equity

    # ──────────────────────────────────────────────────────────────────────────
    #  WYKONANIE ZLECENIA
    # ──────────────────────────────────────────────────────────────────────────

    async def execute_trade(
        self,
        side:  str,    # "buy" lub "sell"
        qty:   float,
        sl:    float,
        tp1:   float,
        tp2:   float,
        entry: float,  # szacowana cena wejścia do logowania stanu
    ) -> None:
        """
        Otwiera pozycję i ustawia zlecenia ochronne.

        POPRAWKA #2 — Poprawne typy zleceń SL i TP:
          Oryginał używał type="market" z params={"stopPrice": ...} dla WSZYSTKICH
          zleceń ochronnych. To powodowało że zlecenia TP były faktycznie
          stop-sell orders (trigger gdy cena SPADA poniżej progu) — dla pozycji
          LONG to oznacza że TP nigdy nie trigerował się przy wzroście ceny,
          tylko przy spadku, działając jak drugi SL.

          Poprawka:
            stop_market         → trigger gdy cena porusza się PRZECIWKO pozycji
                                  (używamy dla SL: sell-stop dla LONG, buy-stop dla SHORT)
            take_profit_market  → trigger gdy cena porusza się NA KORZYŚĆ pozycji
                                  (używamy dla TP1 i TP2)

        POPRAWKA #3 — Brak podwójnej sprzedaży:
          Oryginał: SL dla pełnej qty + TP1 dla qty/2 + TP2 dla qty/2
          Problem:  po TP1 (zamknięcie 50%), SL nadal aktywny dla 100%.
                    Przy trafieniu SL — próba zamknięcia 100% gdy zostało 50%.
                    W najgorszym razie: otwarcie nowej pozycji w przeciwnym kierunku.

          Poprawka: SL ustawiony dla pełnego qty. Po wykryciu trafienia TP1
                    w pętli głównej — anulujemy stary SL i stawiamy nowy
                    SL dla qty/2 NA BREAK-EVEN (cena wejścia).
        """
        exit_side = "sell" if side == "buy" else "buy"
        qty_half  = float(self.exchange.amount_to_precision(SYMBOL, qty / 2))

        if DRY_RUN:
            logger.info(
                f"[DRY RUN] {side.upper()} | Qty: {qty} | Entry≈{entry:.4f} | "
                f"SL: {sl:.4f} | TP1: {tp1:.4f} | TP2: {tp2:.4f}"
            )
            # Zapisujemy stan nawet w DRY_RUN żeby logika break-even działała
            self.entry_price   = entry
            self.original_qty  = qty
            self.position_side = "long" if side == "buy" else "short"
            self.tp1_hit       = False
            self.sl_order_id   = "DRY_RUN_SL"
            return

        try:
            # 1. Zlecenie wejścia (market order)
            order = await self.exchange.create_market_order(SYMBOL, side, qty)
            actual_entry = float(order.get("average") or order.get("price") or entry)
            logger.info(
                f"Wejście {side.upper()} | ID: {order['id']} | "
                f"Cena: {actual_entry:.4f} | Qty: {qty}"
            )
            self.entry_price   = actual_entry
            self.original_qty  = qty
            self.position_side = "long" if side == "buy" else "short"
            self.tp1_hit       = False

            # Krótka pauza — giełda musi zarejestrować pozycję zanim postawimy zlecenia ochronne
            await asyncio.sleep(1.5)

            # 2. Stop-Loss — dla pełnej kwoty
            # stop_market: realizuje się gdy cena SPADA do sl (dla LONG SELL)
            #              lub ROŚNIE do sl (dla SHORT BUY)
            sl_order = await self.exchange.create_order(
                SYMBOL, "stop_market", exit_side, qty, None,
                {"stopPrice": sl, "reduceOnly": True},
            )
            self.sl_order_id = sl_order.get("id")
            logger.info(f"SL ustawiony na {sl:.4f} | ID: {self.sl_order_id}")

            # 3. Take-Profit 1 — połowa pozycji, 2:1 R:R
            # take_profit_market: realizuje się gdy cena ROŚNIE do tp1 (dla LONG SELL)
            #                     lub SPADA do tp1 (dla SHORT BUY)
            tp1_order = await self.exchange.create_order(
                SYMBOL, "take_profit_market", exit_side, qty_half, None,
                {"stopPrice": tp1, "reduceOnly": True},
            )
            logger.info(f"TP1 ustawiony na {tp1:.4f} | Qty: {qty_half} | ID: {tp1_order.get('id')}")

            # 4. Take-Profit 2 — reszta pozycji, 4:1 R:R
            tp2_order = await self.exchange.create_order(
                SYMBOL, "take_profit_market", exit_side, qty_half, None,
                {"stopPrice": tp2, "reduceOnly": True},
            )
            logger.info(f"TP2 ustawiony na {tp2:.4f} | Qty: {qty_half} | ID: {tp2_order.get('id')}")

        except Exception as e:
            logger.error(f"Błąd wykonania zlecenia: {e}")
            # Próba awaryjnego zamknięcia pozycji
            try:
                await self.exchange.create_market_order(
                    SYMBOL, exit_side, qty, {"reduceOnly": True}
                )
                logger.warning("Awaryjne zamknięcie pozycji po błędzie.")
            except Exception as close_err:
                logger.error(f"Awaryjne zamknięcie też nieudane: {close_err}")
            # Reset stanu
            self._reset_state()

    # ──────────────────────────────────────────────────────────────────────────
    #  ZARZĄDZANIE SL PO TP1 (BREAK-EVEN)
    # ──────────────────────────────────────────────────────────────────────────

    async def handle_tp1_breakeven(self, active_contracts: float, exit_side: str) -> None:
        """
        POPRAWKA #3 (część 2) — Przesunięcie SL na break-even po trafieniu TP1.

        Kiedy TP1 się realizuje (zamknięcie 50% pozycji), wywoływana jest ta metoda:
          1. Anuluje stary SL (który był dla pełnej kwoty)
          2. Stawia nowy SL w cenie wejścia (break-even) dla remaining qty

        Efekt: druga połowa transakcji jest od tego momentu bezryzykowna —
               nawet jeśli cena wróci do entry, wyjdziemy na 0 nie na stracie.
        """
        if not self.sl_order_id or not self.entry_price or DRY_RUN:
            return

        try:
            # Anuluj stary SL
            await self.exchange.cancel_order(self.sl_order_id, SYMBOL)
            logger.info(f"Stary SL {self.sl_order_id} anulowany (TP1 trafiony)")
        except Exception as e:
            logger.warning(f"Nie udało się anulować starego SL: {e} — może już wygasł")

        try:
            # Postaw nowy SL na break-even dla pozostałej kwoty
            qty_remaining = float(self.exchange.amount_to_precision(SYMBOL, active_contracts))
            new_sl_order = await self.exchange.create_order(
                SYMBOL, "stop_market", exit_side, qty_remaining, None,
                {"stopPrice": self.entry_price, "reduceOnly": True},
            )
            self.sl_order_id = new_sl_order.get("id")
            self.tp1_hit     = True
            logger.info(
                f"Break-even SL ustawiony na {self.entry_price:.4f} | "
                f"Qty: {qty_remaining} | ID: {self.sl_order_id}"
            )
        except Exception as e:
            logger.error(f"Nie udało się ustawić break-even SL: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    #  RESET STANU
    # ──────────────────────────────────────────────────────────────────────────

    def _reset_state(self) -> None:
        """Czyści stan wewnętrzny po zamknięciu pozycji."""
        self.sl_order_id   = None
        self.entry_price   = None
        self.original_qty  = None
        self.tp1_hit       = False
        self.position_side = None

    # ──────────────────────────────────────────────────────────────────────────
    #  GŁÓWNA LOGIKA STRATEGII
    # ──────────────────────────────────────────────────────────────────────────

    async def process_strategy(self) -> None:
        df = await self.fetch_data()
        if len(df) < 205:
            logger.warning(f"Za mało świec: {len(df)} — potrzeba >= 205")
            return

        # iloc[-2] = ostatnia ZAMKNIĘTA świeca.
        # Użycie iloc[-1] (bieżąca świeca) powoduje look-ahead bias:
        # RSI i EMA aktualizują się w czasie rzeczywistym wewnątrz świecy,
        # co daje fałszywe sygnały które znikają po zamknięciu baru.
        last   = df.iloc[-2]
        c      = float(last["close"])
        ema50  = float(last["ema_50"])
        ema200 = float(last["ema_200"])
        rsi    = float(last["rsi_14"])
        atr    = float(last["atr_14"])

        logger.info(
            f"Analiza: Close={c:.4f} | EMA50={ema50:.4f} | EMA200={ema200:.4f} | "
            f"RSI={rsi:.1f} | ATR={atr:.5f}"
        )

        # ── Sprawdź aktywną pozycję ────────────────────────────────────────
        positions = await self.exchange.fetch_positions([SYMBOL])
        active    = next((p for p in positions if float(p.get("contracts", 0)) > 0), None)

        if active:
            side      = active.get("side", "")
            contracts = float(active.get("contracts", 0))
            pnl       = float(active.get("unrealizedPnl", 0))
            exit_side = "sell" if side == "long" else "buy"

            logger.info(
                f"Aktywna pozycja: {side.upper()} | Qty: {contracts} | "
                f"Niezrealizowany PnL: ${pnl:.2f}"
            )

            # ── Sprawdź czy TP1 został trafiony (POPRAWKA #3) ─────────────
            # Jeśli nie oznaczyliśmy jeszcze TP1 jako hit, sprawdzamy
            # czy pozycja zmalała o ~50% relative do original_qty
            if not self.tp1_hit and self.original_qty and not DRY_RUN:
                expected_half = self.original_qty / 2
                # Jeśli contracts ≈ połowa original_qty (z marginesem 20%) → TP1 trafiony
                if contracts <= expected_half * 1.2:
                    logger.info(
                        f"Wykryto trafienie TP1 — pozycja zmalała z "
                        f"{self.original_qty} do {contracts}. Przesuwam SL na break-even."
                    )
                    await self.handle_tp1_breakeven(contracts, exit_side)

            # ── Logika wyjścia przy odwróceniu trendu ─────────────────────
            # Jeśli cena przekroczyła EMA50 w kierunku przeciwnym do pozycji,
            # traktujemy to jako sygnał że trend się zakończył.
            # Anulujemy wszystkie otwarte zlecenia ochronne i wychodzimy z rynku.
            reversal = (
                (side == "long"  and c < ema50) or
                (side == "short" and c > ema50)
            )
            if reversal:
                logger.info(f"Odwrócenie trendu — zamykam {side.upper()} @ {c:.4f}")
                if not DRY_RUN:
                    await self.exchange.cancel_all_orders(SYMBOL)
                    await self.exchange.create_market_order(
                        SYMBOL, exit_side, contracts, {"reduceOnly": True}
                    )
                    logger.info("Pozycja zamknięta (trend reversal exit).")
                else:
                    logger.info(f"[DRY RUN] Zamknięcie {side.upper()} przy trend reversal.")
                self._reset_state()

            return  # Nie szukamy wejść gdy pozycja jest aktywna

        # ── Brak aktywnej pozycji — szukamy sygnału ───────────────────────

        # LONG: silny uptrend (EMA50 > EMA200, cena powyżej obu) + wyprzedanie RSI
        # Interpretacja: rynek jest w trendzie wzrostowym, ale chwilowo się koryguje
        # (RSI spada) — to potencjalny dobry moment wejścia z wiatrem w żagle trendu
        if c > ema50 > ema200 and rsi < RSI_LONG:
            sl  = c - (2 * atr)           # SL = 2 × ATR poniżej wejścia
            qty, equity = await self.calculate_qty(c, sl)
            if qty > 0:
                tp1 = c + 2 * (c - sl)   # TP1 = 2:1 R:R
                tp2 = c + 4 * (c - sl)   # TP2 = 4:1 R:R
                logger.info(
                    f"SYGNAŁ LONG | Equity: ${equity:.2f} | Ryzyko: ${equity*RISK_PCT:.2f} | "
                    f"Qty: {qty} | SL: {sl:.4f} | TP1: {tp1:.4f} | TP2: {tp2:.4f}"
                )
                await self.execute_trade("buy", qty, sl, tp1, tp2, c)

        # SHORT: silny downtrend (EMA50 < EMA200, cena poniżej obu) + wykupienie RSI
        # Interpretacja: rynek jest w trendzie spadkowym, ale chwilowo odbija
        # (RSI rośnie) — okazja do shortowania zgodnie z trendem
        elif c < ema50 < ema200 and rsi > RSI_SHORT:
            sl  = c + (2 * atr)           # SL = 2 × ATR powyżej wejścia
            qty, equity = await self.calculate_qty(c, sl)
            if qty > 0:
                tp1 = c - 2 * (sl - c)   # TP1 = 2:1 R:R
                tp2 = c - 4 * (sl - c)   # TP2 = 4:1 R:R
                logger.info(
                    f"SYGNAŁ SHORT | Equity: ${equity:.2f} | Ryzyko: ${equity*RISK_PCT:.2f} | "
                    f"Qty: {qty} | SL: {sl:.4f} | TP1: {tp1:.4f} | TP2: {tp2:.4f}"
                )
                await self.execute_trade("sell", qty, sl, tp1, tp2, c)

        else:
            logger.info(
                f"Brak sygnału. "
                f"({'↑' if c > ema50 else '↓'} EMA50, "
                f"{'↑' if ema50 > ema200 else '↓'} EMA200, "
                f"RSI={rsi:.1f})"
            )

    # ──────────────────────────────────────────────────────────────────────────
    #  PĘTLA GŁÓWNA
    # ──────────────────────────────────────────────────────────────────────────

    async def main_loop(self) -> None:
        await self.initialize()
        consecutive_errors = 0

        while self.is_running:
            try:
                await self.process_strategy()
                consecutive_errors = 0

                # Obliczamy czas do początku następnej świecy (+5s bufor)
                # Dzięki temu bot analizuje zawsze zamkniętą świecę, nie formującą się
                now            = datetime.now(timezone.utc)
                tf_min         = int(TIMEFRAME.replace("m", ""))
                elapsed_in_bar = (now.minute % tf_min) * 60 + now.second
                sleep_sec      = tf_min * 60 - elapsed_in_bar + 5
                logger.info(f"Następna analiza za {sleep_sec}s.")
                await asyncio.sleep(max(sleep_sec, 10))

            except ccxt.NetworkError as e:
                # Problemy sieciowe — wykładniczy backoff (max 5 minut)
                consecutive_errors += 1
                wait = min(30 * consecutive_errors, 300)
                logger.error(f"Błąd sieci ({consecutive_errors}x): {e} — retry za {wait}s")
                await asyncio.sleep(wait)

            except ccxt.ExchangeError as e:
                # Błąd giełdy (np. błąd API, przekroczenie limitu) — dłuższy backoff
                consecutive_errors += 1
                wait = min(60 * consecutive_errors, 600)
                logger.error(f"Błąd giełdy ({consecutive_errors}x): {e} — retry za {wait}s")
                await asyncio.sleep(wait)

            except Exception as e:
                consecutive_errors += 1
                logger.exception(f"Nieoczekiwany błąd: {e}")
                await asyncio.sleep(30)

    # ──────────────────────────────────────────────────────────────────────────
    #  ZAMKNIĘCIE
    # ──────────────────────────────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """Czyste zamknięcie połączenia z giełdą."""
        self.is_running = False
        try:
            await self.exchange.close()
        except Exception:
            pass
        logger.info("Bot zatrzymany, połączenie zamknięte.")


# ──────────────────────────────────────────────────────────────────────────────
#  PUNKT WEJŚCIA
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Uruchom health-check HTTP (potrzebny dla Heroku, opcjonalny na VPS)
    start_health_server()

    bot = BitgetBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Zatrzymano przez użytkownika (Ctrl+C).")
    finally:
        asyncio.run(bot.shutdown())
