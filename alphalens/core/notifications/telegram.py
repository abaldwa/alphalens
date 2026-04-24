"""
alphalens/core/notifications/telegram.py

Telegram Bot notification engine.

Sends:
  - EOD report: market cycle + top buy/sell signals
  - Morning alert: gap analysis + top 3 intraday setups
  - Pre-close alert: open intraday positions check
  - Drawdown alert: 10% drawdown + ML recommendation
  - Signal alerts: individual buy/sell triggers
  - Error alerts: scheduler job failures

Message format philosophy:
  - Trading signals: compact, actionable (symbol, entry, target, SL, R:R)
  - Investment reports: full context (cycle, strategy, fundamentals summary)
  - Always include emoji for quick visual scanning on mobile

Setup:
  1. Create a bot via @BotFather on Telegram
  2. Get your chat ID via @userinfobot
  3. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env

Usage:
    tg = TelegramNotifier()
    tg.send_signal(signal_dict)
    tg.send_eod_report(cycle_ctx, signals)
"""

import asyncio
from datetime import datetime
from typing import Optional

from loguru import logger

from config.settings import settings


class TelegramNotifier:

    def __init__(self):
        self.token   = settings.telegram_bot_token
        self.chat_id = settings.telegram_chat_id
        self._bot    = None

    def _get_bot(self):
        if self._bot is None:
            if not self.token:
                raise ValueError("TELEGRAM_BOT_TOKEN not set in .env")
            from telegram import Bot
            self._bot = Bot(token=self.token)
        return self._bot

    def send(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send a message to the configured chat. Returns True on success."""
        if not self.token or not self.chat_id:
            logger.debug("Telegram not configured — skipping")
            return False
        try:
            asyncio.run(self._send_async(message, parse_mode))
            return True
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
            return False

    async def _send_async(self, message: str, parse_mode: str):
        bot = self._get_bot()
        # Split long messages (Telegram limit: 4096 chars)
        for chunk in self._split_message(message, 4000):
            await bot.send_message(
                chat_id    = self.chat_id,
                text       = chunk,
                parse_mode = parse_mode
            )

    # ── Message Templates ─────────────────────────────────────────────────

    def send_signal(self, signal: dict) -> bool:
        """
        Send a BUY or SELL signal alert.
        Compact format for trading signals.
        """
        sig_type = signal.get("signal_type", "BUY").upper()
        symbol   = signal.get("symbol", "?")
        entry    = signal.get("entry_price", 0)
        target   = signal.get("target_price", 0)
        sl       = signal.get("stop_loss", 0)
        rr       = signal.get("risk_reward", 0)
        conf     = signal.get("confidence", 0)
        strategy = signal.get("strategy_name", "")
        tf       = signal.get("timeframe", "").upper().replace("_", "-")
        cycle    = signal.get("cycle_context", "").upper()

        emoji = "🟢" if sig_type == "BUY" else "🔴"
        cycle_emoji = {"BULL": "📈", "BEAR": "📉", "NEUTRAL": "➡️"}.get(cycle, "")

        msg = (
            f"{emoji} <b>{sig_type}: {symbol}</b> [{tf}]\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 Entry:   ₹{entry:,.2f}\n"
            f"🎯 Target:  ₹{target:,.2f}\n"
            f"🛑 SL:      ₹{sl:,.2f}\n"
            f"⚖️ R:R:     {rr:.1f}x\n"
            f"🔮 Conf:    {conf*100:.0f}%\n"
            f"📊 Strat:   {strategy}\n"
            f"{cycle_emoji} Market:  {cycle}\n"
            f"⏰ {datetime.now().strftime('%d-%b %H:%M')}"
        )
        return self.send(msg)

    def send_eod_report(self, cycle_ctx, signals: list) -> bool:
        """
        6:30 PM EOD report.
        Market cycle + sector summary + top signals for tomorrow.
        """
        market   = cycle_ctx.market_cycle.upper()
        conf     = cycle_ctx.market_confidence
        cycle_emoji = {"BULL": "📈", "BEAR": "📉", "NEUTRAL": "➡️"}.get(market, "")

        # Count sector cycles
        sector_summary = self._format_sector_summary(cycle_ctx.sector_cycles)

        # Top signals by timeframe
        buy_signals  = [s for s in signals if s.signal_type == "buy"]
        sell_signals = [s for s in signals if s.signal_type == "sell"]

        msg = (
            f"🌙 <b>AlphaLens EOD Report</b>\n"
            f"{datetime.now().strftime('%d %b %Y')}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{cycle_emoji} <b>Market: {market}</b> ({conf*100:.0f}%)\n\n"
            f"{sector_summary}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>New Signals ({len(signals)} total)</b>\n"
            f"🟢 BUY:  {len(buy_signals)}\n"
            f"🔴 SELL: {len(sell_signals)}\n\n"
        )

        # Top 5 buy signals
        if buy_signals:
            msg += "🟢 <b>Top BUY Setups:</b>\n"
            for s in buy_signals[:5]:
                msg += (
                    f"  • {s.symbol} [{s.timeframe.upper()}] "
                    f"Entry₹{s.entry_price:,.0f} → T₹{s.target_price:,.0f} "
                    f"SL₹{s.stop_loss:,.0f} ({s.risk_reward:.1f}x)\n"
                )

        msg += f"\n⏰ Next review: 9:30 AM"
        return self.send(msg)

    def send_morning_alert(self, gap_data: dict) -> bool:
        """9:30 AM morning gap analysis alert."""
        gap_ups   = gap_data.get("gap_ups", [])
        gap_downs = gap_data.get("gap_downs", [])
        intraday  = gap_data.get("intraday_signals", [])

        msg = (
            f"🌅 <b>AlphaLens Morning Alert</b> — 9:30 AM\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
        )

        if gap_ups:
            msg += f"\n📈 <b>Gap UP ({len(gap_ups)}):</b>\n"
            for g in gap_ups[:5]:
                msg += f"  • {g['symbol']}: +{g['gap_pct']:.1f}% gap\n"

        if gap_downs:
            msg += f"\n📉 <b>Gap DOWN ({len(gap_downs)}):</b>\n"
            for g in gap_downs[:5]:
                msg += f"  • {g['symbol']}: {g['gap_pct']:.1f}% gap\n"

        if intraday:
            msg += f"\n🎯 <b>Top Intraday Setups:</b>\n"
            for s in intraday[:3]:
                msg += (
                    f"  {s['rank']}. {s['symbol']} — "
                    f"Entry ₹{s['entry']:,.0f} T ₹{s['target']:,.0f} SL ₹{s['sl']:,.0f}\n"
                    f"     {s['strategy']} | Conf: {s['conf']*100:.0f}%\n"
                )

        msg += f"\n⏰ Pre-close check at 3:00 PM"
        return self.send(msg)

    def send_preclose_alert(self, alerts: list) -> bool:
        """3:00 PM pre-close intraday position alerts."""
        msg = f"⚡ <b>Pre-Close Review — 3:00 PM</b>\n━━━━━━━━━━━━━━━━━━━━\n"

        for alert in alerts:
            symbol   = alert.get("symbol")
            action   = alert.get("action", "REVIEW")
            pnl_pct  = alert.get("pnl_pct", 0)
            new_sl   = alert.get("new_sl")
            reason   = alert.get("reason", "")

            pnl_emoji = "✅" if pnl_pct >= 0 else "❌"
            msg += (
                f"\n{pnl_emoji} <b>{symbol}</b>: {action}\n"
                f"   P&L: {pnl_pct:+.1f}%"
            )
            if new_sl:
                msg += f" | New SL: ₹{new_sl:,.2f}"
            if reason:
                msg += f"\n   Note: {reason}"
            msg += "\n"

        msg += f"\n⚠️ All intraday positions exit by 3:15 PM"
        return self.send(msg)

    def send_drawdown_alert(self, alert: dict) -> bool:
        """10% drawdown alert for long-term holdings."""
        symbol     = alert.get("symbol")
        avg_cost   = alert.get("avg_cost", 0)
        current    = alert.get("current_price", 0)
        drawdown   = alert.get("drawdown_pct", 0)
        rec        = alert.get("recommendation", "HOLD")
        rec_reason = alert.get("reason", "")

        rec_emoji = {"HOLD": "⏳", "AVERAGE_DOWN": "⬇️", "EXIT": "🚨"}.get(rec, "⚠️")

        msg = (
            f"🚨 <b>DRAWDOWN ALERT: {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📉 Drawdown: {drawdown:.1f}%\n"
            f"💰 Avg Cost:  ₹{avg_cost:,.2f}\n"
            f"📊 Current:   ₹{current:,.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{rec_emoji} <b>ML Recommendation: {rec}</b>\n"
            f"📝 {rec_reason}\n"
            f"⏰ {datetime.now().strftime('%d-%b %H:%M')}"
        )
        return self.send(msg)

    def send_exit_suggestion(self, suggestion: dict) -> bool:
        """Portfolio full — exit candidate suggestion."""
        symbol  = suggestion.get("symbol")
        reason  = suggestion.get("reason", "")
        new_sym = suggestion.get("new_signal_symbol", "")
        tf      = suggestion.get("timeframe", "")

        msg = (
            f"🔄 <b>Portfolio Full — Exit Suggestion</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 New Signal: <b>{new_sym}</b> [{tf.upper()}]\n\n"
            f"📤 Suggested Exit: <b>{symbol}</b>\n"
            f"📝 {reason}\n\n"
            f"💡 See dashboard for all 3 exit perspectives"
        )
        return self.send(msg)

    def send_error_alert(self, job_name: str, error: str) -> bool:
        """Scheduler job failure alert."""
        msg = (
            f"⚠️ <b>AlphaLens Error</b>\n"
            f"Job: {job_name}\n"
            f"Error: {error[:200]}\n"
            f"⏰ {datetime.now().strftime('%d-%b %H:%M')}"
        )
        return self.send(msg)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _format_sector_summary(self, sector_cycles: dict) -> str:
        if not sector_cycles:
            return ""
        bull = [s for s, v in sector_cycles.items() if v.get("cycle") == "bull"]
        bear = [s for s, v in sector_cycles.items() if v.get("cycle") == "bear"]
        neut = [s for s, v in sector_cycles.items() if v.get("cycle") == "neutral"]

        lines = []
        if bull:
            lines.append(f"📈 <b>Bull:</b> {', '.join(bull[:4])}")
        if bear:
            lines.append(f"📉 <b>Bear:</b> {', '.join(bear[:4])}")
        if neut:
            lines.append(f"➡️ <b>Neutral:</b> {', '.join(neut[:3])}")
        return "\n".join(lines)

    @staticmethod
    def _split_message(text: str, max_len: int) -> list:
        """Split a long message into chunks."""
        if len(text) <= max_len:
            return [text]
        lines  = text.split("\n")
        chunks = []
        current = ""
        for line in lines:
            if len(current) + len(line) + 1 <= max_len:
                current += line + "\n"
            else:
                if current:
                    chunks.append(current.strip())
                current = line + "\n"
        if current:
            chunks.append(current.strip())
        return chunks
