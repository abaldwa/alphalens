"""
alphalens/core/notifications/email.py

SMTP email notification engine.

Sends:
  - EOD summary report (daily)
  - Monthly investment portfolio review (rich HTML email)
  - Monthly strategy discovery report
  - Drawdown alert (investment holdings)
  - Signal digest (weekly)

Email format:
  - Trading signals via Telegram (compact/fast)
  - Investment reports via Email (detailed/rich)
  - HTML emails with tables and colour coding

Setup:
  1. Use a Gmail App Password (not account password)
     → Google Account → Security → 2FA → App Passwords
  2. Set EMAIL_ADDRESS, EMAIL_PASSWORD, EMAIL_TO in .env
"""

import smtplib
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from loguru import logger

from config.settings import settings


class EmailNotifier:

    def __init__(self):
        self.smtp_host = settings.email_smtp_host
        self.smtp_port = settings.email_smtp_port
        self.address   = settings.email_address
        self.password  = settings.email_password
        self.to_email  = settings.email_to

    def send(self, subject: str, html_body: str,
             text_body: Optional[str] = None) -> bool:
        """Send an HTML email. Returns True on success."""
        if not all([self.address, self.password, self.to_email]):
            logger.debug("Email not configured — skipping")
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = f"AlphaLens <{self.address}>"
            msg["To"]      = self.to_email

            if text_body:
                msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.login(self.address, self.password)
                server.sendmail(self.address, self.to_email, msg.as_string())

            logger.debug(f"Email sent: {subject}")
            return True

        except Exception as e:
            logger.warning(f"Email send failed: {e}")
            return False

    # ── Message Templates ─────────────────────────────────────────────────

    def send_eod_report(self, cycle_ctx, signals: list) -> bool:
        """Daily 6:30 PM EOD email summary."""
        market = cycle_ctx.market_cycle.upper()
        today  = date.today().strftime("%d %b %Y")
        conf   = cycle_ctx.market_confidence

        buy_signals  = [s for s in signals if s.signal_type == "buy"]
        sell_signals = [s for s in signals if s.signal_type == "sell"]

        cycle_color = {"BULL": "#1a4d2e", "BEAR": "#8b1a1a", "NEUTRAL": "#5a5a5a"}.get(market, "#333")

        signal_rows = ""
        for s in buy_signals[:10]:
            pnl_color = "#1a4d2e"
            signal_rows += f"""
            <tr>
                <td style="padding:6px 10px;font-weight:bold">{s.symbol}</td>
                <td style="padding:6px 10px;color:{pnl_color};font-weight:bold">BUY</td>
                <td style="padding:6px 10px">{s.timeframe.upper().replace('_','-')}</td>
                <td style="padding:6px 10px">₹{s.entry_price:,.2f}</td>
                <td style="padding:6px 10px">₹{s.target_price:,.2f}</td>
                <td style="padding:6px 10px">₹{s.stop_loss:,.2f}</td>
                <td style="padding:6px 10px">{(s.risk_reward or 0):.1f}x</td>
                <td style="padding:6px 10px">{(s.confidence or 0)*100:.0f}%</td>
            </tr>
            """

        sector_rows = ""
        for sector, info in cycle_ctx.sector_cycles.items():
            sc = info.get("cycle", "neutral").upper()
            sc_color = {"BULL": "#1a4d2e", "BEAR": "#8b1a1a", "NEUTRAL": "#5a5a5a"}.get(sc, "#333")
            sector_rows += f"""
            <tr>
                <td style="padding:4px 10px">{sector}</td>
                <td style="padding:4px 10px;color:{sc_color};font-weight:bold">{sc}</td>
                <td style="padding:4px 10px">{info.get('confidence', 0)*100:.0f}%</td>
            </tr>
            """

        html = f"""
        <!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">
        <div style="max-width:700px;margin:0 auto;background:white;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)">

            <div style="background:#0d0d0d;padding:20px 24px">
                <h1 style="color:#b8860b;margin:0;font-size:1.4rem">◈ AlphaLens EOD Report</h1>
                <p style="color:#888;margin:4px 0 0">{today}</p>
            </div>

            <div style="padding:20px 24px;background:#f9f9f9;border-bottom:1px solid #eee">
                <div style="display:flex;align-items:center;gap:12px">
                    <div style="background:{cycle_color};color:white;padding:8px 18px;border-radius:4px;font-size:1.2rem;font-weight:bold">
                        {market}
                    </div>
                    <div style="color:#666">Market Confidence: <b>{conf*100:.0f}%</b></div>
                    <div style="color:#666">New Signals: <b style="color:#1a4d2e">{len(buy_signals)} BUY</b> · <b style="color:#8b1a1a">{len(sell_signals)} SELL</b></div>
                </div>
            </div>

            <div style="padding:20px 24px">
                <h2 style="font-size:1rem;color:#333;margin-top:0">Sector Cycles</h2>
                <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
                    <thead><tr style="background:#f0f0f0">
                        <th style="padding:6px 10px;text-align:left">Sector</th>
                        <th style="padding:6px 10px;text-align:left">Cycle</th>
                        <th style="padding:6px 10px;text-align:left">Confidence</th>
                    </tr></thead>
                    <tbody>{sector_rows}</tbody>
                </table>
            </div>

            <div style="padding:20px 24px;border-top:1px solid #eee">
                <h2 style="font-size:1rem;color:#333;margin-top:0">New Buy Signals</h2>
                <table style="width:100%;border-collapse:collapse;font-size:0.82rem">
                    <thead><tr style="background:#f0f0f0">
                        <th style="padding:6px 10px;text-align:left">Symbol</th>
                        <th style="padding:6px 10px;text-align:left">Type</th>
                        <th style="padding:6px 10px;text-align:left">TF</th>
                        <th style="padding:6px 10px;text-align:right">Entry</th>
                        <th style="padding:6px 10px;text-align:right">Target</th>
                        <th style="padding:6px 10px;text-align:right">SL</th>
                        <th style="padding:6px 10px;text-align:right">R:R</th>
                        <th style="padding:6px 10px;text-align:right">Conf</th>
                    </tr></thead>
                    <tbody>{signal_rows}</tbody>
                </table>
            </div>

            <div style="background:#0d0d0d;padding:12px 24px;text-align:center">
                <p style="color:#666;margin:0;font-size:0.8rem">
                    AlphaLens · Nifty200 Intelligence Platform · Next review: 9:30 AM
                </p>
            </div>
        </div>
        </body></html>
        """
        return self.send(f"AlphaLens EOD — {today} | Market: {market}", html)

    def send_monthly_investment_report(self, report: dict) -> bool:
        """Detailed monthly investment portfolio review email."""
        today    = date.today().strftime("%B %Y")
        holdings = report.get("holdings", [])

        rows = ""
        for h in holdings:
            pnl       = h.get("pnl", 0)
            pnl_pct   = h.get("pnl_pct", 0)
            rec       = h.get("recommendation", "HOLD")
            pnl_color = "#1a4d2e" if pnl >= 0 else "#8b1a1a"
            rec_color = {"HOLD": "#5a5a5a", "AVERAGE_DOWN": "#b8860b", "EXIT": "#8b1a1a"}.get(rec, "#333")

            rows += f"""
            <tr style="border-bottom:1px solid #eee">
                <td style="padding:8px 10px;font-weight:bold">{h.get('symbol')}</td>
                <td style="padding:8px 10px">{h.get('qty')}</td>
                <td style="padding:8px 10px">₹{h.get('avg_cost', 0):,.2f}</td>
                <td style="padding:8px 10px">₹{h.get('current_price', 0):,.2f}</td>
                <td style="padding:8px 10px;color:{pnl_color};font-weight:bold">
                    ₹{pnl:,.0f} ({pnl_pct:+.1f}%)
                </td>
                <td style="padding:8px 10px">₹{h.get('target', 0):,.2f}</td>
                <td style="padding:8px 10px">₹{h.get('stop_loss', 0):,.2f}</td>
                <td style="padding:8px 10px;color:{rec_color};font-weight:bold">{rec}</td>
                <td style="padding:8px 10px;font-size:0.75rem;color:#666">{h.get('reason', '')[:60]}</td>
            </tr>
            """

        total_invested = sum(h.get('avg_cost', 0) * h.get('qty', 0) for h in holdings)
        total_pnl      = sum(h.get('pnl', 0) for h in holdings)
        total_pnl_pct  = (total_pnl / total_invested * 100) if total_invested > 0 else 0
        pnl_color      = "#1a4d2e" if total_pnl >= 0 else "#8b1a1a"

        html = f"""
        <!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">
        <div style="max-width:900px;margin:0 auto;background:white;border-radius:8px;overflow:hidden">

            <div style="background:#0d0d0d;padding:20px 24px">
                <h1 style="color:#b8860b;margin:0">◈ Monthly Investment Review — {today}</h1>
            </div>

            <div style="padding:16px 24px;background:#f9f9f9;border-bottom:1px solid #eee;display:flex;gap:32px">
                <div><div style="font-size:0.75rem;color:#888">TOTAL INVESTED</div>
                     <div style="font-size:1.3rem;font-weight:bold">₹{total_invested:,.0f}</div></div>
                <div><div style="font-size:0.75rem;color:#888">TOTAL P&L</div>
                     <div style="font-size:1.3rem;font-weight:bold;color:{pnl_color}">
                     ₹{total_pnl:,.0f} ({total_pnl_pct:+.1f}%)</div></div>
                <div><div style="font-size:0.75rem;color:#888">HOLDINGS</div>
                     <div style="font-size:1.3rem;font-weight:bold">{len(holdings)}</div></div>
            </div>

            <div style="padding:20px 24px;overflow-x:auto">
                <table style="width:100%;border-collapse:collapse;font-size:0.83rem;min-width:800px">
                    <thead><tr style="background:#f0f0f0">
                        <th style="padding:8px 10px;text-align:left">Symbol</th>
                        <th style="padding:8px 10px;text-align:left">Qty</th>
                        <th style="padding:8px 10px;text-align:left">Avg Cost</th>
                        <th style="padding:8px 10px;text-align:left">Current</th>
                        <th style="padding:8px 10px;text-align:left">P&L</th>
                        <th style="padding:8px 10px;text-align:left">Target</th>
                        <th style="padding:8px 10px;text-align:left">Stop Loss</th>
                        <th style="padding:8px 10px;text-align:left">Recommendation</th>
                        <th style="padding:8px 10px;text-align:left">Reason</th>
                    </tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>

            <div style="background:#0d0d0d;padding:12px 24px;text-align:center">
                <p style="color:#666;margin:0;font-size:0.8rem">AlphaLens · Monthly Investment Review</p>
            </div>
        </div></body></html>
        """
        return self.send(f"AlphaLens Monthly Review — {today}", html)

    def send_drawdown_alert(self, alert: dict) -> bool:
        """Drawdown alert email with full ML recommendation detail."""
        symbol   = alert.get("symbol")
        dd_pct   = alert.get("drawdown_pct", 0)
        rec      = alert.get("recommendation", "HOLD")
        reason   = alert.get("reason", "")
        avg_cost = alert.get("avg_cost", 0)
        current  = alert.get("current_price", 0)

        rec_color = {"HOLD": "#5a5a5a", "AVERAGE_DOWN": "#b8860b", "EXIT": "#8b1a1a"}.get(rec, "#333")

        html = f"""
        <!DOCTYPE html><html><body style="font-family:Arial,sans-serif;padding:20px">
        <div style="max-width:500px;margin:0 auto;background:white;border-radius:8px;border:2px solid #8b1a1a">
            <div style="background:#8b1a1a;padding:16px 20px">
                <h2 style="color:white;margin:0">🚨 Drawdown Alert: {symbol}</h2>
            </div>
            <div style="padding:20px">
                <table style="width:100%;font-size:0.9rem">
                    <tr><td style="color:#666;padding:4px 0">Avg Cost:</td>
                        <td style="font-weight:bold">₹{avg_cost:,.2f}</td></tr>
                    <tr><td style="color:#666;padding:4px 0">Current Price:</td>
                        <td style="font-weight:bold">₹{current:,.2f}</td></tr>
                    <tr><td style="color:#666;padding:4px 0">Drawdown:</td>
                        <td style="font-weight:bold;color:#8b1a1a">{dd_pct:.1f}%</td></tr>
                </table>
                <div style="margin-top:16px;padding:12px;background:#f9f9f9;border-radius:4px;border-left:4px solid {rec_color}">
                    <div style="font-size:0.8rem;color:#888">ML RECOMMENDATION</div>
                    <div style="font-size:1.2rem;font-weight:bold;color:{rec_color}">{rec}</div>
                    <div style="margin-top:8px;font-size:0.85rem;color:#444">{reason}</div>
                </div>
                <p style="text-align:center;margin-top:16px">
                    <a href="http://localhost:8050" style="background:#0d0d0d;color:white;padding:10px 24px;border-radius:4px;text-decoration:none">
                        Open Dashboard
                    </a>
                </p>
            </div>
        </div></body></html>
        """
        return self.send(f"🚨 Drawdown Alert: {symbol} ({dd_pct:.1f}%)", html)

    def send_strategy_discovery_report(self, stats: dict) -> bool:
        """Monthly strategy discovery results email."""
        month    = datetime.now().strftime("%B %Y")
        new_strats = stats.get("new_strategies", [])
        total_tested = stats.get("total_tested", 0)
        duration = stats.get("duration_minutes", 0)

        rows = ""
        for s in new_strats:
            rows += f"""
            <tr style="border-bottom:1px solid #eee">
                <td style="padding:6px 10px;font-weight:bold">{s.get('name')}</td>
                <td style="padding:6px 10px">{s.get('type')}</td>
                <td style="padding:6px 10px">{s.get('best_cycles', [])}</td>
                <td style="padding:6px 10px">{s.get('sharpe', 0):.2f}</td>
                <td style="padding:6px 10px">{s.get('win_rate', 0)*100:.1f}%</td>
                <td style="padding:6px 10px">{s.get('timeframes', [])}</td>
            </tr>
            """

        html = f"""
        <!DOCTYPE html><html><body style="font-family:Arial,sans-serif;padding:20px;background:#f5f5f5">
        <div style="max-width:700px;margin:0 auto;background:white;border-radius:8px">
            <div style="background:#0d0d0d;padding:20px">
                <h1 style="color:#b8860b;margin:0">◈ Strategy Discovery Report — {month}</h1>
            </div>
            <div style="padding:16px 24px;background:#f9f9f9;border-bottom:1px solid #eee">
                <b>{total_tested}</b> strategies tested · <b>{len(new_strats)}</b> new strategies added ·
                Runtime: <b>{duration:.0f} min</b>
            </div>
            <div style="padding:20px 24px">
                <table style="width:100%;border-collapse:collapse;font-size:0.83rem">
                    <thead><tr style="background:#f0f0f0">
                        <th style="padding:6px 10px;text-align:left">Strategy Name</th>
                        <th style="padding:6px 10px;text-align:left">Type</th>
                        <th style="padding:6px 10px;text-align:left">Best Cycle</th>
                        <th style="padding:6px 10px;text-align:left">Sharpe</th>
                        <th style="padding:6px 10px;text-align:left">Win Rate</th>
                        <th style="padding:6px 10px;text-align:left">Timeframes</th>
                    </tr></thead>
                    <tbody>{rows if rows else "<tr><td colspan='6' style='padding:16px;text-align:center;color:#888'>No new strategies discovered this month</td></tr>"}</tbody>
                </table>
            </div>
        </div></body></html>
        """
        return self.send(f"AlphaLens Strategy Discovery — {month}: {len(new_strats)} new", html)
