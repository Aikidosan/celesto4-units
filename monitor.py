#!/usr/bin/env python3
"""
Celesto 4 Unit Status Monitor
Fetches Google Sheet via Sheets API v4, detects sold/available status changes
by cell background color, and sends email notifications via Resend API.
"""

import json
import os
import re
import sys
import requests
from datetime import datetime, timezone

# ─── Configuration ────────────────────────────────────────────────────────────
SHEET_ID       = '1q98jo63wFwgFxJTb_0adzKDHOV3U7np_tdydqBaA0nw'
SHEETS_API_KEY = 'AIzaSyC-NxmKd47OR-MZoWRYDkswR114J8VPTWQ'
RESEND_API_KEY = 're_R8RxkMVo_LtA4jUG4Y1oesVZWtbqcEJ3c'
EMAIL_FROM     = 'Celesto 4 Monitor <monitor@mydubai.io>'
EMAIL_TO       = ['a.mitiushkin@gmail.com', 'ariel@mydubai.io']

STATE_FILE       = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state.json')
SOLD_STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sold_status.json')

# Color thresholds (RGB 0–1 scale)
# Gray  ≈ (0.949, 0.949, 0.949) → SOLD
SOLD_GRAY_VALUE = 0.9490196
GRAY_THRESHOLD  = 0.015

# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_sold_color(bg: dict) -> bool:
    """Return True if the background color matches the 'sold' gray."""
    r = bg.get('red',   1.0)
    g = bg.get('green', 1.0)
    b = bg.get('blue',  1.0)
    return (
        abs(r - SOLD_GRAY_VALUE) < GRAY_THRESHOLD and
        abs(g - SOLD_GRAY_VALUE) < GRAY_THRESHOLD and
        abs(b - SOLD_GRAY_VALUE) < GRAY_THRESHOLD
    )

def parse_price(val: str) -> int:
    """Convert a formatted price string like '1,150,000' to an integer."""
    if not val:
        return 0
    try:
        return int(val.replace(',', '').replace(' ', '').split('.')[0])
    except ValueError:
        return 0

# ─── Fetch sheet data ─────────────────────────────────────────────────────────

def fetch_sheet() -> list:
    """
    Fetch all rows from Sheet1 using the Sheets API v4 with includeGridData.
    Returns a list of unit dicts: {key, floor, unit_no, unit_type, area_sqft, price, sold}

    Sheet structure:
      Row 1: Title row ("celesto 4")
      Row 2: Header row ("Unit No.", "Unit Type", "Unit Area (Sqft)", "price")
      Row 3+: Data rows. Col A may contain "Floor N" label on the first unit row of each floor,
              OR a separate blank separator row precedes the floor label.
              Col B = unit number (integer), Col C = unit type, Col D = area sqft, Col E = price.
    """
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
    params = {
        'includeGridData': 'true',
        'ranges': 'Sheet1',
        'key': SHEETS_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = data['sheets'][0]['data'][0].get('rowData', [])
    units = []
    current_floor = None

    for row in rows:
        cells = row.get('values', [])
        if not cells:
            continue

        def get_val(i):
            if i < len(cells):
                return (cells[i].get('formattedValue') or '').strip()
            return ''

        def get_bg(i):
            if i < len(cells):
                return cells[i].get('effectiveFormat', {}).get('backgroundColor', {})
            return {}

        col_a = get_val(0)   # may contain "Floor N" label
        col_b = get_val(1)   # unit number
        col_c = get_val(2)   # unit type
        col_d = get_val(3)   # area sqft
        col_e = get_val(4)   # price

        # Detect floor label in col A (may co-exist with unit data on same row)
        if col_a and 'floor' in col_a.lower():
            m = re.search(r'\d+', col_a)
            if m:
                current_floor = int(m.group())
            # Do NOT continue — the same row may also contain unit data

        # Skip header rows and empty rows
        if not col_b:
            continue

        # col_b must be a numeric unit number
        try:
            unit_no = int(float(col_b.replace(',', '')))
        except (ValueError, AttributeError):
            continue

        # Skip if no floor context yet
        if current_floor is None:
            continue

        # Skip if no unit type (likely a header or separator row)
        unit_type = col_c.strip() if col_c else ''
        if not unit_type or unit_type.lower() in ('unit type', 'type'):
            continue

        area_sqft = 0.0
        try:
            area_sqft = float(col_d.replace(',', '')) if col_d else 0.0
        except ValueError:
            pass

        price = parse_price(col_e)

        # Determine sold status from background color of col B (unit number cell)
        bg = get_bg(1)
        sold = is_sold_color(bg)

        key = f"{current_floor}-{unit_no}"
        units.append({
            'key':       key,
            'floor':     current_floor,
            'unit_no':   unit_no,
            'unit_type': unit_type,
            'area_sqft': area_sqft,
            'price':     price,
            'sold':      sold,
        })

    return units

# ─── State management ─────────────────────────────────────────────────────────

def load_state() -> dict:
    """Load previous state from state.json. Returns {} if not found."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(units: list):
    """Save current unit sold-status map to state.json."""
    state = {u['key']: u['sold'] for u in units}
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    print(f"[monitor] State saved to {STATE_FILE} ({len(state)} units)")

def update_sold_status_json(units: list):
    """Update sold_status.json used by the web frontend."""
    sold_map = {u['key']: u['sold'] for u in units}
    with open(SOLD_STATUS_FILE, 'w') as f:
        json.dump(sold_map, f, separators=(',', ':'))
    print(f"[monitor] sold_status.json updated ({len(sold_map)} units)")

# ─── Change detection ─────────────────────────────────────────────────────────

def detect_changes(units: list, prev_state: dict) -> tuple:
    """
    Compare current units against previous state.
    Returns (newly_sold, newly_available) — each a list of unit dicts.
    """
    newly_sold      = []
    newly_available = []

    for u in units:
        key = u['key']
        prev_sold = prev_state.get(key)

        if prev_sold is None:
            # Unit not in previous state — skip (first run or new unit)
            continue

        if not prev_sold and u['sold']:
            newly_sold.append(u)
        elif prev_sold and not u['sold']:
            newly_available.append(u)

    return newly_sold, newly_available

# ─── Email notification ───────────────────────────────────────────────────────

def format_unit(u: dict) -> str:
    price_str = f"AED {u['price']:,}" if u['price'] else 'N/A'
    return (
        f"Floor {u['floor']} · Unit {u['unit_no']} · {u['unit_type']} · "
        f"{u['area_sqft']} sqft · {price_str}"
    )

def build_email_html(newly_sold: list, newly_available: list, run_time: str) -> str:
    def make_rows(units):
        return ''.join(
            f"<tr>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee'>Floor {u['floor']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee'>Unit {u['unit_no']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee'>{u['unit_type']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee'>{u['area_sqft']} sqft</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee'>AED {u['price']:,}</td>"
            f"</tr>"
            for u in units
        )

    table_style = (
        "border-collapse:collapse;width:100%;font-family:Arial,sans-serif;"
        "font-size:14px;margin-bottom:24px"
    )
    th_style = (
        "background:#f0f0f0;padding:8px 12px;text-align:left;"
        "border-bottom:2px solid #ccc;font-weight:600"
    )
    headers = (
        f"<th style='{th_style}'>Floor</th>"
        f"<th style='{th_style}'>Unit #</th>"
        f"<th style='{th_style}'>Type</th>"
        f"<th style='{th_style}'>Area</th>"
        f"<th style='{th_style}'>Price</th>"
    )

    sold_section = ''
    if newly_sold:
        n = len(newly_sold)
        sold_section = f"""
        <h2 style="color:#8b2323;margin-top:24px">&#128308; Newly Sold ({n} unit{'s' if n != 1 else ''})</h2>
        <table style="{table_style}">
          <thead><tr>{headers}</tr></thead>
          <tbody>{make_rows(newly_sold)}</tbody>
        </table>"""

    avail_section = ''
    if newly_available:
        n = len(newly_available)
        avail_section = f"""
        <h2 style="color:#065f46;margin-top:24px">&#128994; Newly Available ({n} unit{'s' if n != 1 else ''})</h2>
        <table style="{table_style}">
          <thead><tr>{headers}</tr></thead>
          <tbody>{make_rows(newly_available)}</tbody>
        </table>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:auto;padding:24px">
      <h1 style="color:#01696f;border-bottom:2px solid #01696f;padding-bottom:8px">
        Celesto 4 &mdash; Unit Status Update
      </h1>
      <p style="color:#555;font-size:13px">Detected at {run_time} UTC</p>
      {sold_section}
      {avail_section}
      <hr style="margin-top:32px;border:none;border-top:1px solid #ddd">
      <p style="color:#999;font-size:12px">
        This is an automated notification from the Celesto 4 unit monitor.<br>
        Source: Google Sheet ID {SHEET_ID}
      </p>
    </body></html>
    """

def build_email_text(newly_sold: list, newly_available: list, run_time: str) -> str:
    lines = [f"Celesto 4 — Unit Status Update ({run_time} UTC)", ""]
    if newly_sold:
        n = len(newly_sold)
        lines.append(f"NEWLY SOLD ({n} unit{'s' if n != 1 else ''}):")
        for u in newly_sold:
            lines.append(f"  - {format_unit(u)}")
        lines.append("")
    if newly_available:
        n = len(newly_available)
        lines.append(f"NEWLY AVAILABLE ({n} unit{'s' if n != 1 else ''}):")
        for u in newly_available:
            lines.append(f"  - {format_unit(u)}")
        lines.append("")
    lines.append("---")
    lines.append("Automated notification from Celesto 4 unit monitor.")
    return "\n".join(lines)

def send_email(newly_sold: list, newly_available: list):
    """Send email notification via Resend API."""
    run_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')

    subject_parts = []
    if newly_sold:
        n = len(newly_sold)
        subject_parts.append(f"{n} unit{'s' if n != 1 else ''} sold")
    if newly_available:
        n = len(newly_available)
        subject_parts.append(f"{n} unit{'s' if n != 1 else ''} now available")
    subject = f"Celesto 4 Update: {', '.join(subject_parts)}"

    html_body = build_email_html(newly_sold, newly_available, run_time)
    text_body = build_email_text(newly_sold, newly_available, run_time)

    payload = {
        'from':    EMAIL_FROM,
        'to':      EMAIL_TO,
        'subject': subject,
        'html':    html_body,
        'text':    text_body,
    }

    resp = requests.post(
        'https://api.resend.com/emails',
        headers={
            'Authorization': f'Bearer {RESEND_API_KEY}',
            'Content-Type':  'application/json',
        },
        json=payload,
        timeout=15,
    )

    if resp.status_code in (200, 201):
        print(f"[monitor] Email sent successfully (id={resp.json().get('id')})")
        return True
    else:
        print(f"[monitor] Email send FAILED: {resp.status_code} — {resp.text}")
        return False

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[monitor] Starting Celesto 4 monitor — {datetime.now(timezone.utc).isoformat()}")

    # 1. Fetch current state from Google Sheet
    print("[monitor] Fetching sheet data…")
    try:
        units = fetch_sheet()
    except Exception as e:
        print(f"[monitor] ERROR fetching sheet: {e}")
        sys.exit(1)

    print(f"[monitor] Fetched {len(units)} units from sheet")
    if not units:
        print("[monitor] WARNING: No units parsed — aborting without saving state.")
        sys.exit(1)

    # Count sold/available
    sold_count  = sum(1 for u in units if u['sold'])
    avail_count = len(units) - sold_count
    print(f"[monitor] Current status: {sold_count} sold, {avail_count} available")

    # 2. Load previous state
    prev_state = load_state()
    is_first_run = len(prev_state) == 0
    if is_first_run:
        print("[monitor] No previous state found — this is the first run. Saving baseline state.")

    # 3. Detect changes
    newly_sold, newly_available = detect_changes(units, prev_state)

    if newly_sold:
        print(f"[monitor] Newly SOLD ({len(newly_sold)}):")
        for u in newly_sold:
            print(f"  - {format_unit(u)}")
    if newly_available:
        print(f"[monitor] Newly AVAILABLE ({len(newly_available)}):")
        for u in newly_available:
            print(f"  - {format_unit(u)}")

    # 4. Send email if changes detected (and not first run)
    if (newly_sold or newly_available) and not is_first_run:
        print("[monitor] Changes detected — sending email notification…")
        send_email(newly_sold, newly_available)
    elif is_first_run:
        print("[monitor] First run — baseline state saved, no email sent.")
    else:
        print("[monitor] No changes detected — no email sent.")

    # 5. Save updated state
    save_state(units)

    # 6. Update sold_status.json for the web frontend
    update_sold_status_json(units)

    print(f"[monitor] Done — {datetime.now(timezone.utc).isoformat()}")

if __name__ == '__main__':
    main()
