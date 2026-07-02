// dashboard/static/js/calendar_picker.js — site-wide trading-calendar
// validation for <input type="date">. Fetches the NSE holiday list once
// (cached module-level), then on `change` flags weekends/holidays via
// setCustomValidity + an inline message near the input (no alert()).
// Usage: CalendarPicker.attach("backdate-input")

const CalendarPicker = (() => {
  let cachedHolidays = null;
  let fetchPromise = null;

  function load() {
    if (cachedHolidays) return Promise.resolve(cachedHolidays);
    if (fetchPromise) return fetchPromise;
    fetchPromise = apiGet("/api/v1/ops/trading-calendar/holidays")
      .then((r) => {
        cachedHolidays = new Set((r && r.holidays) || []);
        return cachedHolidays;
      })
      .catch(() => {
        cachedHolidays = new Set();
        return cachedHolidays;
      });
    return fetchPromise;
  }

  function ensureErrorEl(input) {
    const id = input.id + "-calendar-error";
    let errEl = document.getElementById(id);
    if (!errEl) {
      errEl = document.createElement("div");
      errEl.id = id;
      errEl.className = "field-error";
      errEl.style.color = "var(--red, #d33)";
      errEl.style.fontSize = "12px";
      errEl.style.marginTop = "4px";
      input.insertAdjacentElement("afterend", errEl);
    }
    return errEl;
  }

  function isWeekend(dateStr) {
    // dateStr is "YYYY-MM-DD" — construct as UTC to avoid TZ drift.
    const d = new Date(dateStr + "T00:00:00Z");
    const day = d.getUTCDay();
    return day === 0 || day === 6;
  }

  function validate(input) {
    const errEl = ensureErrorEl(input);
    const val = input.value;
    if (!val) {
      input.setCustomValidity("");
      errEl.textContent = "";
      return;
    }
    let msg = "";
    if (isWeekend(val)) {
      msg = "This date is a weekend — NSE is closed.";
    } else if (cachedHolidays && cachedHolidays.has(val)) {
      msg = "This date is an NSE trading holiday.";
    }
    input.setCustomValidity(msg);
    errEl.textContent = msg;
  }

  function attach(inputId) {
    const input = document.getElementById(inputId);
    if (!input) return;
    load().then(() => validate(input));
    input.addEventListener("change", () => {
      load().then(() => validate(input));
    });
  }

  return { attach, load };
})();
