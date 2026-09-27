# FOMC study: SOXL and MU around Fed decisions

How SOXL and MU trade before, during and after FOMC decisions: 84 scheduled
meetings from Jan 2016 to Jul 2026, with a focus on the 24 since Sep 2023. It
covers the decision (hike/cut/hold), surprise (2-year yield move plus a news
read), technical setup, macro backdrop and MU/NVDA earnings timing. It ends with
scenarios for the 16 Sep 2026 decision.

Self-contained: prices and macro data are fetched from Yahoo and FRED, and
nothing in the app or its DuckDB is touched.

## Run order

```bash
cd study/fomc
uv run python fetch.py                                   # prices_daily.csv, prices_hourly.csv, fred.csv
uv run --with lxml python -c "import fetch; fetch.earnings()"   # earnings_dates.csv (yfinance needs lxml)
uv run python meetings.py      # meetings.csv: dates + move derived from DFEDTARU
uv run python analyze.py       # out/meeting_features.csv, paths, drift, group_stats, factor_corr, intraday_summary
uv run python scenarios.py     # out/current_state.json, analog_hikes, analog_selloff, scenario_buckets
uv run python report.py        # fomc_study.html
```

`labels.csv` is hand-written: pre-meeting pricing, news tone and same-day
events for each meeting since Sep 2023.

Yahoo only serves about 730 days of 60-minute bars, so `prices_hourly.csv`
starts in Oct 2023. Re-fetching later will drop the oldest meetings from the
intraday columns.

## Findings (prices through 14 Sep 2026)

- **Fed days are louder, not directional.** SOXL's 1:30pm-to-close move on
  Fed days had a median of 2.7%, versus 1.2% on other afternoons, and averaged
  about zero. Over the full day, SOXL averaged +1.3% on Fed days since 2016
  (any day: +0.4%), but that beat only 91% of random draws, so it isn't
  conclusive.
- **Moves reverse.** Since Sep 2023, SOXL's Fed-day return had a rank
  correlation of −0.40 with the next 5 days (−0.14 since 2016).
- **Hikes are the one clear split.** After the 19 hikes, SOXL's median next
  21 days was −8.5% (42% higher). After holds it was +7.8%, and after cuts
  +9.6%. MU was far less sensitive (+2.3% after hikes).
- **Market-read tone matters more than the decision label.** When the 2-year
  yield rose 4bp or more (18 meetings), SOXL's median month was −0.6% (44%
  up). When it fell 4bp or more, the median was +7.2%.
- **Weak factor links.** Over ten years, no technical or macro factor on the
  eve of a meeting (RSI, 200-day, VIX, core inflation, real rate,
  unemployment trend) reached a rank correlation of ±0.25 with the next 5 or
  21 days.
- **Entering after a slide:** when SOXL fell 10%+ in the 10 days before a
  meeting (13 cases), the next 5 days had a median of +7.1% (69% up), but the
  next 21 days −5.7%.

Scenario probabilities in the report are judgement, anchored on futures
pricing (a 25bp hike priced at over 90% as of 14 Sep 2026).
