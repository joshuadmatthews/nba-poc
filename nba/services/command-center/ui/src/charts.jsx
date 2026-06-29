// Dependency-free SVG charts for the Command Center. Tuned for the dark enterprise theme.
import React from 'react';

const fmt = (n) => (n == null ? '—' : Intl.NumberFormat().format(n));

// Vertical conversion funnel with stage counts + drop-off %.
export function Funnel({ stages, accent = '#6ea8fe' }) {
  const max = Math.max(1, ...stages.map((s) => s.count));
  return (
    <div className="funnel">
      {stages.map((s, i) => {
        const w = Math.max(6, (s.count / max) * 100);
        const prev = i > 0 ? stages[i - 1].count : s.count;
        const conv = prev ? Math.round((s.count / prev) * 100) : 100;
        return (
          <div className="funnel-row" key={i}>
            <div className="funnel-label">{s.label}</div>
            <div className="funnel-bar-wrap">
              <div className="funnel-bar" style={{ width: w + '%', background: `linear-gradient(90deg, ${accent}, ${accent}88)` }}>
                <span className="funnel-count">{fmt(s.count)}</span>
              </div>
            </div>
            <div className={'funnel-conv' + (conv < 100 ? ' drop' : '')}>{i === 0 ? '' : conv + '%'}</div>
          </div>
        );
      })}
    </div>
  );
}

// Horizontal labelled bars (e.g. action performance, score distribution).
export function BarList({ items, accent = '#6ea8fe', valueKey = 'n', labelKey = 'label', max }) {
  const m = max || Math.max(1, ...items.map((d) => Number(d[valueKey]) || 0));
  return (
    <div className="barlist">
      {items.map((d, i) => (
        <div className="bar-row" key={i}>
          <div className="bar-label" title={d[labelKey]}>{d[labelKey]}</div>
          <div className="bar-track">
            <div className="bar-fill" style={{ width: ((Number(d[valueKey]) || 0) / m) * 100 + '%', background: accent }} />
          </div>
          <div className="bar-val">{fmt(Number(d[valueKey]) || 0)}</div>
        </div>
      ))}
      {!items.length && <div className="muted">no data</div>}
    </div>
  );
}

// Donut for categorical breakdown (dispositions).
export function Donut({ data, colors = ['#4ade80', '#fbbf24', '#f87171', '#6ea8fe', '#c084fc'] }) {
  const total = data.reduce((a, d) => a + (Number(d.n) || 0), 0) || 1;
  let acc = 0; const R = 52, C = 2 * Math.PI * R;
  return (
    <div className="donut">
      <svg viewBox="0 0 140 140" width="140" height="140">
        <circle cx="70" cy="70" r={R} fill="none" stroke="#1e2430" strokeWidth="16" />
        {data.map((d, i) => {
          const frac = (Number(d.n) || 0) / total;
          const dash = `${frac * C} ${C}`;
          const el = <circle key={i} cx="70" cy="70" r={R} fill="none" stroke={colors[i % colors.length]}
            strokeWidth="16" strokeDasharray={dash} strokeDashoffset={-acc * C} transform="rotate(-90 70 70)" />;
          acc += frac; return el;
        })}
        <text x="70" y="66" textAnchor="middle" className="donut-num">{fmt(total)}</text>
        <text x="70" y="84" textAnchor="middle" className="donut-lab">total</text>
      </svg>
      <div className="donut-legend">
        {data.map((d, i) => (
          <div className="leg" key={i}><span className="dot" style={{ background: colors[i % colors.length] }} />{d.bucket} <b>{fmt(Number(d.n) || 0)}</b></div>
        ))}
      </div>
    </div>
  );
}

export function Kpi({ label, value, sub, accent }) {
  return (
    <div className="kpi">
      <div className="kpi-val" style={accent ? { color: accent } : null}>{fmt(value)}</div>
      <div className="kpi-label">{label}</div>
      {sub != null && <div className="kpi-sub">{sub}</div>}
    </div>
  );
}
