import React, { useEffect, useRef, useState } from 'react';
import { gql, coerce } from './gql.js';

const CMPS = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'exists'];
// Operators valid for a fact's declared type — mirrors the action-library's API gate, so the builder never
// offers an operator the server will reject. Unknown type => all (the API still enforces).
function operatorsFor(valueType) {
  const t = (valueType || '').toUpperCase();
  if (t === 'BOOL' || t === 'BOOLEAN') return ['eq', 'ne', 'exists'];
  if (t === 'STRING') return ['eq', 'ne', 'in', 'exists'];
  if (t === 'LONG' || t === 'DOUBLE' || t === 'INT' || t === 'NUMBER') return ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'exists'];
  return CMPS;
}
const isBoolType = (vt) => ['BOOL', 'BOOLEAN'].includes((vt || '').toUpperCase());
const CHANNELS = ['email', 'sms', 'push', 'website', 'ivr', 'agent_assist'];
// Soft-completion funnel stages per channel (mirrors the rules engine / action-library CHANNEL_FUNNEL).
// The soft bar defaults to the terminal stage; this lets an action override it per channel.
const SOFT_STAGES = { email: ['Delivered', 'Read', 'LinkClicked'], sms: ['Delivered', 'LinkClicked'],
  push: ['Delivered', 'Opened'], voice: ['Answered', 'Completed'], mail: ['Delivered'],
  _default: ['Presented', 'Accepted', 'Completed'] };
const clone = (o) => JSON.parse(JSON.stringify(o));
const emptyTree = () => ({ op: 'all', conditions: [] });

// ---- fact library (every member fact known in gold) — fetched ONCE, shared across every rule builder ----
let _factPromise = null;
export function useFactLibrary() {
  const [facts, setFacts] = useState(_factLib);
  useEffect(() => {
    if (_factLib.length) return;
    _factPromise = _factPromise || gql(`{ factLibrary { key valueType distinctValues count samples } }`).then((d) => d.factLibrary || []);
    let live = true;
    _factPromise.then((f) => { _factLib = f; if (live) setFacts(f); }).catch(() => {});
    return () => { live = false; };
  }, []);
  return facts;
}
let _factLib = [];

// Autocomplete fact picker: type to filter, scrollable dropdown, keyboard nav (↑/↓/Enter/Esc). Free-text
// is allowed too (a fact not yet in gold), so it never blocks authoring a brand-new fact.
function FactInput({ value, onChange, placeholder }) {
  const facts = useFactLibrary();
  const [open, setOpen] = useState(false);
  const [hi, setHi] = useState(0);
  const ref = useRef(null);
  const q = (value || '').toLowerCase();
  const matches = (q ? facts.filter((f) => f.key.toLowerCase().includes(q)) : facts).slice(0, 60);
  useEffect(() => {
    const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', h);
    return () => document.removeEventListener('mousedown', h);
  }, []);
  const pick = (k) => { onChange(k); setOpen(false); };
  return (
    <div className="fi" ref={ref}>
      <input className="mono" placeholder={placeholder} value={value || ''} spellCheck={false}
        onChange={(e) => { onChange(e.target.value); setOpen(true); setHi(0); }}
        onFocus={() => setOpen(true)}
        onKeyDown={(e) => {
          if (!open && (e.key === 'ArrowDown' || e.key === 'ArrowUp')) { setOpen(true); return; }
          if (e.key === 'ArrowDown') { e.preventDefault(); setHi((h) => Math.min(h + 1, matches.length - 1)); }
          else if (e.key === 'ArrowUp') { e.preventDefault(); setHi((h) => Math.max(h - 1, 0)); }
          else if (e.key === 'Enter' && open && matches[hi]) { e.preventDefault(); pick(matches[hi].key); }
          else if (e.key === 'Escape') setOpen(false);
        }} />
      {open && matches.length > 0 && (
        <div className="fi-drop">
          {matches.map((f, i) => (
            <div key={f.key} className={'fi-opt' + (i === hi ? ' hi' : '')}
              onMouseDown={(e) => { e.preventDefault(); pick(f.key); }} onMouseEnter={() => setHi(i)}>
              <span className="fi-key">{f.key}</span>
              <span className="fi-meta">{f.valueType || '?'}{f.distinctValues ? ` · ${f.distinctValues} vals` : ''}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---- condition tree builder ----
export function ConditionBuilder({ tree, onChange, allowEmpty }) {
  const facts = useFactLibrary();
  const typeOf = (key) => (facts.find((f) => f.key === key) || {}).valueType;
  const t = tree && tree.op ? tree : emptyTree();
  const set = (next) => onChange(next);
  const setCond = (i, field, val) => { const n = clone(t); n.conditions[i][field] = field === 'value' ? coerce(val) : val; set(n); };
  // Picking a fact snaps the operator to one valid for its type (and defaults a boolean value to true), so the
  // builder can't author a condition the action-library API would reject.
  const setFact = (i, key) => {
    const n = clone(t); const c = n.conditions[i]; c.fact = key;
    const ops = operatorsFor(typeOf(key));
    if (!ops.includes(c.cmp)) c.cmp = ops[0];
    if (isBoolType(typeOf(key)) && c.cmp !== 'exists') c.value = true;
    set(n);
  };
  const add = () => { const n = clone(t); n.conditions.push({ fact: '', cmp: 'eq', value: '' }); set(n); };
  const del = (i) => { const n = clone(t); n.conditions.splice(i, 1); set(n); };
  return (
    <div className="cb">
      <div className="cb-head">
        <div className="seg">
          {['all', 'any'].map((op) => <button key={op} type="button" className={t.op === op ? 'on' : ''} onClick={() => set({ ...t, op })}>{op === 'all' ? 'ALL of' : 'ANY of'}</button>)}
        </div>
        <button type="button" className="btn ghost sm" onClick={add}>+ condition</button>
      </div>
      {t.conditions.map((c, i) => {
        const vt = typeOf(c.fact); const ops = operatorsFor(vt);
        return (
          <div className="cb-cond" key={i}>
            <FactInput value={c.fact} onChange={(v) => setFact(i, v)} placeholder="fact key, e.g. operator.activity.daysSinceLogin" />
            <select value={c.cmp} onChange={(e) => setCond(i, 'cmp', e.target.value)}>{ops.map((m) => <option key={m}>{m}</option>)}</select>
            {c.cmp !== 'exists' && (isBoolType(vt)
              ? <select className="val" value={String(c.value)} onChange={(e) => setCond(i, 'value', e.target.value)}><option value="true">true</option><option value="false">false</option></select>
              : <input className="val" placeholder="value" value={String(c.value ?? '')} onChange={(e) => setCond(i, 'value', e.target.value)} />)}
            <button type="button" className="btn icon danger" onClick={() => del(i)}>✕</button>
          </div>
        );
      })}
      {!t.conditions.length && <div className="muted sm">{allowEmpty ? 'no conditions (always matches)' : 'add a condition'}</div>}
    </div>
  );
}

function Field({ label, children, hint }) {
  return <label className="field"><span className="field-label">{label}{hint && <i> · {hint}</i>}</span>{children}</label>;
}

// ---- action groups (taxonomy tree) shared helpers ----
// Flatten the group tree into depth-indented <option> rows (for the action editor's group picker).
function groupOptions(groups) {
  const kids = {};
  (groups || []).forEach((g) => { (kids[g.parentId || ''] ||= []).push(g); });
  const out = [];
  const walk = (parent, depth) => (kids[parent] || []).sort((a, b) => a.name.localeCompare(b.name))
    .forEach((g) => { out.push({ id: g.id, label: '  '.repeat(depth) + g.name, depth }); walk(g.id, depth + 1); });
  walk('', 0);
  return out;
}
// Fetch the groups once for a picker.
function useGroupList() {
  const [groups, setGroups] = useState([]);
  useEffect(() => { gql(`{ actionGroups { id name parentId } }`).then((d) => setGroups(d.actionGroups || [])).catch(() => {}); }, []);
  return groups;
}
function useExperienceList() {
  const [exps, setExps] = useState([]);
  useEffect(() => { gql(`{ experiences { id name } }`).then((d) => setExps(d.experiences || [])).catch(() => {}); }, []);
  return exps;
}

// ---- action editor ----
function ActionEditor({ value, onSaved, onCancel }) {
  const [a, setA] = useState(() => {
    const base = value ? clone(value) : { name: '', ttlSeconds: 86400, channels: [{ channel: 'email', contentKey: '' }], inclusion: emptyTree(), exclusion: emptyTree(), groupId: null, experienceId: null };
    base.completion = base.completion || emptyTree();                      // hard-completion goal
    if (base.autoExcludeOnCompletion == null) base.autoExcludeOnCompletion = true;
    base.hardTtlSeconds = base.hardTtlSeconds ?? '';
    return base;
  });
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [softOpen, setSoftOpen] = useState({});                            // per-channel "soft completion" disclosure
  const groups = useGroupList();
  const experiences = useExperienceList();
  const upd = (patch) => setA((x) => ({ ...x, ...patch }));
  const setChan = (i, field, v) => { const c = clone(a.channels); c[i][field] = v; upd({ channels: c }); };
  // content-key VARIANTS (A/B + targeting): each variant has its own key + an optional % split and/or fact rules
  const addVariant = (i) => { const c = clone(a.channels); (c[i].variants = c[i].variants || []).push({ contentKey: '', percent: '', conditions: emptyTree() }); upd({ channels: c }); };
  const setVariant = (ci, vi, field, v) => { const c = clone(a.channels); c[ci].variants[vi][field] = v; upd({ channels: c }); };
  const delVariant = (ci, vi) => { const c = clone(a.channels); c[ci].variants.splice(vi, 1); upd({ channels: c }); };

  async function save() {
    setBusy(true); setErr(null);
    try {
      const input = { id: a.id, name: a.name, ttlSeconds: Number(a.ttlSeconds) || null,
        channels: a.channels.map((c) => ({ channel: c.channel, contentKey: c.contentKey || null,
          softCompletion: (c.softCompletion || '').trim() || null,   // per-channel soft bar override (else funnel terminal)
          // only persist variants that actually name a key; empty % / empty conditions = "no gate" (always)
          variants: (c.variants || []).filter((v) => (v.contentKey || '').trim()).map((v) => ({
            contentKey: v.contentKey.trim(),
            percent: v.percent === '' || v.percent == null ? null : Number(v.percent),
            conditions: v.conditions && v.conditions.conditions?.length ? v.conditions : null,
          })) })),
        inclusion: a.inclusion, exclusion: a.exclusion,
        // hard completion: the goal tree (only if it has conditions), the wait window, and the auto-retire flag
        completion: a.completion && a.completion.conditions?.length ? a.completion : null,
        hardTtlSeconds: a.hardTtlSeconds === '' || a.hardTtlSeconds == null ? null : Number(a.hardTtlSeconds),
        autoExcludeOnCompletion: a.autoExcludeOnCompletion !== false };
      input.groupId = a.groupId || null;          // taxonomy assignment rides the action doc
      input.experienceId = a.experienceId || null; // business-journey taxonomy
      await gql(`mutation($i: ActionInput!){ upsertAction(input: $i){ id } }`, { i: input });
      onSaved();
    } catch (e) { setErr(e.message); } finally { setBusy(false); }
  }

  return (
    <div className="editor">
      <div className="editor-grid">
        <Field label="Action name"><input value={a.name} onChange={(e) => upd({ name: e.target.value })} placeholder="e.g. Reengage Email" /></Field>
        <Field label="Re-send after" hint="seconds · soft cooldown"><input type="number" value={a.ttlSeconds} onChange={(e) => upd({ ttlSeconds: e.target.value })} /></Field>
        <Field label="Group" hint="taxonomy"><select value={a.groupId || ''} onChange={(e) => upd({ groupId: e.target.value || null })}>
          <option value="">— ungrouped —</option>
          {groupOptions(groups).map((o) => <option key={o.id} value={o.id}>{o.label}</option>)}
        </select></Field>
        <Field label="Experience" hint="journey"><select value={a.experienceId || ''} onChange={(e) => upd({ experienceId: e.target.value || null })}>
          <option value="">— none —</option>
          {experiences.map((x) => <option key={x.id} value={x.id}>{x.name}</option>)}
        </select></Field>
      </div>

      <div className="block">
        <div className="block-head"><h4>Channels</h4><button type="button" className="btn ghost sm" onClick={() => upd({ channels: [...a.channels, { channel: 'email', contentKey: '' }] })}>+ channel</button></div>
        {a.channels.map((c, i) => (
          <div className="chan-block" key={i}>
            <div className="chan-row">
              <select value={c.channel} onChange={(e) => setChan(i, 'channel', e.target.value)}>{CHANNELS.map((ch) => <option key={ch}>{ch}</option>)}</select>
              <input className="mono" placeholder="content template key, e.g. tmpl.reengage.v1" value={c.contentKey || ''} onChange={(e) => setChan(i, 'contentKey', e.target.value)} />
              <button type="button" className="btn icon" title="add a content variant (A/B + targeting)" onClick={() => addVariant(i)}>＋</button>
              <button type="button" className="btn icon danger" onClick={() => upd({ channels: a.channels.filter((_, j) => j !== i) })}>✕</button>
            </div>
            {/* Soft completion is the channel's engagement bar — defaults to the funnel terminal, hidden unless opened. */}
            <button type="button" className="btn ghost xs soft-tog" onClick={() => setSoftOpen((s) => ({ ...s, [i]: !s[i] }))}>{softOpen[i] ? '▾' : '▸'} soft completion</button>
            {softOpen[i] && (
              <div className="soft-override"><span className="muted sm">counts as engaged at</span>
                <select value={c.softCompletion || ''} onChange={(e) => setChan(i, 'softCompletion', e.target.value)}>
                  <option value="">— channel default (final stage) —</option>
                  {(SOFT_STAGES[c.channel] || SOFT_STAGES._default).map((st) => <option key={st}>{st}</option>)}
                </select></div>
            )}
            {(c.variants || []).length > 0 && (
              <div className="variants">
                <div className="variants-head">Variants <span className="muted sm">— first match wins; nobody matches → the base key above. (ML-decided variants: coming soon.)</span></div>
                {(c.variants || []).map((v, vi) => (
                  <div className="variant" key={vi}>
                    <div className="variant-row">
                      <input className="mono" placeholder="variant content key, e.g. tmpl.reengage.v2" value={v.contentKey || ''} onChange={(e) => setVariant(i, vi, 'contentKey', e.target.value)} />
                      <span className="pct"><input type="number" min="1" max="100" placeholder="%" value={v.percent ?? ''} onChange={(e) => setVariant(i, vi, 'percent', e.target.value)} /><i>% random</i></span>
                      <button type="button" className="btn icon danger" onClick={() => delVariant(i, vi)}>✕</button>
                    </div>
                    <div className="variant-when"><span className="muted sm">who (facts) — optional</span>
                      <ConditionBuilder tree={v.conditions} onChange={(t) => setVariant(i, vi, 'conditions', t)} allowEmpty /></div>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
        {!a.channels.length && <div className="muted sm">add at least one channel</div>}
      </div>

      <div className="block"><div className="block-head"><h4>Inclusion <span className="muted sm">— member must match</span></h4></div>
        <ConditionBuilder tree={a.inclusion} onChange={(t) => upd({ inclusion: t })} /></div>
      <div className="block"><div className="block-head"><h4>Exclusion <span className="muted sm">— blocks the action</span></h4></div>
        <ConditionBuilder tree={a.exclusion} onChange={(t) => upd({ exclusion: t })} allowEmpty /></div>

      <div className="block"><div className="block-head"><h4>Completion <span className="muted sm">— the GOAL: the member did the thing (hard completion)</span></h4></div>
        <ConditionBuilder tree={a.completion} onChange={(t) => upd({ completion: t })} allowEmpty />
        <div className="completion-opts">
          <label className="chk"><input type="checkbox" checked={a.autoExcludeOnCompletion !== false} onChange={(e) => upd({ autoExcludeOnCompletion: e.target.checked })} />
            <span>Exclude once completed <i className="muted sm">— auto-retire this action for the member, every channel</i></span></label>
          <Field label="Wait for completion" hint="seconds · hard TTL"><input type="number" placeholder="optional" value={a.hardTtlSeconds} onChange={(e) => upd({ hardTtlSeconds: e.target.value })} /></Field>
        </div></div>

      {err && <div className="err">{err}</div>}
      <div className="editor-actions">
        <button className="btn primary" disabled={busy || !a.name} onClick={save}>{busy ? 'Saving…' : (a.id ? 'Save action' : 'Create action')}</button>
        <button className="btn ghost" onClick={onCancel}>Cancel</button>
      </div>
    </div>
  );
}

// ---- rule editor (global or channel) ----
function RuleEditor({ kind, value, onSaved, onCancel }) {
  const isChannel = kind === 'channel';
  const [r, setR] = useState(() => value ? clone(value) : { name: '', channel: isChannel ? 'email' : undefined, logic: emptyTree() });
  const [busy, setBusy] = useState(false); const [err, setErr] = useState(null);
  const upd = (patch) => setR((x) => ({ ...x, ...patch }));
  async function save() {
    setBusy(true); setErr(null);
    try {
      const input = { id: r.id, name: r.name, logic: r.logic, ...(isChannel ? { channel: r.channel } : {}) };
      const m = kind === 'channel' ? 'upsertChannelRule' : kind === 'milestone' ? 'upsertMilestone' : 'upsertGlobalRule';
      await gql(`mutation($i: RuleInput!){ ${m}(input: $i){ id } }`, { i: input });
      onSaved();
    } catch (e) { setErr(e.message); } finally { setBusy(false); }
  }
  return (
    <div className="editor">
      <div className="editor-grid">
        <Field label="Rule name"><input value={r.name} onChange={(e) => upd({ name: e.target.value })} placeholder="e.g. Max 3 comms / week" /></Field>
        {isChannel && <Field label="Channel"><select value={r.channel} onChange={(e) => upd({ channel: e.target.value })}>{CHANNELS.map((c) => <option key={c}>{c}</option>)}</select></Field>}
      </div>
      <div className="block"><div className="block-head"><h4>Logic <span className="muted sm">— must hold for the action to pass</span></h4></div>
        <ConditionBuilder tree={r.logic} onChange={(t) => upd({ logic: t })} /></div>
      {err && <div className="err">{err}</div>}
      <div className="editor-actions">
        <button className="btn primary" disabled={busy || !r.name} onClick={save}>{busy ? 'Saving…' : (r.id ? 'Save rule' : 'Create rule')}</button>
        <button className="btn ghost" onClick={onCancel}>Cancel</button>
      </div>
    </div>
  );
}

// ---- studio: list + edit for a given entity ----
export function Studio({ kind }) {
  // kind: 'actions' | 'global' | 'channel'
  const [items, setItems] = useState(null);
  const [editing, setEditing] = useState(null);   // object | 'new' | null
  const [err, setErr] = useState(null);

  const Q = {
    actions: `{ actions { id name ttlSeconds channels { channel contentKey variants softCompletion } inclusion exclusion completion hardTtlSeconds autoExcludeOnCompletion factsUsed groupId experienceId } }`,
    global: `{ globalRules { id name logic factsUsed } }`,
    channel: `{ channelRules { id name channel logic factsUsed } }`,
    milestone: `{ milestones { id name logic factsUsed } }`,
  }[kind];
  const pick = (d) => d.actions || d.globalRules || d.channelRules || d.milestones;
  const load = () => gql(Q).then((d) => setItems(pick(d))).catch((e) => setErr(e.message));
  useEffect(() => { setItems(null); setEditing(null); load(); /* eslint-disable-next-line */ }, [kind]);

  const del = async (id) => {
    const m = kind === 'actions' ? 'deleteAction' : kind === 'global' ? 'deleteGlobalRule' : kind === 'milestone' ? 'deleteMilestone' : 'deleteChannelRule';
    try { await gql(`mutation($id: ID!){ ${m}(id: $id) }`, { id }); load(); } catch (e) { setErr(e.message); }
  };
  const onSaved = () => { setEditing(null); load(); };

  if (editing) {
    const v = editing === 'new' ? null : editing;
    return kind === 'actions'
      ? <ActionEditor value={v} onSaved={onSaved} onCancel={() => setEditing(null)} />
      : <RuleEditor kind={kind} value={v} onSaved={onSaved} onCancel={() => setEditing(null)} />;
  }

  const title = { actions: 'Actions', global: 'Global rules', channel: 'Channel rules', milestone: 'Milestones' }[kind];
  const newLabel = { actions: 'New action', global: 'New global rule', channel: 'New channel rule', milestone: 'New milestone' }[kind];
  return (
    <section className="card wide">
      <div className="card-head"><div><h3>{title}</h3><span className="card-sub">authoring · writes to the Action Library → live NBA pipeline</span></div>
        <button className="btn primary sm" onClick={() => setEditing('new')}>＋ {newLabel}</button></div>
      {err && <div className="err">{err}</div>}
      {!items ? <div className="skeleton" /> : (
        <div className="def-list">
          {items.map((it) => (
            <div className="def-row" key={it.id}>
              <div className="def-main">
                <div className="def-name">{it.name}{it.channel && <span className="tag">{it.channel}</span>}</div>
                <div className="def-facts">{(it.factsUsed || []).map((f) => <span className="chip" key={f}>{f}</span>)}
                  {it.channels && it.channels.map((c) => <span className="chip alt" key={c.channel}>{c.channel}{c.contentKey ? ' · ' + c.contentKey : ''}{(c.variants || []).length ? ' +' + c.variants.length : ''}</span>)}</div>
              </div>
              <div className="def-actions">
                <button className="btn ghost sm" onClick={() => setEditing(it)}>Edit</button>
                <button className="btn icon danger" onClick={() => del(it.id)}>✕</button>
              </div>
            </div>
          ))}
          {!items.length && <div className="muted">none yet — create one</div>}
        </div>
      )}
    </section>
  );
}

// ---- Action GROUPS — a taxonomy TREE over actions. Create groups (nest for >1 level), assign actions,
//      browse a group's actions ROLLED UP across descendants, and delete EMPTY groups. ----
export function Groups() {
  const [groups, setGroups] = useState(null);
  const [actions, setActions] = useState([]);
  const [sel, setSel] = useState(null);                 // selected group id (null = the Ungrouped bucket)
  const [expanded, setExpanded] = useState({});
  const [err, setErr] = useState(null);
  const [rootName, setRootName] = useState('');
  const [addUnder, setAddUnder] = useState(null);
  const [subName, setSubName] = useState('');

  const load = () => Promise.all([
    gql(`{ actionGroups { id name parentId } }`).then((d) => d.actionGroups || []),
    gql(`{ actions { id name groupId } }`).then((d) => d.actions || []),
  ]).then(([g, a]) => { setGroups(g); setActions(a); }).catch((e) => setErr(e.message));
  useEffect(() => { load(); }, []);

  const kids = {};
  (groups || []).forEach((g) => { (kids[g.parentId || '__root'] ||= []).push(g); });
  Object.values(kids).forEach((arr) => arr.sort((a, b) => a.name.localeCompare(b.name)));
  const byId = Object.fromEntries((groups || []).map((g) => [g.id, g]));
  const descIds = (id) => { const out = [id]; (kids[id] || []).forEach((c) => out.push(...descIds(c.id))); return out; };
  const directCount = (id) => (actions || []).filter((a) => a.groupId === id).length;
  const subtreeCount = (id) => { const s = new Set(descIds(id)); return (actions || []).filter((a) => a.groupId && s.has(a.groupId)).length; };
  const isEmpty = (id) => (kids[id] || []).length === 0 && directCount(id) === 0;
  const unassigned = (actions || []).filter((a) => !a.groupId);
  const opts = groupOptions(groups || []);
  const selActions = sel ? (actions || []).filter((a) => { const s = new Set(descIds(sel)); return a.groupId && s.has(a.groupId); }) : [];

  const create = async (name, parentId) => {
    if (!name.trim()) return;
    try { await gql(`mutation($n:String!,$p:ID){ createActionGroup(name:$n, parentId:$p){ id } }`, { n: name.trim(), p: parentId || null });
      setErr(null); setRootName(''); setSubName(''); setAddUnder(null); if (parentId) setExpanded((x) => ({ ...x, [parentId]: true })); load();
    } catch (e) { setErr(e.message); }
  };
  const del = async (id) => { try { await gql(`mutation($id:ID!){ deleteActionGroup(id:$id) }`, { id }); if (sel === id) setSel(null); load(); } catch (e) { setErr(e.message); } };
  const assign = async (actionId, groupId) => { try { await gql(`mutation($a:ID!,$g:ID){ assignActionGroup(actionId:$a, groupId:$g) }`, { a: actionId, g: groupId || null }); load(); } catch (e) { setErr(e.message); } };

  function Node({ g, depth }) {
    const childList = kids[g.id] || [];
    const open = expanded[g.id];
    const empty = isEmpty(g.id);
    return (
      <div className="grp-node">
        <div className={'grp-row' + (sel === g.id ? ' on' : '')} style={{ paddingLeft: depth * 18 + 6 }}>
          <button className="grp-tw" onClick={() => setExpanded((x) => ({ ...x, [g.id]: !x[g.id] }))}>{childList.length ? (open ? '▾' : '▸') : '·'}</button>
          <span className="grp-name" onClick={() => setSel(g.id)}>{g.name}</span>
          <span className="grp-count" title="actions in this group, incl. subgroups">{subtreeCount(g.id)}</span>
          <button className="btn icon sm" title="add subgroup" onClick={() => { setAddUnder(g.id); setSubName(''); }}>＋</button>
          <button className="btn icon sm danger" disabled={!empty} title={empty ? 'delete group' : 'not empty — has subgroups or actions'} onClick={() => del(g.id)}>✕</button>
        </div>
        {addUnder === g.id && (
          <div className="grp-addrow" style={{ paddingLeft: (depth + 1) * 18 + 6 }}>
            <input autoFocus placeholder="subgroup name" value={subName} onChange={(e) => setSubName(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') create(subName, g.id); if (e.key === 'Escape') setAddUnder(null); }} />
            <button className="btn primary sm" onClick={() => create(subName, g.id)}>Add</button>
            <button className="btn ghost sm" onClick={() => setAddUnder(null)}>Cancel</button>
          </div>
        )}
        {open && childList.map((k) => <Node key={k.id} g={k} depth={depth + 1} />)}
      </div>
    );
  }

  return (
    <div className="grid">
      <section className="card wide">
        <div className="card-head"><div><h3>Action groups</h3>
          <span className="card-sub">taxonomy tree · assign actions, browse a group (incl. subgroups), delete empty groups</span></div></div>
        {err && <div className="err">{err}</div>}
        <div className="grp-newroot">
          <input placeholder="new top-level group" value={rootName} onChange={(e) => setRootName(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') create(rootName, null); }} />
          <button className="btn primary sm" onClick={() => create(rootName, null)}>＋ Add group</button>
        </div>
        {!groups ? <div className="skeleton" /> : (
          <div className="grp-tree">
            <div className={'grp-row' + (sel === null ? ' on' : '')} style={{ paddingLeft: 6 }}>
              <span className="grp-tw">·</span>
              <span className="grp-name" onClick={() => setSel(null)}>Ungrouped</span>
              <span className="grp-count">{unassigned.length}</span>
            </div>
            {(kids['__root'] || []).map((g) => <Node key={g.id} g={g} depth={0} />)}
            {!(kids['__root'] || []).length && <div className="muted sm">no groups yet — add one above</div>}
          </div>
        )}
      </section>

      <section className="card">
        <div className="card-head"><div><h3>{sel ? byId[sel]?.name : 'Ungrouped'}</h3>
          <span className="card-sub">{sel ? `${selActions.length} action(s) here + subgroups` : `${unassigned.length} action(s) with no group`}</span></div></div>
        <div className="grp-actions">
          {(sel ? selActions : unassigned).map((a) => (
            <div className="grp-action" key={a.id}>
              <div className="grp-aname">{a.name}{sel && a.groupId !== sel && byId[a.groupId] && <span className="tag">{byId[a.groupId].name}</span>}</div>
              <select value={a.groupId || ''} onChange={(e) => assign(a.id, e.target.value || null)}>
                <option value="">— ungrouped —</option>
                {opts.map((o) => <option key={o.id} value={o.id}>{o.label}</option>)}
              </select>
            </div>
          ))}
          {!(sel ? selActions : unassigned).length && <div className="muted sm">no actions here</div>}
        </div>
      </section>
    </div>
  );
}

// ---- EXPERIENCES — a flat, business-facing taxonomy (enrollment, onboarding…) that groups actions into
//      member journeys. Create, assign actions, delete empty. (Peer to action groups, no tree.) ----
export function Experiences() {
  const [exps, setExps] = useState(null);
  const [actions, setActions] = useState([]);
  const [sel, setSel] = useState(null);
  const [err, setErr] = useState(null);
  const [name, setName] = useState('');
  const [desc, setDesc] = useState('');

  const load = () => Promise.all([
    gql(`{ experiences { id name description } }`).then((d) => d.experiences || []),
    gql(`{ actions { id name experienceId } }`).then((d) => d.actions || []),
  ]).then(([e, a]) => { setExps(e); setActions(a); }).catch((e) => setErr(e.message));
  useEffect(() => { load(); }, []);

  const countOf = (id) => (actions || []).filter((a) => a.experienceId === id).length;
  const unassigned = (actions || []).filter((a) => !a.experienceId);
  const selActions = sel ? (actions || []).filter((a) => a.experienceId === sel) : unassigned;
  const opts = (exps || []).map((x) => ({ id: x.id, label: x.name }));

  const create = async () => {
    if (!name.trim()) return;
    try { await gql(`mutation($n:String!,$d:String){ createExperience(name:$n, description:$d){ id } }`, { n: name.trim(), d: desc.trim() || null }); setName(''); setDesc(''); setErr(null); load(); }
    catch (e) { setErr(e.message); }
  };
  const del = async (id) => { try { await gql(`mutation($id:ID!){ deleteExperience(id:$id) }`, { id }); if (sel === id) setSel(null); load(); } catch (e) { setErr(e.message); } };
  const assign = async (actionId, experienceId) => { try { await gql(`mutation($a:ID!,$e:ID){ assignExperience(actionId:$a, experienceId:$e) }`, { a: actionId, e: experienceId || null }); load(); } catch (e) { setErr(e.message); } };

  return (
    <div className="grid">
      <section className="card wide">
        <div className="card-head"><div><h3>Experiences</h3>
          <span className="card-sub">business-facing journeys (enrollment, onboarding…) — assign actions, delete empty</span></div></div>
        {err && <div className="err">{err}</div>}
        <div className="grp-newroot">
          <input placeholder="new experience (e.g. Onboarding)" value={name} onChange={(e) => setName(e.target.value)} />
          <input placeholder="description (optional)" value={desc} onChange={(e) => setDesc(e.target.value)} onKeyDown={(e) => { if (e.key === 'Enter') create(); }} />
          <button className="btn primary sm" onClick={create}>＋ Add</button>
        </div>
        {!exps ? <div className="skeleton" /> : (
          <div className="grp-tree">
            <div className={'grp-row' + (sel === null ? ' on' : '')}><span className="grp-tw">·</span>
              <span className="grp-name" onClick={() => setSel(null)}>Unassigned</span><span className="grp-count">{unassigned.length}</span></div>
            {exps.map((x) => (
              <div className={'grp-row' + (sel === x.id ? ' on' : '')} key={x.id}>
                <span className="grp-tw">●</span>
                <span className="grp-name" onClick={() => setSel(x.id)}>{x.name}{x.description && <i className="muted sm"> — {x.description}</i>}</span>
                <span className="grp-count">{countOf(x.id)}</span>
                <button className="btn icon sm danger" disabled={countOf(x.id) > 0} title={countOf(x.id) > 0 ? 'not empty — has actions' : 'delete'} onClick={() => del(x.id)}>✕</button>
              </div>
            ))}
            {!exps.length && <div className="muted sm">no experiences yet — add one above</div>}
          </div>
        )}
      </section>
      <section className="card">
        <div className="card-head"><div><h3>{sel ? (exps.find((x) => x.id === sel) || {}).name : 'Unassigned'}</h3>
          <span className="card-sub">{selActions.length} action(s)</span></div></div>
        <div className="grp-actions">
          {selActions.map((a) => (
            <div className="grp-action" key={a.id}>
              <div className="grp-aname">{a.name}</div>
              <select value={a.experienceId || ''} onChange={(e) => assign(a.id, e.target.value || null)}>
                <option value="">— none —</option>
                {opts.map((o) => <option key={o.id} value={o.id}>{o.label}</option>)}
              </select>
            </div>
          ))}
          {!selActions.length && <div className="muted sm">no actions here</div>}
        </div>
      </section>
    </div>
  );
}
