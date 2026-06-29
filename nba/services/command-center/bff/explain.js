// Decision explainer — re-evaluates an action's rules against a member's facts and returns a
// plain-English, per-rule pass/fail breakdown. This is what powers "exactly why this action was
// eligible (or not)" in the Trace view, without changing the rules engine: the decision is
// deterministic from (facts, rules), so we reproduce it. Facts = a map<key, value-string> (gold).

const CMP_SYM = { eq: '=', ne: '≠', gt: '>', gte: '≥', lt: '<', lte: '≤', in: 'in', exists: 'exists' };
const TYPE_DEFAULT = (v) => (typeof v === 'boolean' ? false : typeof v === 'number' ? 0 : '');

function actual(facts, key, like) {
  const raw = facts ? facts[key] : undefined;
  if (raw === undefined || raw === null) return { present: false, val: TYPE_DEFAULT(like) };
  if (typeof like === 'boolean') return { present: true, val: String(raw).toLowerCase() === 'true' };
  if (typeof like === 'number') { const n = Number(raw); return { present: true, val: Number.isNaN(n) ? 0 : n }; }
  return { present: true, val: String(raw) };
}

function evalCond(c, facts) {
  const a = actual(facts, c.fact, c.value);
  let passed;
  switch (c.cmp) {
    case 'eq': passed = a.val === c.value; break;
    case 'ne': passed = a.val !== c.value; break;
    case 'gt': passed = a.val > c.value; break;
    case 'gte': passed = a.val >= c.value; break;
    case 'lt': passed = a.val < c.value; break;
    case 'lte': passed = a.val <= c.value; break;
    case 'exists': passed = a.present; break;
    case 'in': passed = (Array.isArray(c.value) ? c.value : [c.value]).includes(a.val); break;
    default: passed = true;
  }
  const shown = a.present ? a.val : `${a.val} (default)`;
  const detail = c.cmp === 'exists'
    ? `${c.fact} ${a.present ? 'exists' : 'missing'}`
    : `${c.fact} = ${shown} ${CMP_SYM[c.cmp] || c.cmp} ${JSON.stringify(c.value)}`;
  return { fact: c.fact, passed, detail };
}

// evaluate a condition tree -> { passed, op, checks:[{detail,passed}] }
function evalTree(tree, facts) {
  if (!tree || !Array.isArray(tree.conditions) || tree.conditions.length === 0) return { passed: true, op: 'all', checks: [] };
  const checks = tree.conditions.map((c) => (c.conditions ? { ...evalTree(c, facts), nested: true } : evalCond(c, facts)));
  const passed = tree.op === 'any' ? checks.some((x) => x.passed) : checks.every((x) => x.passed);
  return { passed, op: tree.op || 'all', checks };
}

// Full eligibility explanation for an action: inclusion + exclusion + global + channel rules.
export function explainEligibility(action, globalRules, channelRules, facts) {
  const incl = evalTree(action.inclusion, facts);
  const excl = action.exclusion && action.exclusion.conditions?.length ? evalTree(action.exclusion, facts) : null;
  const globals = (globalRules || []).map((g) => ({ name: g.name, ...evalTree(g.logic, facts) }));
  const chans = (action.channels || []).map((ch) => {
    const crules = (channelRules || []).filter((c) => c.channel === ch.channel).map((c) => ({ name: c.name, ...evalTree(c.logic, facts) }));
    const eligible = incl.passed && (!excl || !excl.passed) && globals.every((g) => g.passed) && crules.every((c) => c.passed);
    const stages = [
      { label: 'Inclusion (must match)', passed: incl.passed, checks: incl.checks },
      ...(excl ? [{ label: 'Exclusion (must NOT match)', passed: !excl.passed, checks: excl.checks, negate: true }] : []),
      ...globals.map((g) => ({ label: 'Global · ' + g.name, passed: g.passed, checks: g.checks })),
      ...crules.map((c) => ({ label: 'Channel · ' + c.name, passed: c.passed, checks: c.checks })),
    ];
    const failedAt = stages.find((s) => !s.passed);
    return { channel: ch.channel, eligible, stages,
      reason: eligible ? 'all rules passed' : `blocked at "${failedAt?.label}"` };
  });
  return { actionId: action.id, name: action.name, channels: chans };
}
