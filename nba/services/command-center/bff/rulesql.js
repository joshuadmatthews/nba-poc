// Rule-to-SQL compiler — turns the NBA structured condition trees (the SAME JSON the rules engine
// compiles to Drools) into SQL boolean expressions over the gold_member_facts map (entityId ->
// map<key,value>). This lets the Command Center run the rule FUNNEL directly against gold, decoupled
// from the heavy Drools server. Missing facts default to their type-zero (matches the engine).

const lit = (v) => {
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
};

// element_at(facts,'k') is a string; cast per the literal's type with a type-zero default for missing facts
function factExpr(fact, value) {
  const raw = `element_at(facts, '${String(fact).replace(/'/g, "''")}')`;
  if (typeof value === 'boolean') return `COALESCE(lower(${raw}) = 'true', false)`;
  if (typeof value === 'number') return `COALESCE(try_cast(${raw} AS DOUBLE), 0)`;
  return `COALESCE(${raw}, '')`;
}

function condSql(c) {
  const f = factExpr(c.fact, c.value);
  const v = lit(c.value);
  switch (c.cmp) {
    case 'eq': return `${f} = ${v}`;
    case 'ne': return `${f} <> ${v}`;
    case 'gt': return `${f} > ${v}`;
    case 'gte': return `${f} >= ${v}`;
    case 'lt': return `${f} < ${v}`;
    case 'lte': return `${f} <= ${v}`;
    case 'exists': return `element_at(facts, '${String(c.fact).replace(/'/g, "''")}') IS NOT NULL`;
    case 'in': {
      const arr = Array.isArray(c.value) ? c.value : [c.value];
      return `${factExpr(c.fact, arr[0])} IN (${arr.map(lit).join(', ')})`;
    }
    default: return 'true';
  }
}

// Compile a condition tree {op: all|any, conditions:[{fact,cmp,value} | nested tree]} -> SQL boolean
export function treeSql(tree) {
  if (!tree || !Array.isArray(tree.conditions) || tree.conditions.length === 0) return 'true';
  const parts = tree.conditions.map((c) => (c.conditions ? treeSql(c) : condSql(c)));
  return '(' + parts.join(tree.op === 'any' ? ' OR ' : ' AND ') + ')';
}

const AND = (...xs) => xs.filter(Boolean).map((x) => `(${x})`).join(' AND ') || 'true';

// Build a funnel: ordered stages of cumulative SQL predicates over gold_member_facts.
// stages[i].sql is the cumulative WHERE; the resolver counts members at each.
export function buildFunnel({ inclusion, exclusion, globalRules = [], channelRules = [] }) {
  const incl = treeSql(inclusion);
  const excl = exclusion && exclusion.conditions?.length ? `NOT ${treeSql(exclusion)}` : null;
  const globals = globalRules.map((g) => treeSql(g.logic || g));
  const channels = channelRules.map((c) => treeSql(c.logic || c));
  const stages = [];
  let cum = 'true';
  const push = (label, pred) => { cum = AND(cum, pred); stages.push({ label, sql: cum }); };
  stages.push({ label: 'All members', sql: 'true' });
  push('Inclusion', incl);
  if (excl) push('Exclusion', excl);
  globals.forEach((g, i) => push(globalRules[i].name || `Global rule ${i + 1}`, g));
  channels.forEach((c, i) => push(channelRules[i].name || `Channel rule ${i + 1}`, c));
  return stages;
}
