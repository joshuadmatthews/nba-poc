// Tiny GraphQL client — POST to the BFF (same origin via nginx /graphql proxy).
export async function gql(query, variables) {
  const r = await fetch('/graphql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(j.errors.map((e) => e.message).join('; '));
  return j.data;
}

// Coerce a text input into a typed value for the structured logic.
export function coerce(s) {
  const t = String(s).trim();
  if (t === 'true') return true;
  if (t === 'false') return false;
  if (t !== '' && !isNaN(Number(t))) return Number(t);
  return s;
}
