/** Replace the home directory prefix with `~` for display. Cross-platform: works with
 *  both `/` (macOS/Linux) and `\` (Windows) separators. */
export function prettyPath(p: string, home: string): string {
  const h = home.replace(/[\\/]+$/, ''); // strip trailing separator
  if (p === h) return '~';
  const sep = h.includes('\\') ? '\\' : '/';
  if (p.startsWith(h + sep)) return '~' + p.slice(h.length);
  return p;
}

/** Expand a `~`-prefixed path back to an absolute path before sending to the backend. */
export function expandPath(p: string, home: string): string {
  const h = home.replace(/[\\/]+$/, '');
  const sep = h.includes('\\') ? '\\' : '/';
  if (p === '~') return h;
  if (p.startsWith('~/') || p.startsWith('~\\')) return h + sep + p.slice(2);
  return p;
}
