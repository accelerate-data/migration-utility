export function toSlugPreview(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/** Today's date as a YYYY-MM-DD string in the local timezone. */
export function localTodayString(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/** Convert a local YYYY-MM-DD date string (midnight local) to a UTC ISO string for storage. */
export function localDateToUtc(localDate: string): string {
  const [y, mo, d] = localDate.split('-').map(Number);
  return new Date(y, mo - 1, d, 0, 0, 0, 0).toISOString();
}

/** Convert a stored UTC ISO string back to a local YYYY-MM-DD string for display. */
export function utcToLocalDate(utcString: string): string {
  const dt = new Date(utcString);
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, '0');
  const d = String(dt.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}
