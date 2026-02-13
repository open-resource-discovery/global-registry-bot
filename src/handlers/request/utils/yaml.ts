export function escRe(s: string = ''): string {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
