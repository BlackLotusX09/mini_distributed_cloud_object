export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes)) return "-";
  if (bytes === 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let val = bytes;
  let idx = 0;
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024;
    idx += 1;
  }
  return idx === 0 ? `${val} B` : `${val.toFixed(2)} ${units[idx]}`;
}

export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

export function formatRelativeTime(epochSec: number | null | undefined, nowSec: number): string {
  if (epochSec === null || epochSec === undefined) return "never";
  const diff = Math.max(0, Math.round(nowSec - epochSec));
  if (diff < 2) return "just now";
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export function formatIsoUtc(epochSec: number | null | undefined): string {
  if (epochSec === null || epochSec === undefined) return "-";
  return new Date(epochSec * 1000).toISOString();
}

export function shortId(id: string): string {
  return id.length > 13 ? `${id.slice(0, 8)}...${id.slice(-4)}` : id;
}

export const WORKER_COLORS: Record<string, { bg: string; border: string; text: string; pill: string; short: string }> = {
  worker1: { bg: "bg-blue-600", border: "border-blue-600", text: "text-blue-700", pill: "bg-blue-50 border-blue-200 text-blue-800", short: "W1" },
  worker2: { bg: "bg-purple-600", border: "border-purple-600", text: "text-purple-700", pill: "bg-purple-50 border-purple-200 text-purple-800", short: "W2" },
  worker3: { bg: "bg-orange-500", border: "border-orange-500", text: "text-orange-700", pill: "bg-orange-50 border-orange-200 text-orange-800", short: "W3" },
  worker4: { bg: "bg-teal-600", border: "border-teal-600", text: "text-teal-700", pill: "bg-teal-50 border-teal-200 text-teal-800", short: "W4" },
};

export function getWorkerStyle(workerId: string) {
  return (
    WORKER_COLORS[workerId] ?? {
      bg: "bg-slate-600",
      border: "border-slate-600",
      text: "text-slate-700",
      pill: "bg-slate-100 border-slate-300 text-slate-800",
      short: workerId.slice(0, 3).toUpperCase(),
    }
  );
}
