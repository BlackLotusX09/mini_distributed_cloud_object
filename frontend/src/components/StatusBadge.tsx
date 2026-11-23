import { AlertTriangle, CheckCircle2, HelpCircle, Info, Layers, ShieldAlert, XCircle } from "lucide-react";

type BadgeVariant = "healthy" | "warning" | "error" | "info" | "neutral" | "accent";

const VARIANT_CLASSES: Record<BadgeVariant, string> = {
  healthy: "bg-emerald-50 text-emerald-800 border-emerald-200",
  warning: "bg-amber-50 text-amber-800 border-amber-200",
  error: "bg-rose-50 text-rose-800 border-rose-200",
  info: "bg-sky-50 text-sky-800 border-sky-200",
  accent: "bg-indigo-50 text-indigo-800 border-indigo-200",
  neutral: "bg-slate-100 text-slate-700 border-slate-300",
};

function resolveVariant(status: string): { variant: BadgeVariant; icon: typeof CheckCircle2; label: string } {
  const up = status.toUpperCase();
  if (up === "ALIVE" || up === "HEALTHY" || up === "OK" || up === "COMMITTED" || up === "CONNECTED") {
    return { variant: "healthy", icon: CheckCircle2, label: up };
  }
  if (up === "DEGRADED" || up === "UNDER_REPLICATED" || up === "WARN" || up === "PENDING") {
    return { variant: "warning", icon: AlertTriangle, label: up };
  }
  if (up === "CORRUPT" || up === "UNRECOVERABLE" || up === "STALE") {
    return { variant: "error", icon: ShieldAlert, label: up };
  }
  if (up === "DEAD" || up === "UNAVAILABLE" || up === "ERROR" || up === "DELETING" || up === "UNREACHABLE") {
    return { variant: "error", icon: XCircle, label: up };
  }
  if (up === "OVER_REPLICATED" || up === "PENDING_DELETE") {
    return { variant: "accent", icon: Layers, label: up };
  }
  if (up === "INFO") {
    return { variant: "info", icon: Info, label: up };
  }
  return { variant: "neutral", icon: HelpCircle, label: up || "UNKNOWN" };
}

export function StatusBadge({ status, labelOverride }: { status: string; labelOverride?: string }) {
  const { variant, icon: Icon, label } = resolveVariant(status);
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-semibold tracking-wide ${VARIANT_CLASSES[variant]}`}
    >
      <Icon className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
      <span>{labelOverride ?? label}</span>
    </span>
  );
}
