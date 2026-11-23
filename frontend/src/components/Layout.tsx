import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { Activity, AlertTriangle, Database, FolderKanban, HardDrive, ShieldOff, Terminal } from "lucide-react";
import { useClusterSync, useServerClock } from "../api/hooks";
import { formatRelativeTime } from "../utils/format";
import { StatusBadge } from "./StatusBadge";
import { DemoCommandsModal } from "./DemoCommandsModal";

export function Layout() {
  const { isStaleOrUnreachable, adminDisabled, lastSyncMs, lastError, consecutiveFailures } = useClusterSync();
  const [demoOpen, setDemoOpen] = useState(false);
  useServerClock(1000);

  const navClass = ({ isActive }: { isActive: boolean }) =>
    `inline-flex items-center gap-2 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors ${
      isActive ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
    }`;

  const lastSyncSec = lastSyncMs ? lastSyncMs / 1000 : null;
  const nowSec = Date.now() / 1000;

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/95 backdrop-blur">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-4 py-3 sm:px-6">
          <div className="flex items-center gap-6">
            <NavLink to="/" className="flex items-center gap-2.5">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-slate-900 text-white">
                <HardDrive className="h-5 w-5" />
              </div>
              <div>
                <div className="text-sm font-bold tracking-tight text-slate-900">Mini DFS Console</div>
                <div className="text-[11px] text-slate-500">Distributed Object Store</div>
              </div>
            </NavLink>

            <nav className="flex items-center gap-1.5" aria-label="Primary navigation">
              <NavLink to="/" end className={navClass}>
                <Database className="h-4 w-4" />
                <span>Overview</span>
              </NavLink>
              <NavLink to="/objects" className={navClass}>
                <FolderKanban className="h-4 w-4" />
                <span>Objects</span>
              </NavLink>
              <NavLink to="/events" className={navClass}>
                <Activity className="h-4 w-4" />
                <span>Events</span>
              </NavLink>
            </nav>
          </div>

          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs">
              {adminDisabled ? (
                <StatusBadge status="ERROR" labelOverride="Admin Disabled (403)" />
              ) : isStaleOrUnreachable ? (
                <StatusBadge status="UNREACHABLE" labelOverride="Controller Unreachable" />
              ) : (
                <StatusBadge status="CONNECTED" labelOverride="Connected" />
              )}
              <span
                className="text-slate-500"
                title={lastSyncSec ? new Date(lastSyncSec * 1000).toISOString() : "Never synced"}
              >
                Last sync: {formatRelativeTime(lastSyncSec, nowSec)}
              </span>
            </div>

            <button
              type="button"
              onClick={() => setDemoOpen(true)}
              className="inline-flex items-center gap-1.5 rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100"
            >
              <Terminal className="h-3.5 w-3.5" />
              <span>Demo Commands</span>
            </button>
          </div>
        </div>

        {adminDisabled && (
          <div className="border-t border-rose-200 bg-rose-50 px-4 py-2 text-xs text-rose-900">
            <div className="mx-auto flex max-w-7xl items-center gap-2">
              <ShieldOff className="h-4 w-4 shrink-0 text-rose-700" />
              <span>
                Admin API is disabled on the controller (<code>ADMIN_ENABLED=false</code>, HTTP 403). Object client operations remain available on the Objects page.
              </span>
            </div>
          </div>
        )}

        {isStaleOrUnreachable && (
          <div className="border-t border-amber-300 bg-amber-50 px-4 py-2 text-xs text-amber-950" role="alert">
            <div className="mx-auto flex max-w-7xl items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 shrink-0 text-amber-700" />
                <span>
                  <strong>Stale / Controller Unreachable</strong> ({consecutiveFailures} consecutive poll failures). Retaining last known state.
                  {lastError ? ` Error: ${lastError}` : ""}
                </span>
              </div>
              <span className="font-mono text-[11px] text-amber-800">Retrying every 1.5s</span>
            </div>
          </div>
        )}
      </header>

      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6">
        <Outlet />
      </main>

      <DemoCommandsModal open={demoOpen} onClose={() => setDemoOpen(false)} />
    </div>
  );
}
