import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { ArrowRight, Clock, Cpu, Database, HardDrive, RefreshCw, ShieldCheck, Terminal } from "lucide-react";
import { api, ApiRequestError, type ScrubRunResponse } from "../api/client";
import { useLatestEvents, useOverview, useServerClock } from "../api/hooks";
import { formatBytes, formatDuration, formatIsoUtc, formatRelativeTime } from "../utils/format";
import { StatusBadge } from "../components/StatusBadge";
import { DemoCommandsModal } from "../components/DemoCommandsModal";

export function OverviewPage() {
  const { data: overview, isLoading, error } = useOverview();
  const { data: eventsRes } = useLatestEvents(5);
  const nowSec = useServerClock(1000);

  const [scrubbing, setScrubbing] = useState(false);
  const [scrubResult, setScrubResult] = useState<ScrubRunResponse | null>(null);
  const [scrubError, setScrubError] = useState<string | null>(null);
  const [demoOpen, setDemoOpen] = useState(false);

  // Detect worker state transitions to briefly highlight cards
  const prevStatuses = useRef<Record<string, string>>({});
  const [highlighted, setHighlighted] = useState<Record<string, boolean>>({});

  useEffect(() => {
    if (!overview) return;
    const changed: string[] = [];
    for (const w of overview.workers) {
      const prev = prevStatuses.current[w.worker_id];
      if (prev && prev !== w.status) {
        changed.push(w.worker_id);
      }
      prevStatuses.current[w.worker_id] = w.status;
    }
    if (changed.length > 0) {
      setHighlighted((cur) => {
        const next = { ...cur };
        changed.forEach((id) => {
          next[id] = true;
        });
        return next;
      });
      const timer = window.setTimeout(() => {
        setHighlighted((cur) => {
          const next = { ...cur };
          changed.forEach((id) => {
            delete next[id];
          });
          return next;
        });
      }, 2800);
      return () => window.clearTimeout(timer);
    }
  }, [overview]);

  const handleScrub = async () => {
    setScrubbing(true);
    setScrubError(null);
    try {
      const res = await api.triggerScrub();
      setScrubResult(res);
    } catch (err) {
      setScrubError(err instanceof ApiRequestError ? `${err.code}: ${err.message}` : "Scrub failed");
    } finally {
      setScrubbing(false);
    }
  };

  if (isLoading && !overview) {
    return (
      <div className="rounded-xl border border-slate-200 bg-white p-8 text-center text-sm text-slate-500">
        Loading cluster overview...
      </div>
    );
  }

  if (error && !overview) {
    return (
      <div className="rounded-xl border border-rose-200 bg-rose-50 p-6 text-sm text-rose-800">
        Failed to load cluster overview: {error instanceof Error ? error.message : "Unknown error"}
      </div>
    );
  }

  const workers = overview?.workers ?? [];
  const rep = overview?.replication;
  const totals = overview?.totals;
  const cfg = overview?.config;
  const scrub = overview?.last_scrub;

  const totalChunks = rep?.total_chunks ?? 0;
  const segments = [
    { key: "healthy", label: "Healthy", count: rep?.healthy ?? 0, bar: "bg-emerald-500", dot: "bg-emerald-500" },
    { key: "under", label: "Under-Replicated", count: rep?.under_replicated ?? 0, bar: "bg-amber-400", dot: "bg-amber-400" },
    { key: "unavail", label: "Unavailable", count: rep?.unavailable ?? 0, bar: "bg-orange-600", dot: "bg-orange-600" },
    { key: "corrupt", label: "Corrupt", count: rep?.corrupt ?? 0, bar: "bg-rose-600", dot: "bg-rose-600" },
    { key: "over", label: "Over-Replicated", count: rep?.over_replicated ?? 0, bar: "bg-indigo-500", dot: "bg-indigo-500" },
  ];

  const replicationRatio =
    totals && totals.logical_bytes > 0 ? (totals.raw_bytes / totals.logical_bytes).toFixed(2) : "0.00";

  const latest5 = [...(eventsRes?.events ?? [])].slice(-5).reverse();

  return (
    <div className="space-y-6">
      {/* Top Action Bar */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Cluster Overview</h1>
          <p className="text-xs text-slate-500">
            Live node liveness, replication health bar, storage amplification, and transition events (1.5s polling)
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2.5">
          <button
            type="button"
            onClick={() => setDemoOpen(true)}
            className="inline-flex items-center gap-1.5 rounded-lg border border-slate-300 bg-white px-3.5 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-100"
          >
            <Terminal className="h-4 w-4" />
            <span>Demo Commands</span>
          </button>

          <button
            type="button"
            onClick={handleScrub}
            disabled={scrubbing}
            className="inline-flex items-center gap-1.5 rounded-lg bg-slate-900 px-4 py-2 text-xs font-semibold text-white hover:bg-slate-800 disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${scrubbing ? "animate-spin" : ""}`} />
            <span>{scrubbing ? "Scrubbing Cluster..." : "Run Scrub Now"}</span>
          </button>
        </div>
      </div>

      {scrubResult && (
        <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-2.5 text-xs text-emerald-900">
          <div className="flex items-center gap-2">
            <ShieldCheck className="h-4 w-4 text-emerald-700" />
            <span>
              Scrub completed: checked <strong>{scrubResult.chunks_checked}</strong> chunk(s) across{" "}
              <strong>{scrubResult.workers_scrubbed}</strong> worker(s), corrupt found:{" "}
              <strong>{scrubResult.corrupt_found}</strong>
              {scrubResult.repairs_triggered ? " (repair triggered)" : ""}.
            </span>
          </div>
          <button type="button" onClick={() => setScrubResult(null)} className="text-emerald-700 underline">
            Dismiss
          </button>
        </div>
      )}

      {scrubError && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-2.5 text-xs text-rose-800">
          {scrubError}
        </div>
      )}

      {/* 4 Worker Cards */}
      <section aria-label="Storage Workers">
        <div className="mb-2 flex items-center justify-between">
          <h2 className="text-sm font-semibold text-slate-700">Storage Workers ({workers.length})</h2>
          <span className="text-xs text-slate-500">
            Heartbeat timeout: {cfg?.heartbeat_timeout ?? 15}s · Repair grace: {cfg?.repair_grace_period ?? 30}s
          </span>
        </div>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {workers.map((w) => {
            const isDead = w.status === "DEAD";
            const hbAge = Math.max(0, Math.round(nowSec - w.last_heartbeat));
            const deadFor = w.dead_since !== null && w.dead_since !== undefined ? Math.max(0, Math.round(nowSec - w.dead_since)) : null;
            const graceSec = cfg?.repair_grace_period ?? 30;
            const repairStartsIn = deadFor !== null ? Math.max(0, Math.round(graceSec - deadFor)) : null;
            const isFlashing = Boolean(highlighted[w.worker_id]);

            return (
              <div
                key={w.worker_id}
                className={`rounded-xl border p-4 transition-all duration-300 ${
                  isDead
                    ? "border-rose-300 bg-rose-50/80 shadow-sm"
                    : "border-slate-200 bg-white shadow-xs"
                } ${isFlashing ? "ring-2 ring-amber-400" : ""}`}
              >
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="text-sm font-bold text-slate-900">{w.name}</div>
                    <div className="font-mono text-[11px] text-slate-500">{w.address}</div>
                  </div>
                  <StatusBadge status={w.status} />
                </div>

                <div className="mt-4 space-y-1.5 text-xs">
                  <div className="flex items-center justify-between text-slate-600">
                    <span>Last heartbeat</span>
                    <span className="font-mono font-semibold text-slate-900" title={formatIsoUtc(w.last_heartbeat)}>
                      {hbAge}s ago
                    </span>
                  </div>
                  <div className="flex items-center justify-between text-slate-600">
                    <span>Chunks (OK / Corrupt)</span>
                    <span className="font-mono font-semibold text-slate-900">
                      {w.chunk_count}{" "}
                      {w.corrupt_replicas > 0 && (
                        <span className="text-rose-700">({w.corrupt_replicas} corrupt)</span>
                      )}
                    </span>
                  </div>
                  <div className="flex items-center justify-between text-slate-600">
                    <span>Used disk</span>
                    <span className="font-mono font-semibold text-slate-900">{formatBytes(w.used_bytes)}</span>
                  </div>
                </div>

                {isDead && (
                  <div className="mt-3 rounded-lg border border-rose-200 bg-rose-100/80 px-2.5 py-2 text-xs text-rose-900">
                    <div className="flex items-center justify-between font-medium">
                      <span>Dead for</span>
                      <span className="font-mono">{deadFor !== null ? formatDuration(deadFor) : "-"}</span>
                    </div>
                    <div className="mt-0.5 flex items-center justify-between text-[11px] text-rose-800">
                      <span>Repair countdown</span>
                      <span className="font-mono font-semibold">
                        {repairStartsIn !== null && repairStartsIn > 0
                          ? `starts in ${repairStartsIn}s`
                          : "repair active / eligible"}
                      </span>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </section>

      {/* Replication Health Bar */}
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-xs">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">Replication Health</h2>
            <p className="text-xs text-slate-500">
              Chunk durability across {totalChunks} committed chunk(s) at target R={cfg?.replication_factor ?? 2}
            </p>
          </div>
          {rep && rep.under_replicated === 0 && rep.unavailable === 0 && rep.corrupt === 0 ? (
            <StatusBadge status="HEALTHY" labelOverride="All Chunks Healthy" />
          ) : rep && (rep.unavailable > 0 || rep.unrecoverable > 0 || rep.corrupt > 0) ? (
            <StatusBadge status="CORRUPT" labelOverride="Attention Required" />
          ) : (
            <StatusBadge status="UNDER_REPLICATED" labelOverride="Re-Replicating / Degraded" />
          )}
        </div>

        <div className="mt-4 h-5 w-full overflow-hidden rounded-full bg-slate-100">
          {totalChunks === 0 ? (
            <div className="flex h-full items-center justify-center text-[11px] font-medium text-slate-500">
              No committed chunks stored yet
            </div>
          ) : (
            <div className="flex h-full w-full">
              {segments.map((seg) => {
                if (seg.count <= 0) return null;
                const pct = (seg.count / totalChunks) * 100;
                return (
                  <div
                    key={seg.key}
                    style={{ width: `${pct}%` }}
                    className={`${seg.bar} transition-all duration-300`}
                    title={`${seg.label}: ${seg.count} (${pct.toFixed(1)}%)`}
                  />
                );
              })}
            </div>
          )}
        </div>

        <div className="mt-3 flex flex-wrap gap-4 text-xs">
          {segments.map((seg) => {
            const pct = totalChunks > 0 ? ((seg.count / totalChunks) * 100).toFixed(1) : "0.0";
            return (
              <div key={seg.key} className="flex items-center gap-1.5 text-slate-700">
                <span className={`inline-block h-2.5 w-2.5 rounded-full ${seg.dot}`} aria-hidden="true" />
                <span className="font-medium">{seg.label}:</span>
                <span className="font-mono font-semibold text-slate-900">
                  {seg.count} ({pct}%)
                </span>
              </div>
            );
          })}
        </div>
      </section>

      {/* Cluster Totals & Config Strip */}
      <section className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="flex items-center gap-2 text-xs font-medium text-slate-500">
            <Database className="h-4 w-4 text-slate-400" />
            <span>Committed Objects &amp; Chunks</span>
          </div>
          <div className="mt-2 text-2xl font-bold text-slate-900">{totals?.objects ?? 0} objects</div>
          <div className="mt-0.5 text-xs text-slate-600">{totals?.chunks ?? 0} chunks total</div>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="flex items-center gap-2 text-xs font-medium text-slate-500">
            <HardDrive className="h-4 w-4 text-slate-400" />
            <span>Logical vs Raw Stored Size</span>
          </div>
          <div className="mt-2 text-lg font-bold text-slate-900">
            {formatBytes(totals?.logical_bytes ?? 0)} <span className="text-xs font-normal text-slate-500">logical</span>
          </div>
          <div className="mt-0.5 text-xs text-slate-600">
            {formatBytes(totals?.raw_bytes ?? 0)} raw ({replicationRatio}x footprint)
          </div>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="flex items-center gap-2 text-xs font-medium text-slate-500">
            <ShieldCheck className="h-4 w-4 text-slate-400" />
            <span>Last Cluster Scrub</span>
          </div>
          <div
            className="mt-2 text-lg font-bold text-slate-900"
            title={scrub?.last_scrub_at ? formatIsoUtc(scrub.last_scrub_at) : "Not yet run"}
          >
            {scrub?.last_scrub_at ? formatRelativeTime(scrub.last_scrub_at, nowSec) : "Not run yet"}
          </div>
          <div className="mt-0.5 text-xs text-slate-600">
            {scrub?.chunks_checked ?? 0} checked · {scrub?.corrupt_found ?? 0} corrupt
          </div>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="flex items-center gap-2 text-xs font-medium text-slate-500">
            <Cpu className="h-4 w-4 text-slate-400" />
            <span>Active Cluster Config</span>
          </div>
          <div className="mt-2 flex flex-wrap gap-1.5 text-[11px]">
            <span className="rounded bg-slate-100 px-2 py-0.5 font-mono text-slate-800">
              R={cfg?.replication_factor ?? 2}
            </span>
            <span className="rounded bg-slate-100 px-2 py-0.5 font-mono text-slate-800">
              chunk={formatBytes(cfg?.chunk_size ?? 1048576)}
            </span>
            <span className="rounded bg-slate-100 px-2 py-0.5 font-mono text-slate-800">
              hb={cfg?.heartbeat_timeout ?? 15}s
            </span>
            <span className="rounded bg-slate-100 px-2 py-0.5 font-mono text-slate-800">
              grace={cfg?.repair_grace_period ?? 30}s
            </span>
            <span className="rounded bg-slate-100 px-2 py-0.5 font-mono text-slate-800">
              scrub={cfg?.scrub_interval ?? 300}s
            </span>
          </div>
        </div>
      </section>

      {/* Latest 5 Events Mini-Feed */}
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-xs">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Clock className="h-4 w-4 text-slate-500" />
            <h2 className="text-sm font-semibold text-slate-900">Latest 5 State Events</h2>
          </div>
          <Link to="/events" className="inline-flex items-center gap-1 text-xs font-semibold text-blue-700 hover:underline">
            <span>View Full Event Stream</span>
            <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        </div>

        {latest5.length === 0 ? (
          <p className="mt-3 text-xs text-slate-500">No state transitions recorded yet.</p>
        ) : (
          <div className="mt-3 divide-y divide-slate-100">
            {latest5.map((ev) => (
              <div key={ev.id} className="flex flex-wrap items-center justify-between gap-2 py-2.5 text-xs">
                <div className="flex items-center gap-2.5">
                  <StatusBadge status={ev.severity} />
                  <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
                    {ev.type}
                  </span>
                  <span className="text-slate-800">{ev.message}</span>
                </div>
                <span className="font-mono text-slate-500" title={formatIsoUtc(ev.ts)}>
                  #{ev.id} · {formatRelativeTime(ev.ts, nowSec)}
                </span>
              </div>
            ))}
          </div>
        )}
      </section>

      <DemoCommandsModal open={demoOpen} onClose={() => setDemoOpen(false)} />
    </div>
  );
}
