import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowDownCircle, Pause, Play, RotateCcw } from "lucide-react";
import { api, type EventItem } from "../api/client";
import { POLL_INTERVAL_MS, useClusterSync, useServerClock } from "../api/hooks";
import { formatIsoUtc, formatRelativeTime } from "../utils/format";
import { StatusBadge } from "../components/StatusBadge";

export function EventsPage() {
  const { reportSuccess, reportFailure } = useClusterSync();
  const nowSec = useServerClock(1000);

  const [events, setEvents] = useState<EventItem[]>([]);
  const [sinceId, setSinceId] = useState(0);
  const [paused, setPaused] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [severityFilter, setSeverityFilter] = useState<string>("ALL");
  const [typeFilter, setTypeFilter] = useState<string>("ALL");

  const bottomRef = useRef<HTMLDivElement | null>(null);
  const sinceRef = useRef(0);
  sinceRef.current = sinceId;

  const { data } = useQuery({
    queryKey: ["admin", "events", "stream"],
    queryFn: async () => {
      try {
        const res = await api.getEvents(sinceRef.current, 200);
        reportSuccess();
        if (res.events.length > 0) {
          setSinceId(res.latest_id);
          setEvents((prev) => {
            const seen = new Set(prev.map((e) => e.id));
            const merged = [...prev];
            for (const ev of res.events) {
              if (!seen.has(ev.id)) merged.push(ev);
            }
            return merged.slice(-1000);
          });
        } else if (res.latest_id > sinceRef.current) {
          setSinceId(res.latest_id);
        }
        return res;
      } catch (err) {
        reportFailure(err);
        throw err;
      }
    },
    refetchInterval: paused ? false : POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    retry: false,
  });

  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [events.length, autoScroll]);

  const distinctTypes = Array.from(new Set(events.map((e) => e.type))).sort();

  const filtered = events.filter((ev) => {
    if (severityFilter !== "ALL" && ev.severity !== severityFilter) return false;
    if (typeFilter !== "ALL" && ev.type !== typeFilter) return false;
    return true;
  });

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Cluster Events Stream</h1>
          <p className="text-xs text-slate-500">
            Incremental <code>?since={sinceId}</code> polling · Retaining {events.length} cached state transitions (latest ID:{" "}
            {data?.latest_id ?? sinceId})
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => setPaused((p) => !p)}
            className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-semibold ${
              paused
                ? "border-amber-300 bg-amber-50 text-amber-900"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100"
            }`}
          >
            {paused ? <Play className="h-3.5 w-3.5" /> : <Pause className="h-3.5 w-3.5" />}
            <span>{paused ? "Resume Polling" : "Pause Polling"}</span>
          </button>

          <button
            type="button"
            onClick={() => setAutoScroll((a) => !a)}
            className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-semibold ${
              autoScroll
                ? "border-blue-300 bg-blue-50 text-blue-800"
                : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100"
            }`}
          >
            <ArrowDownCircle className="h-3.5 w-3.5" />
            <span>Auto-scroll: {autoScroll ? "ON" : "OFF"}</span>
          </button>

          <button
            type="button"
            onClick={() => {
              setEvents([]);
              setSinceId(0);
            }}
            className="inline-flex items-center gap-1.5 rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-100"
          >
            <RotateCcw className="h-3.5 w-3.5" />
            <span>Reload All</span>
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 rounded-xl border border-slate-200 bg-white p-3.5 text-xs">
        <div className="flex items-center gap-1.5">
          <span className="font-semibold text-slate-600">Severity:</span>
          {["ALL", "INFO", "WARN", "ERROR"].map((sev) => (
            <button
              key={sev}
              type="button"
              onClick={() => setSeverityFilter(sev)}
              className={`rounded-md px-2.5 py-1 font-medium ${
                severityFilter === sev
                  ? "bg-slate-900 text-white"
                  : "bg-slate-100 text-slate-700 hover:bg-slate-200"
              }`}
            >
              {sev}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-2">
          <label htmlFor="event-type-filter" className="font-semibold text-slate-600">
            Event Type:
          </label>
          <select
            id="event-type-filter"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
            className="rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs"
          >
            <option value="ALL">All Types</option>
            {distinctTypes.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Event Stream List */}
      <div className="max-h-[65vh] overflow-y-auto rounded-xl border border-slate-200 bg-white shadow-xs">
        {filtered.length === 0 ? (
          <div className="p-8 text-center text-xs text-slate-500">No matching events yet.</div>
        ) : (
          <div className="divide-y divide-slate-100">
            {filtered.map((ev) => (
              <div key={ev.id} className="flex flex-wrap items-start justify-between gap-3 px-4 py-3 text-xs hover:bg-slate-50/80">
                <div className="space-y-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-mono text-[11px] text-slate-400">#{ev.id}</span>
                    <StatusBadge status={ev.severity} />
                    <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] font-medium text-slate-800">
                      {ev.type}
                    </span>
                    <span className="font-medium text-slate-900">{ev.message}</span>
                  </div>
                  {ev.data && Object.keys(ev.data).length > 0 && (
                    <div className="font-mono text-[11px] text-slate-500">{JSON.stringify(ev.data)}</div>
                  )}
                </div>

                <span
                  className="shrink-0 font-mono text-[11px] text-slate-500"
                  title={formatIsoUtc(ev.ts)}
                >
                  {formatRelativeTime(ev.ts, nowSec)}
                </span>
              </div>
            ))}
            <div ref={bottomRef} />
          </div>
        )}
      </div>
    </div>
  );
}
