import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Check, Copy, Download, Edit3, Plus, Save, Trash2, X } from "lucide-react";
import { api, ApiRequestError, type ChunkDetail } from "../api/client";
import { useObjectChunks, useObjectMetadata } from "../api/hooks";
import { formatBytes, formatIsoUtc, getWorkerStyle } from "../utils/format";
import { StatusBadge } from "../components/StatusBadge";

const CHUNKS_PER_PAGE = 1024;

function classifyChunk(chunk: ChunkDetail): "HEALTHY" | "UNDER_REPLICATED" | "CORRUPT" {
  const validAlive = chunk.replicas.filter((r) => r.state === "OK" && r.worker_status === "ALIVE").length;
  const hasCorrupt = chunk.replicas.some((r) => r.state === "CORRUPT");
  if (validAlive === 0 || hasCorrupt) return "CORRUPT";
  if (validAlive < 2) return "UNDER_REPLICATED";
  return "HEALTHY";
}

export function ObjectDetailPage() {
  const { id = "" } = useParams<{ id: string }>();
  const queryClient = useQueryClient();

  const { data: meta, isLoading: metaLoading, error: metaError } = useObjectMetadata(id);
  const { data: chunksData, error: chunksError } = useObjectChunks(id);

  const [editingMeta, setEditingMeta] = useState(false);
  const [draftMeta, setDraftMeta] = useState<{ key: string; value: string; deleted?: boolean }[]>([]);
  const [savingMeta, setSavingMeta] = useState(false);
  const [metaSaveMsg, setMetaSaveMsg] = useState<string | null>(null);
  const [metaSaveErr, setMetaSaveErr] = useState<string | null>(null);

  const [workerFilter, setWorkerFilter] = useState<string | null>(null);
  const [chunkPage, setChunkPage] = useState(0);
  const [selectedChunk, setSelectedChunk] = useState<ChunkDetail | null>(null);
  const [copiedCmd, setCopiedCmd] = useState(false);

  const openEditor = () => {
    const entries = Object.entries(meta?.custom ?? {}).map(([key, value]) => ({ key, value, deleted: false }));
    setDraftMeta(entries.length > 0 ? entries : [{ key: "", value: "", deleted: false }]);
    setMetaSaveErr(null);
    setMetaSaveMsg(null);
    setEditingMeta(true);
  };

  const saveMetaPatch = async () => {
    if (!meta) return;
    setSavingMeta(true);
    setMetaSaveErr(null);
    try {
      const patch: Record<string, string | null> = {};
      for (const k of Object.keys(meta.custom)) {
        const match = draftMeta.find((d) => d.key.trim() === k);
        if (!match || match.deleted) {
          patch[k] = null;
        }
      }
      for (const row of draftMeta) {
        const k = row.key.trim();
        if (!k || row.deleted) continue;
        patch[k] = row.value;
      }
      const updated = await api.patchObjectMetadata(meta.object_id, patch);
      setEditingMeta(false);
      setMetaSaveMsg(`Metadata updated (modified_at: ${formatIsoUtc(updated.modified_at)} — checksum & chunks untouched)`);
      queryClient.invalidateQueries({ queryKey: ["objects", "metadata", id] });
    } catch (err) {
      setMetaSaveErr(err instanceof ApiRequestError ? `${err.code}: ${err.message}` : "Patch failed");
    } finally {
      setSavingMeta(false);
    }
  };

  const chunks = chunksData?.chunks ?? [];
  const totalPages = Math.max(1, Math.ceil(chunks.length / CHUNKS_PER_PAGE));
  const safePage = Math.min(chunkPage, totalPages - 1);
  const pageChunks = chunks.slice(safePage * CHUNKS_PER_PAGE, (safePage + 1) * CHUNKS_PER_PAGE);

  // Keep selected chunk synchronized with live 1.5s polling updates
  useEffect(() => {
    if (!selectedChunk) return;
    const latest = chunks.find((c) => c.chunk_id === selectedChunk.chunk_id);
    if (latest) setSelectedChunk(latest);
  }, [chunksData]); // eslint-disable-line react-hooks/exhaustive-deps

  if (metaLoading && !meta) {
    return <div className="rounded-xl border border-slate-200 bg-white p-8 text-center text-sm text-slate-500">Loading object details...</div>;
  }

  if (metaError && !meta) {
    return (
      <div className="space-y-3">
        <Link to="/objects" className="inline-flex items-center gap-1 text-xs font-semibold text-blue-700 hover:underline">
          <ArrowLeft className="h-3.5 w-3.5" /> Back to Objects
        </Link>
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-6 text-sm text-rose-800">
          Object not found or unreachable: {metaError instanceof Error ? metaError.message : "Unknown error"}
        </div>
      </div>
    );
  }

  if (!meta) return null;

  const knownWorkers = Array.from(
    new Set(chunks.flatMap((c) => c.replicas.map((r) => r.worker_id))).add("worker1").add("worker2").add("worker3").add("worker4")
  ).sort();

  const healthyCount = chunks.filter((c) => classifyChunk(c) === "HEALTHY").length;
  const degradedCount = chunks.filter((c) => classifyChunk(c) === "UNDER_REPLICATED").length;
  const corruptCount = chunks.filter((c) => classifyChunk(c) === "CORRUPT").length;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <Link to="/objects" className="inline-flex items-center gap-1 text-xs font-semibold text-blue-700 hover:underline">
            <ArrowLeft className="h-3.5 w-3.5" /> Back to Objects
          </Link>
          <h1 className="mt-1 text-xl font-bold text-slate-900">{meta.name}</h1>
          <p className="font-mono text-xs text-slate-500">{meta.object_id}</p>
        </div>

        <a
          href={api.downloadUrl(meta.object_id)}
          download={meta.name}
          className="inline-flex items-center gap-1.5 rounded-lg bg-slate-900 px-4 py-2 text-xs font-semibold text-white hover:bg-slate-800"
        >
          <Download className="h-4 w-4" />
          <span>Download ({formatBytes(meta.size)})</span>
        </a>
      </div>

      {/* System Metadata + Inline Editable Custom Metadata */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-xs">
          <h2 className="text-sm font-semibold text-slate-900">System Metadata (Immutable Core)</h2>
          <dl className="mt-3 grid grid-cols-1 gap-2.5 text-xs sm:grid-cols-2">
            <div className="rounded-lg bg-slate-50 p-2.5">
              <dt className="text-slate-500">Size</dt>
              <dd className="mt-0.5 font-mono font-semibold text-slate-900">{formatBytes(meta.size)} ({meta.size} B)</dd>
            </div>
            <div className="rounded-lg bg-slate-50 p-2.5">
              <dt className="text-slate-500">Chunks / Content-Type</dt>
              <dd className="mt-0.5 font-mono font-semibold text-slate-900">{meta.chunk_count} · {meta.content_type}</dd>
            </div>
            <div className="rounded-lg bg-slate-50 p-2.5">
              <dt className="text-slate-500">Created At</dt>
              <dd className="mt-0.5 font-mono text-slate-800">{formatIsoUtc(meta.created_at)}</dd>
            </div>
            <div className="rounded-lg bg-slate-50 p-2.5">
              <dt className="text-slate-500">Modified At</dt>
              <dd className="mt-0.5 font-mono font-semibold text-slate-900">{formatIsoUtc(meta.modified_at)}</dd>
            </div>
            <div className="rounded-lg bg-slate-50 p-2.5 sm:col-span-2">
              <dt className="text-slate-500">Whole-Object SHA-256</dt>
              <dd className="mt-0.5 break-all font-mono text-[11px] text-slate-800">{meta.checksum}</dd>
            </div>
          </dl>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-xs">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-900">Custom Metadata (PATCH without touching chunks)</h2>
            {!editingMeta && (
              <button
                type="button"
                onClick={openEditor}
                className="inline-flex items-center gap-1.5 rounded-lg border border-slate-300 px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
              >
                <Edit3 className="h-3.5 w-3.5" />
                <span>Edit</span>
              </button>
            )}
          </div>

          {metaSaveMsg && (
            <div className="mt-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-900">
              {metaSaveMsg}
            </div>
          )}
          {metaSaveErr && (
            <div className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-800">
              {metaSaveErr}
            </div>
          )}

          {editingMeta ? (
            <div className="mt-3 space-y-2.5">
              {draftMeta.map((row, idx) => (
                <div key={idx} className="flex items-center gap-2">
                  <input
                    type="text"
                    placeholder="key"
                    value={row.key}
                    disabled={row.deleted}
                    onChange={(e) =>
                      setDraftMeta((cur) => cur.map((r, i) => (i === idx ? { ...r, key: e.target.value } : r)))
                    }
                    className="w-36 rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs disabled:bg-slate-100"
                  />
                  <input
                    type="text"
                    placeholder="value"
                    value={row.deleted ? "(will delete key)" : row.value}
                    disabled={row.deleted}
                    onChange={(e) =>
                      setDraftMeta((cur) => cur.map((r, i) => (i === idx ? { ...r, value: e.target.value } : r)))
                    }
                    className="flex-1 rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs disabled:bg-rose-50 disabled:text-rose-700"
                  />
                  <button
                    type="button"
                    onClick={() =>
                      setDraftMeta((cur) =>
                        cur.map((r, i) => (i === idx ? { ...r, deleted: !r.deleted } : r))
                      )
                    }
                    className="rounded border border-slate-300 p-1.5 text-slate-600 hover:bg-slate-100"
                    title="Toggle delete key"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </div>
              ))}
              <div className="flex flex-wrap items-center justify-between gap-2 pt-2">
                <button
                  type="button"
                  onClick={() => setDraftMeta((cur) => [...cur, { key: "", value: "", deleted: false }])}
                  className="inline-flex items-center gap-1 rounded border border-slate-300 px-2.5 py-1 text-xs font-medium text-slate-700"
                >
                  <Plus className="h-3.5 w-3.5" /> Add Row
                </button>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setEditingMeta(false)}
                    className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    disabled={savingMeta}
                    onClick={saveMetaPatch}
                    className="inline-flex items-center gap-1.5 rounded-lg bg-slate-900 px-3.5 py-1.5 text-xs font-semibold text-white hover:bg-slate-800 disabled:opacity-50"
                  >
                    <Save className="h-3.5 w-3.5" />
                    <span>{savingMeta ? "Saving..." : "Save PATCH"}</span>
                  </button>
                </div>
              </div>
            </div>
          ) : Object.keys(meta.custom).length === 0 ? (
            <p className="mt-4 text-xs text-slate-500">No custom metadata keys attached.</p>
          ) : (
            <div className="mt-3 divide-y divide-slate-100 rounded-lg border border-slate-200">
              {Object.entries(meta.custom).map(([k, v]) => (
                <div key={k} className="flex items-center justify-between px-3 py-2 text-xs">
                  <span className="font-mono font-semibold text-slate-700">{k}</span>
                  <span className="font-mono text-slate-900">{v}</span>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>

      {/* Interactive Chunk Map */}
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-xs">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">
              Interactive Chunk Map ({chunks.length} chunk(s) · live 1.5s polling)
            </h2>
            <p className="mt-0.5 text-xs text-slate-500">
              Healthy: {healthyCount} · Under-Replicated: {degradedCount} · Corrupt/Unavailable: {corruptCount}
            </p>
          </div>

          {/* Worker Legend Filter Pills */}
          <div className="flex flex-wrap items-center gap-1.5 text-xs">
            <button
              type="button"
              onClick={() => setWorkerFilter(null)}
              className={`rounded-full border px-2.5 py-1 font-medium ${
                workerFilter === null
                  ? "border-slate-900 bg-slate-900 text-white"
                  : "border-slate-300 bg-white text-slate-700"
              }`}
            >
              All Workers
            </button>
            {knownWorkers.map((wid) => {
              const st = getWorkerStyle(wid);
              const active = workerFilter === wid;
              return (
                <button
                  key={wid}
                  type="button"
                  onClick={() => setWorkerFilter(active ? null : wid)}
                  className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 font-medium transition ${
                    active ? "border-slate-900 ring-2 ring-slate-300 " + st.pill : st.pill
                  }`}
                >
                  <span className={`inline-block h-2.5 w-2.5 rounded-full ${st.bg}`} />
                  <span>{wid}</span>
                </button>
              );
            })}
          </div>
        </div>

        {/* Visual State Key */}
        <div className="mt-3 flex flex-wrap gap-4 border-b border-slate-100 pb-3 text-[11px] text-slate-600">
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full bg-blue-600" />
            <span>Solid dot = OK + ALIVE</span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-dashed border-purple-600 bg-white" />
            <span>Hollow dashed dot = DEAD worker</span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-flex h-3 w-3 items-center justify-center rounded-full bg-rose-600 text-[8px] font-bold text-white">!</span>
            <span>Red marker = CORRUPT</span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full border border-dashed border-slate-400 bg-slate-100" />
            <span>Empty slot = Missing replica</span>
          </span>
        </div>

        {chunksError && !chunksData ? (
          <div className="mt-4 rounded border border-rose-200 bg-rose-50 p-3 text-xs text-rose-800">
            Admin chunks endpoint unavailable ({chunksError instanceof Error ? chunksError.message : "error"}).
          </div>
        ) : chunks.length === 0 ? (
          <div className="mt-4 text-xs text-slate-500">0-byte object (0 chunks).</div>
        ) : (
          <>
            {totalPages > 1 && (
              <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-600">
                <span>
                  Chunk range {safePage * CHUNKS_PER_PAGE}–
                  {Math.min((safePage + 1) * CHUNKS_PER_PAGE - 1, chunks.length - 1)} (grouped in pages of {CHUNKS_PER_PAGE})
                </span>
                <div className="flex gap-1.5">
                  <button
                    type="button"
                    disabled={safePage === 0}
                    onClick={() => setChunkPage((p) => Math.max(0, p - 1))}
                    className="rounded border border-slate-300 px-2.5 py-1 disabled:opacity-40"
                  >
                    Prev Page
                  </button>
                  <button
                    type="button"
                    disabled={safePage + 1 >= totalPages}
                    onClick={() => setChunkPage((p) => p + 1)}
                    className="rounded border border-slate-300 px-2.5 py-1 disabled:opacity-40"
                  >
                    Next Page
                  </button>
                </div>
              </div>
            )}

            <div className="mt-4 grid grid-cols-4 gap-2 sm:grid-cols-8 md:grid-cols-10 lg:grid-cols-12">
              {pageChunks.map((ch) => {
                const state = classifyChunk(ch);
                const matchesFilter =
                  workerFilter === null || ch.replicas.some((r) => r.worker_id === workerFilter);
                const isSelected = selectedChunk?.chunk_id === ch.chunk_id;

                const cellTheme =
                  state === "HEALTHY"
                    ? "border-emerald-200 bg-emerald-50/40"
                    : state === "UNDER_REPLICATED"
                      ? "border-amber-300 bg-amber-50"
                      : "border-rose-300 bg-rose-50";

                const slots: (typeof ch.replicas[number] | null)[] = [
                  ch.replicas[0] ?? null,
                  ch.replicas[1] ?? null,
                  ...ch.replicas.slice(2),
                ];

                return (
                  <button
                    key={ch.chunk_id}
                    type="button"
                    onClick={() => setSelectedChunk(ch)}
                    title={`Chunk #${ch.index} (${formatBytes(ch.size)}) — ${state}`}
                    className={`flex flex-col items-center justify-between rounded-lg border p-2 text-left transition ${cellTheme} ${
                      matchesFilter ? "opacity-100" : "opacity-25"
                    } ${isSelected ? "ring-2 ring-slate-900" : "hover:border-slate-400"}`}
                  >
                    <span className="font-mono text-[10px] font-semibold text-slate-700">#{ch.index}</span>
                    <div className="mt-1.5 flex items-center gap-1.5">
                      {slots.map((rep, slotIdx) => {
                        if (!rep) {
                          return (
                            <span
                              key={`empty-${slotIdx}`}
                              className="inline-block h-3 w-3 rounded-full border border-dashed border-slate-400 bg-slate-100"
                              title="Missing replica"
                            />
                          );
                        }
                        const st = getWorkerStyle(rep.worker_id);
                        if (rep.state === "CORRUPT") {
                          return (
                            <span
                              key={rep.worker_id}
                              className="inline-flex h-3.5 w-3.5 items-center justify-center rounded-full bg-rose-600 text-[9px] font-bold text-white"
                              title={`${rep.worker_id}: CORRUPT`}
                            >
                              !
                            </span>
                          );
                        }
                        if (rep.worker_status === "DEAD") {
                          return (
                            <span
                              key={rep.worker_id}
                              className={`inline-block h-3 w-3 rounded-full border-2 border-dashed bg-white ${st.border}`}
                              title={`${rep.worker_id}: worker DEAD`}
                            />
                          );
                        }
                        return (
                          <span
                            key={rep.worker_id}
                            className={`inline-block h-3 w-3 rounded-full ${st.bg}`}
                            title={`${rep.worker_id}: OK (ALIVE)`}
                          />
                        );
                      })}
                    </div>
                  </button>
                );
              })}
            </div>
          </>
        )}

        {/* Selected Chunk Inspection Drawer */}
        {selectedChunk && (
          <div className="mt-5 rounded-xl border border-slate-300 bg-slate-50 p-4 text-xs">
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="flex items-center gap-2">
                  <span className="text-sm font-bold text-slate-900">Chunk #{selectedChunk.index}</span>
                  <StatusBadge status={classifyChunk(selectedChunk)} />
                </div>
                <div className="mt-0.5 font-mono text-[11px] text-slate-600">{selectedChunk.chunk_id}</div>
              </div>
              <button
                type="button"
                onClick={() => setSelectedChunk(null)}
                className="rounded p-1 text-slate-500 hover:bg-slate-200"
                aria-label="Close chunk drawer"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="rounded-lg border border-slate-200 bg-white p-3">
                <div className="text-[11px] font-semibold uppercase text-slate-500">Chunk Checksum &amp; Size</div>
                <div className="mt-1 font-mono text-slate-900">{formatBytes(selectedChunk.size)} ({selectedChunk.size} B)</div>
                <div className="mt-1 break-all font-mono text-[11px] text-slate-600">{selectedChunk.checksum}</div>
              </div>

              <div className="rounded-lg border border-slate-200 bg-white p-3">
                <div className="text-[11px] font-semibold uppercase text-slate-500">Replicas ({selectedChunk.replicas.length})</div>
                <div className="mt-1.5 space-y-1.5">
                  {selectedChunk.replicas.map((r) => {
                    const st = getWorkerStyle(r.worker_id);
                    return (
                      <div key={r.worker_id} className="flex items-center justify-between">
                        <span className={`inline-flex items-center gap-1.5 rounded border px-2 py-0.5 font-mono text-[11px] ${st.pill}`}>
                          <span className={`h-2 w-2 rounded-full ${st.bg}`} />
                          {r.worker_id}
                        </span>
                        <div className="flex items-center gap-1.5">
                          <StatusBadge status={r.state} />
                          <StatusBadge status={r.worker_status} />
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>

            {selectedChunk.replicas.length > 0 && (
              <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2">
                <code className="text-[11px] text-slate-800">
                  bash scripts/corrupt_chunk.sh {selectedChunk.replicas[0].worker_id} {selectedChunk.chunk_id}
                </code>
                <button
                  type="button"
                  onClick={async () => {
                    try {
                      await navigator.clipboard.writeText(
                        `bash scripts/corrupt_chunk.sh ${selectedChunk.replicas[0].worker_id} ${selectedChunk.chunk_id}`
                      );
                      setCopiedCmd(true);
                      window.setTimeout(() => setCopiedCmd(false), 1500);
                    } catch {
                      // ignore
                    }
                  }}
                  className="inline-flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-[11px] font-medium text-slate-700 hover:bg-slate-100"
                >
                  {copiedCmd ? <Check className="h-3 w-3 text-emerald-600" /> : <Copy className="h-3 w-3" />}
                  <span>{copiedCmd ? "Copied" : "Copy Corrupt Demo Cmd"}</span>
                </button>
              </div>
            )}
          </div>
        )}
      </section>
    </div>
  );
}
