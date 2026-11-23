import { useRef, useState } from "react";
import { Link } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { Check, Copy, Download, FileUp, Plus, Trash2, UploadCloud, X } from "lucide-react";
import { api, ApiRequestError, uploadFileWithProgress, type ObjectRow } from "../api/client";
import { useObjectsList, useServerClock } from "../api/hooks";
import { formatBytes, formatIsoUtc, formatRelativeTime, shortId } from "../utils/format";
import { StatusBadge } from "../components/StatusBadge";

interface UploadQueueItem {
  localId: string;
  file: File;
  loaded: number;
  total: number;
  status: "uploading" | "completed" | "failed";
  error?: string;
  objectId?: string;
  abort: () => void;
}

interface MetaKV {
  key: string;
  value: string;
}

const PAGE_LIMIT = 20;

export function ObjectsPage() {
  const queryClient = useQueryClient();
  const nowSec = useServerClock(1000);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const [offset, setOffset] = useState(0);
  const { data, isLoading, error } = useObjectsList(PAGE_LIMIT, offset);

  const [dragActive, setDragActive] = useState(false);
  const [metaPairs, setMetaPairs] = useState<MetaKV[]>([]);
  const [queue, setQueue] = useState<UploadQueueItem[]>([]);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ObjectRow | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const startUploads = (files: FileList | File[]) => {
    const metaObj: Record<string, string> = {};
    for (const pair of metaPairs) {
      const k = pair.key.trim();
      if (k) metaObj[k] = pair.value;
    }

    Array.from(files).forEach((file, idx) => {
      const localId = `${Date.now()}_${idx}_${file.name}`;
      const handle = uploadFileWithProgress(file, metaObj, (loaded, total) => {
        setQueue((cur) =>
          cur.map((item) => (item.localId === localId ? { ...item, loaded, total } : item))
        );
      });

      const entry: UploadQueueItem = {
        localId,
        file,
        loaded: 0,
        total: file.size || 1,
        status: "uploading",
        abort: handle.abort,
      };
      setQueue((cur) => [entry, ...cur]);

      handle.promise
        .then((res) => {
          setQueue((cur) =>
            cur.map((item) =>
              item.localId === localId
                ? { ...item, status: "completed", loaded: file.size, total: file.size, objectId: res.object_id }
                : item
            )
          );
          queryClient.invalidateQueries({ queryKey: ["objects"] });
          queryClient.invalidateQueries({ queryKey: ["admin", "overview"] });
        })
        .catch((err) => {
          const msg =
            err instanceof ApiRequestError
              ? `${err.status ? `HTTP ${err.status} ` : ""}[${err.code}] ${err.message}`
              : "Upload failed";
          setQueue((cur) =>
            cur.map((item) => (item.localId === localId ? { ...item, status: "failed", error: msg } : item))
          );
        });
    });
  };

  const handleCopyId = async (id: string) => {
    try {
      await navigator.clipboard.writeText(id);
      setCopiedId(id);
      window.setTimeout(() => setCopiedId(null), 1500);
    } catch {
      // ignore
    }
  };

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    setDeleteError(null);
    try {
      await api.deleteObject(deleteTarget.object_id);
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: ["objects"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "overview"] });
    } catch (err) {
      setDeleteError(err instanceof ApiRequestError ? `${err.code}: ${err.message}` : "Delete failed");
    } finally {
      setDeleting(false);
    }
  };

  const objects = data?.objects ?? [];
  const total = data?.total ?? 0;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold text-slate-900">Stored Objects &amp; Uploads</h1>
        <p className="text-xs text-slate-500">
          Streaming multipart upload with real-time XHR progress, custom metadata attachment, and paginated object catalog
        </p>
      </div>

      {/* Multi-File Drag-and-Drop Upload Panel */}
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-xs">
        <div
          onDragOver={(e) => {
            e.preventDefault();
            setDragActive(true);
          }}
          onDragLeave={() => setDragActive(false)}
          onDrop={(e) => {
            e.preventDefault();
            setDragActive(false);
            if (e.dataTransfer.files?.length) startUploads(e.dataTransfer.files);
          }}
          className={`flex flex-col items-center justify-center rounded-xl border-2 border-dashed p-6 text-center transition-colors ${
            dragActive ? "border-blue-500 bg-blue-50/70" : "border-slate-300 bg-slate-50/60 hover:bg-slate-50"
          }`}
        >
          <UploadCloud className="h-9 w-9 text-slate-500" />
          <p className="mt-2 text-sm font-semibold text-slate-900">
            Drag &amp; drop files here to stream into the cluster (R=2)
          </p>
          <p className="mt-0.5 text-xs text-slate-500">
            Supports multiple files simultaneously; chunks are streamed in 1 MiB stripes
          </p>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            onChange={(e) => {
              if (e.target.files?.length) {
                startUploads(e.target.files);
                e.target.value = "";
              }
            }}
          />
          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            className="mt-3 inline-flex items-center gap-1.5 rounded-lg bg-slate-900 px-4 py-2 text-xs font-semibold text-white hover:bg-slate-800"
          >
            <FileUp className="h-4 w-4" />
            <span>Select Files</span>
          </button>
        </div>

        {/* Optional Custom Metadata Pairs for New Uploads */}
        <div className="mt-4 border-t border-slate-100 pt-4">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-700">Custom Upload Metadata (Optional)</span>
            <button
              type="button"
              onClick={() => setMetaPairs((cur) => [...cur, { key: "", value: "" }])}
              className="inline-flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
            >
              <Plus className="h-3.5 w-3.5" />
              <span>Add Key-Value</span>
            </button>
          </div>
          {metaPairs.length > 0 && (
            <div className="mt-2 space-y-2">
              {metaPairs.map((pair, idx) => (
                <div key={idx} className="flex items-center gap-2">
                  <input
                    type="text"
                    placeholder="key"
                    value={pair.key}
                    onChange={(e) =>
                      setMetaPairs((cur) => cur.map((p, i) => (i === idx ? { ...p, key: e.target.value } : p)))
                    }
                    className="w-44 rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs"
                  />
                  <input
                    type="text"
                    placeholder="value"
                    value={pair.value}
                    onChange={(e) =>
                      setMetaPairs((cur) => cur.map((p, i) => (i === idx ? { ...p, value: e.target.value } : p)))
                    }
                    className="flex-1 rounded-lg border border-slate-300 px-2.5 py-1.5 text-xs"
                  />
                  <button
                    type="button"
                    onClick={() => setMetaPairs((cur) => cur.filter((_, i) => i !== idx))}
                    className="rounded p-1.5 text-slate-500 hover:bg-slate-100"
                    aria-label="Remove metadata row"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Active / Recent Upload Progress Queue */}
        {queue.length > 0 && (
          <div className="mt-4 space-y-2 border-t border-slate-100 pt-4">
            <div className="flex items-center justify-between">
              <span className="text-xs font-semibold text-slate-700">Upload Transfers ({queue.length})</span>
              <button
                type="button"
                onClick={() => setQueue((cur) => cur.filter((x) => x.status === "uploading"))}
                className="text-xs text-slate-500 hover:underline"
              >
                Clear finished
              </button>
            </div>
            {queue.map((item) => {
              const pct = item.total > 0 ? Math.min(100, Math.round((item.loaded / item.total) * 100)) : 100;
              return (
                <div key={item.localId} className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="flex items-center gap-2 font-medium text-slate-900">
                      <span>{item.file.name}</span>
                      <span className="text-slate-500">({formatBytes(item.file.size)})</span>
                    </div>
                    <div className="flex items-center gap-2">
                      {item.status === "uploading" && (
                        <>
                          <span className="font-mono text-slate-700">{pct}%</span>
                          <button
                            type="button"
                            onClick={item.abort}
                            className="rounded border border-rose-300 bg-white px-2 py-0.5 text-[11px] font-medium text-rose-700 hover:bg-rose-50"
                          >
                            Cancel
                          </button>
                        </>
                      )}
                      {item.status === "completed" && item.objectId && (
                        <>
                          <StatusBadge status="COMMITTED" />
                          <Link
                            to={`/objects/${item.objectId}`}
                            className="font-semibold text-blue-700 hover:underline"
                          >
                            Inspect Chunk Map
                          </Link>
                        </>
                      )}
                      {item.status === "failed" && <StatusBadge status="ERROR" labelOverride="Failed" />}
                    </div>
                  </div>
                  <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-slate-200">
                    <div
                      className={`h-full transition-all ${
                        item.status === "failed"
                          ? "bg-rose-500"
                          : item.status === "completed"
                            ? "bg-emerald-500"
                            : "bg-blue-600"
                      }`}
                      style={{ width: `${item.status === "completed" ? 100 : pct}%` }}
                    />
                  </div>
                  {item.error && <div className="mt-1.5 font-mono text-[11px] text-rose-700">{item.error}</div>}
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* Objects Catalog Table */}
      <section className="rounded-xl border border-slate-200 bg-white shadow-xs">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-5 py-3.5">
          <h2 className="text-sm font-semibold text-slate-900">Objects ({total})</h2>
          <div className="flex items-center gap-2 text-xs text-slate-600">
            <span>
              Showing {total === 0 ? 0 : offset + 1}–{Math.min(offset + PAGE_LIMIT, total)} of {total}
            </span>
            <button
              type="button"
              disabled={offset === 0}
              onClick={() => setOffset((o) => Math.max(0, o - PAGE_LIMIT))}
              className="rounded border border-slate-300 px-2.5 py-1 font-medium disabled:opacity-40"
            >
              Prev
            </button>
            <button
              type="button"
              disabled={offset + PAGE_LIMIT >= total}
              onClick={() => setOffset((o) => o + PAGE_LIMIT)}
              className="rounded border border-slate-300 px-2.5 py-1 font-medium disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </div>

        {error && !data ? (
          <div className="p-5 text-xs text-rose-700">Failed to load objects list.</div>
        ) : isLoading && !data ? (
          <div className="p-6 text-center text-xs text-slate-500">Loading objects...</div>
        ) : objects.length === 0 ? (
          <div className="p-8 text-center text-xs text-slate-500">
            No committed objects stored yet. Drag a file above to upload.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-200 bg-slate-50 text-[11px] uppercase tracking-wider text-slate-500">
                <tr>
                  <th className="px-4 py-3">Name</th>
                  <th className="px-4 py-3">Object ID</th>
                  <th className="px-4 py-3">Size</th>
                  <th className="px-4 py-3">Chunks</th>
                  <th className="px-4 py-3">Content Type</th>
                  <th className="px-4 py-3">Created</th>
                  <th className="px-4 py-3 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {objects.map((obj) => (
                  <tr key={obj.object_id} className="hover:bg-slate-50/80">
                    <td className="px-4 py-3 font-medium">
                      <Link to={`/objects/${obj.object_id}`} className="text-blue-700 hover:underline">
                        {obj.name}
                      </Link>
                    </td>
                    <td className="px-4 py-3 font-mono text-slate-600">
                      <div className="inline-flex items-center gap-1.5">
                        <span title={obj.object_id}>{shortId(obj.object_id)}</span>
                        <button
                          type="button"
                          onClick={() => handleCopyId(obj.object_id)}
                          className="rounded p-1 text-slate-400 hover:bg-slate-200 hover:text-slate-700"
                          title="Copy full UUID"
                        >
                          {copiedId === obj.object_id ? (
                            <Check className="h-3.5 w-3.5 text-emerald-600" />
                          ) : (
                            <Copy className="h-3.5 w-3.5" />
                          )}
                        </button>
                      </div>
                    </td>
                    <td className="px-4 py-3 font-mono text-slate-800">{formatBytes(obj.size)}</td>
                    <td className="px-4 py-3 font-mono text-slate-700">{obj.chunk_count}</td>
                    <td className="px-4 py-3 text-slate-600">{obj.content_type || "application/octet-stream"}</td>
                    <td className="px-4 py-3 text-slate-600" title={formatIsoUtc(obj.created_at)}>
                      {formatRelativeTime(obj.created_at, nowSec)}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="inline-flex items-center gap-2">
                        <a
                          href={api.downloadUrl(obj.object_id)}
                          download={obj.name}
                          className="inline-flex items-center gap-1 rounded border border-slate-300 bg-white px-2.5 py-1 text-[11px] font-medium text-slate-700 hover:bg-slate-100"
                        >
                          <Download className="h-3.5 w-3.5" />
                          <span>Download</span>
                        </a>
                        <button
                          type="button"
                          onClick={() => {
                            setDeleteError(null);
                            setDeleteTarget(obj);
                          }}
                          className="inline-flex items-center gap-1 rounded border border-rose-200 bg-rose-50 px-2.5 py-1 text-[11px] font-medium text-rose-700 hover:bg-rose-100"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                          <span>Delete</span>
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Delete Confirmation Modal */}
      {deleteTarget && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-4"
          role="dialog"
          aria-modal="true"
        >
          <div className="w-full max-w-md rounded-xl border border-slate-200 bg-white p-5 shadow-xl">
            <h3 className="text-base font-bold text-slate-900">Delete Object?</h3>
            <p className="mt-2 text-xs text-slate-600">
              This triggers two-phase deletion of <strong>{deleteTarget.name}</strong> (
              <code>{shortId(deleteTarget.object_id)}</code>) and removes all {deleteTarget.chunk_count} chunk(s) across workers.
            </p>
            {deleteError && (
              <div className="mt-3 rounded border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-800">
                {deleteError}
              </div>
            )}
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                disabled={deleting}
                onClick={() => setDeleteTarget(null)}
                className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={deleting}
                onClick={confirmDelete}
                className="rounded-lg bg-rose-600 px-3.5 py-1.5 text-xs font-semibold text-white hover:bg-rose-700 disabled:opacity-50"
              >
                {deleting ? "Deleting..." : "Confirm Delete"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
