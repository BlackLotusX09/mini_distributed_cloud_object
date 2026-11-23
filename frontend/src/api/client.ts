import type { components } from "./schema";

export type OverviewResponse = components["schemas"]["OverviewResponse"];
export type OverviewWorker = components["schemas"]["OverviewWorker"];
export type OverviewReplication = components["schemas"]["OverviewReplication"];
export type OverviewTotals = components["schemas"]["OverviewTotals"];
export type ScrubSummary = components["schemas"]["ScrubSummary"];
export type ClusterConfig = components["schemas"]["ClusterConfig"];
export type EventItem = components["schemas"]["EventItem"];
export type EventsResponse = components["schemas"]["EventsResponse"];

export interface ObjectRow {
  object_id: string;
  name: string;
  size: number;
  content_type: string;
  checksum: string;
  chunk_count: number;
  created_at: number;
  modified_at: number;
}

export interface ObjectListResponse {
  objects: ObjectRow[];
  total: number;
  limit: number;
  offset: number;
}

export interface ObjectMetadataResponse extends ObjectRow {
  custom: Record<string, string>;
}

export interface ChunkReplicaDetail {
  worker_id: string;
  state: "OK" | "CORRUPT" | "PENDING_DELETE" | string;
  worker_status: "ALIVE" | "DEAD" | string;
}

export interface ChunkDetail {
  index: number;
  chunk_id: string;
  size: number;
  checksum: string;
  replicas: ChunkReplicaDetail[];
}

export interface ObjectChunksResponse {
  object_id: string;
  name: string;
  status: string;
  chunks: ChunkDetail[];
}

export interface ScrubRunResponse {
  workers_scrubbed: number;
  chunks_checked: number;
  corrupt_found: number;
  repairs_triggered: boolean;
}

export class ApiRequestError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

export const API_BASE = (import.meta.env.VITE_API_BASE || "").replace(/\/+$/, "");

export function apiUrl(path: string): string {
  return `${API_BASE}${path}`;
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(apiUrl(path), init);
  const text = await resp.text();
  let parsed: Record<string, unknown> | null = null;
  if (text) {
    try {
      parsed = JSON.parse(text) as Record<string, unknown>;
    } catch {
      parsed = null;
    }
  }
  if (!resp.ok) {
    const code = typeof parsed?.error === "string" ? parsed.error : `HTTP_${resp.status}`;
    const msg = typeof parsed?.message === "string" ? parsed.message : resp.statusText || "Request failed";
    throw new ApiRequestError(resp.status, code, msg);
  }
  return (parsed ?? {}) as T;
}

export const api = {
  getOverview: () => requestJson<OverviewResponse>("/api/v1/admin/overview"),
  getEvents: (since = 0, limit = 100) =>
    requestJson<EventsResponse>(`/api/v1/admin/events?since=${since}&limit=${limit}`),
  triggerScrub: () => requestJson<ScrubRunResponse>("/api/v1/admin/scrub", { method: "POST" }),
  listObjects: (limit = 25, offset = 0) =>
    requestJson<ObjectListResponse>(`/api/v1/objects?limit=${limit}&offset=${offset}`),
  getObjectMetadata: (id: string) =>
    requestJson<ObjectMetadataResponse>(`/api/v1/objects/${encodeURIComponent(id)}/metadata`),
  getObjectChunks: (id: string) =>
    requestJson<ObjectChunksResponse>(`/api/v1/admin/objects/${encodeURIComponent(id)}/chunks`),
  patchObjectMetadata: (id: string, patch: Record<string, string | null>) =>
    requestJson<ObjectMetadataResponse>(`/api/v1/objects/${encodeURIComponent(id)}/metadata`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    }),
  deleteObject: async (id: string): Promise<void> => {
    const resp = await fetch(apiUrl(`/api/v1/objects/${encodeURIComponent(id)}`), { method: "DELETE" });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new ApiRequestError(
        resp.status,
        typeof body?.error === "string" ? body.error : `HTTP_${resp.status}`,
        typeof body?.message === "string" ? body.message : resp.statusText
      );
    }
  },
  downloadUrl: (id: string) => apiUrl(`/api/v1/objects/${encodeURIComponent(id)}`),
};

export interface UploadTaskHandle {
  promise: Promise<ObjectRow & { metadata: Record<string, string> }>;
  abort: () => void;
}

export function uploadFileWithProgress(
  file: File,
  metadata: Record<string, string>,
  onProgress: (loaded: number, total: number) => void
): UploadTaskHandle {
  const xhr = new XMLHttpRequest();
  const promise = new Promise<ObjectRow & { metadata: Record<string, string> }>((resolve, reject) => {
    xhr.open("POST", apiUrl("/api/v1/objects"), true);

    xhr.upload.onprogress = (ev) => {
      if (ev.lengthComputable) {
        onProgress(ev.loaded, ev.total);
      } else {
        onProgress(ev.loaded, file.size);
      }
    };

    xhr.onload = () => {
      let body: Record<string, unknown> = {};
      try {
        body = xhr.responseText ? (JSON.parse(xhr.responseText) as Record<string, unknown>) : {};
      } catch {
        body = {};
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        onProgress(file.size, file.size);
        resolve(body as unknown as ObjectRow & { metadata: Record<string, string> });
      } else {
        reject(
          new ApiRequestError(
            xhr.status,
            typeof body.error === "string" ? body.error : `HTTP_${xhr.status}`,
            typeof body.message === "string" ? body.message : xhr.statusText || "Upload failed"
          )
        );
      }
    };

    xhr.onerror = () => reject(new ApiRequestError(0, "NETWORK_ERROR", "Controller unreachable during upload"));
    xhr.onabort = () => reject(new ApiRequestError(0, "ABORTED", "Upload cancelled by user"));

    const form = new FormData();
    form.append("file", file, file.name);
    if (Object.keys(metadata).length > 0) {
      form.append("metadata", JSON.stringify(metadata));
    }
    xhr.send(form);
  });

  return { promise, abort: () => xhr.abort() };
}
