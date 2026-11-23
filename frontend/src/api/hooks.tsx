import React, { createContext, useCallback, useContext, useEffect, useState } from "react";
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { api, ApiRequestError, type ObjectChunksResponse, type ObjectListResponse, type ObjectMetadataResponse, type OverviewResponse, type EventsResponse } from "./client";

export const POLL_INTERVAL_MS = 1500;

interface ClusterSyncState {
  consecutiveFailures: number;
  isStaleOrUnreachable: boolean;
  adminDisabled: boolean;
  lastError: string | null;
  lastSyncMs: number | null;
  serverTimeOffsetSec: number;
  reportSuccess: (serverTime?: number) => void;
  reportFailure: (err: unknown) => void;
}

const ClusterSyncContext = createContext<ClusterSyncState>({
  consecutiveFailures: 0,
  isStaleOrUnreachable: false,
  adminDisabled: false,
  lastError: null,
  lastSyncMs: null,
  serverTimeOffsetSec: 0,
  reportSuccess: () => {},
  reportFailure: () => {},
});

export function ClusterSyncProvider({ children }: { children: React.ReactNode }) {
  const [consecutiveFailures, setConsecutiveFailures] = useState(0);
  const [adminDisabled, setAdminDisabled] = useState(false);
  const [lastError, setLastError] = useState<string | null>(null);
  const [lastSyncMs, setLastSyncMs] = useState<number | null>(null);
  const [serverTimeOffsetSec, setServerTimeOffsetSec] = useState(0);

  const reportSuccess = useCallback((serverTime?: number) => {
    setConsecutiveFailures(0);
    setAdminDisabled(false);
    setLastError(null);
    const nowMs = Date.now();
    setLastSyncMs(nowMs);
    if (typeof serverTime === "number") {
      setServerTimeOffsetSec(serverTime - nowMs / 1000);
    }
  }, []);

  const reportFailure = useCallback((err: unknown) => {
    setConsecutiveFailures((prev) => prev + 1);
    if (err instanceof ApiRequestError && err.status === 403 && err.code === "ADMIN_DISABLED") {
      setAdminDisabled(true);
    }
    const msg =
      err instanceof ApiRequestError
        ? `${err.code}: ${err.message}`
        : err instanceof Error
          ? err.message
          : "Controller unreachable";
    setLastError(msg);
  }, []);

  return (
    <ClusterSyncContext.Provider
      value={{
        consecutiveFailures,
        isStaleOrUnreachable: consecutiveFailures >= 2 && !adminDisabled,
        adminDisabled,
        lastError,
        lastSyncMs,
        serverTimeOffsetSec,
        reportSuccess,
        reportFailure,
      }}
    >
      {children}
    </ClusterSyncContext.Provider>
  );
}

export function useClusterSync() {
  return useContext(ClusterSyncContext);
}

/** Smooth 1-second display clock anchored to controller server_time (triggers zero network calls). */
export function useServerClock(intervalMs = 1000): number {
  const { serverTimeOffsetSec } = useClusterSync();
  const [nowSec, setNowSec] = useState(() => Date.now() / 1000 + serverTimeOffsetSec);

  useEffect(() => {
    setNowSec(Date.now() / 1000 + serverTimeOffsetSec);
    const id = window.setInterval(() => {
      setNowSec(Date.now() / 1000 + serverTimeOffsetSec);
    }, intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs, serverTimeOffsetSec]);

  return nowSec;
}

export function useOverview(): UseQueryResult<OverviewResponse> {
  const { reportSuccess, reportFailure } = useClusterSync();
  return useQuery({
    queryKey: ["admin", "overview"],
    queryFn: async () => {
      try {
        const res = await api.getOverview();
        reportSuccess(res.server_time);
        return res;
      } catch (err) {
        reportFailure(err);
        throw err;
      }
    },
    refetchInterval: POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    retry: false,
    placeholderData: (prev) => prev,
  });
}

export function useLatestEvents(limit = 5): UseQueryResult<EventsResponse> {
  const { reportFailure } = useClusterSync();
  return useQuery({
    queryKey: ["admin", "events", "latest", limit],
    queryFn: async () => {
      try {
        return await api.getEvents(0, 200);
      } catch (err) {
        reportFailure(err);
        throw err;
      }
    },
    refetchInterval: POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    retry: false,
    placeholderData: (prev) => prev,
  });
}

export function useObjectsList(limit: number, offset: number): UseQueryResult<ObjectListResponse> {
  const { reportSuccess, reportFailure } = useClusterSync();
  return useQuery({
    queryKey: ["objects", "list", limit, offset],
    queryFn: async () => {
      try {
        const res = await api.listObjects(limit, offset);
        reportSuccess();
        return res;
      } catch (err) {
        reportFailure(err);
        throw err;
      }
    },
    refetchInterval: POLL_INTERVAL_MS * 2,
    refetchIntervalInBackground: false,
    retry: false,
    placeholderData: (prev) => prev,
  });
}

export function useObjectMetadata(objectId: string): UseQueryResult<ObjectMetadataResponse> {
  return useQuery({
    queryKey: ["objects", "metadata", objectId],
    queryFn: () => api.getObjectMetadata(objectId),
    enabled: Boolean(objectId),
    refetchInterval: POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    retry: false,
    placeholderData: (prev) => prev,
  });
}

export function useObjectChunks(objectId: string): UseQueryResult<ObjectChunksResponse> {
  const { reportSuccess, reportFailure } = useClusterSync();
  return useQuery({
    queryKey: ["admin", "objects", objectId, "chunks"],
    queryFn: async () => {
      try {
        const res = await api.getObjectChunks(objectId);
        reportSuccess();
        return res;
      } catch (err) {
        reportFailure(err);
        throw err;
      }
    },
    enabled: Boolean(objectId),
    refetchInterval: POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    retry: false,
    placeholderData: (prev) => prev,
  });
}
