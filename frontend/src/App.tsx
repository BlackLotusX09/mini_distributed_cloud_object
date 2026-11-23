import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ClusterSyncProvider } from "./api/hooks";
import { Layout } from "./components/Layout";
import { OverviewPage } from "./pages/OverviewPage";
import { ObjectsPage } from "./pages/ObjectsPage";
import { ObjectDetailPage } from "./pages/ObjectDetailPage";
import { EventsPage } from "./pages/EventsPage";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: true,
      staleTime: 0,
    },
  },
});

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ClusterSyncProvider>
        <BrowserRouter basename="/ui">
          <Routes>
            <Route element={<Layout />}>
              <Route path="/" element={<OverviewPage />} />
              <Route path="/objects" element={<ObjectsPage />} />
              <Route path="/objects/:id" element={<ObjectDetailPage />} />
              <Route path="/events" element={<EventsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </ClusterSyncProvider>
    </QueryClientProvider>
  );
}

export default App;
