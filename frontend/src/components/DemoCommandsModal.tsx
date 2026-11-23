import { useState } from "react";
import { Check, Copy, Terminal, X } from "lucide-react";

interface Snippet {
  title: string;
  description: string;
  cmd: string;
}

const DEMO_SNIPPETS: Snippet[] = [
  {
    title: "Stop worker2 (Simulate Node Crash)",
    description: "Controller detects missed heartbeats after 15s (DEAD) and begins R=2 repair after the 30s grace period.",
    cmd: "docker stop worker2",
  },
  {
    title: "Start worker2 (Trigger Rejoin Reconciliation)",
    description: "worker2 re-registers, reconciles disk inventory, clears pending deletes, and trims over-replicated chunks.",
    cmd: "docker start worker2",
  },
  {
    title: "Inject Bit-Rot Corruption",
    description: "Flip a byte inside a stored chunk on disk (replace <worker> and <chunk_id> from the Chunk Map).",
    cmd: "bash scripts/corrupt_chunk.sh worker1 <chunk_id>",
  },
  {
    title: "Check Container Health & Cluster Invariants",
    description: "Verify HTTP health across ports 8000–8004 and validate R=2 disk-to-DB invariants.",
    cmd: "bash scripts/check_health.sh && .venv/bin/python scripts/check_invariants.py",
  },
];

export function DemoCommandsModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [copiedIdx, setCopiedIdx] = useState<number | null>(null);

  if (!open) return null;

  const copy = async (idx: number, text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedIdx(idx);
      window.setTimeout(() => setCopiedIdx(null), 1500);
    } catch {
      // Fallback ignored in insecure non-localhost origins
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="demo-commands-title"
    >
      <div className="w-full max-w-2xl rounded-xl border border-slate-200 bg-white shadow-xl">
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-4">
          <div className="flex items-center gap-2">
            <Terminal className="h-5 w-5 text-slate-700" />
            <h2 id="demo-commands-title" className="text-base font-semibold text-slate-900">
              CLI Fault-Injection &amp; Demo Snippets
            </h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
            aria-label="Close modal"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="space-y-3 p-5">
          <p className="text-xs text-slate-600">
            For security isolation, the controller has no access to Docker sockets. Run these commands from your host shell while watching the dashboard react every 1.5s:
          </p>
          {DEMO_SNIPPETS.map((item, idx) => (
            <div key={item.title} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-medium text-slate-900">{item.title}</div>
                  <div className="mt-0.5 text-xs text-slate-600">{item.description}</div>
                </div>
                <button
                  type="button"
                  onClick={() => copy(idx, item.cmd)}
                  className="inline-flex shrink-0 items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
                >
                  {copiedIdx === idx ? (
                    <>
                      <Check className="h-3.5 w-3.5 text-emerald-600" />
                      <span>Copied</span>
                    </>
                  ) : (
                    <>
                      <Copy className="h-3.5 w-3.5" />
                      <span>Copy</span>
                    </>
                  )}
                </button>
              </div>
              <pre className="mt-2 overflow-x-auto rounded bg-slate-900 px-3 py-2 text-xs text-slate-100">
                <code>{item.cmd}</code>
              </pre>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
