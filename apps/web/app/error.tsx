"use client";

/**
 * Shown when a route throws. The usual cause is the Rust API being unreachable,
 * and the default Next page says only "a server-side exception has occurred",
 * which tells an operator nothing about which half is down.
 */
export default function Error({ reset }: { error: Error; reset: () => void }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-800 p-8 text-center">
      <p className="text-sm text-neutral-300">The market API did not answer.</p>
      <p className="mt-2 text-xs text-neutral-500">
        Check that the server is running and that <code>NEXT_PUBLIC_API_URL</code> points
        at it.
      </p>
      <button
        onClick={reset}
        className="mt-4 rounded border border-neutral-700 px-3 py-1.5 text-xs text-neutral-300 hover:bg-neutral-800"
      >
        Try again
      </button>
    </div>
  );
}
