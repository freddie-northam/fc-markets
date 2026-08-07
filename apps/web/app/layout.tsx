import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "FC Market",
  description: "A canonical ledger of EA Sports FC Ultimate Team market prices",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="border-b border-neutral-800">
          <div className="mx-auto flex max-w-6xl items-baseline gap-3 px-6 py-4">
            <Link href="/" className="text-sm font-semibold tracking-tight">
              FC Market
            </Link>
            <span className="text-xs text-neutral-500">Ultimate Team price ledger</span>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-6 py-8">{children}</main>
      </body>
    </html>
  );
}
