import type { NextConfig } from "next";

const config: NextConfig = {
  reactStrictMode: true,
  // The market table and every chart read live ledger data, so nothing here is
  // safe to pre-render at build time.
  experimental: {},
};

export default config;
