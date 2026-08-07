"use client";

import { createChart, ColorType, LineSeries, type IChartApi, type UTCTimestamp } from "lightweight-charts";
import { useEffect, useRef } from "react";

export type ChartPoint = { time: UTCTimestamp; value: number };

/**
 * The price history for one card in one market.
 *
 * The series is stepped, not smoothed. `market_observations` records changes
 * only, so between two rows the price genuinely held that value: a straight
 * interpolation would draw a drift that never happened.
 */
export function PriceChart({ points }: { points: ChartPoint[] }) {
  const container = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!container.current || points.length === 0) return;

    const chart: IChartApi = createChart(container.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#a3a3a3",
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: "#262626" },
        horzLines: { color: "#262626" },
      },
      rightPriceScale: { borderColor: "#404040" },
      timeScale: { borderColor: "#404040", timeVisible: true },
      height: 320,
      autoSize: true,
    });

    const series = chart.addSeries(LineSeries, {
      color: "#34d399",
      lineWidth: 2,
      lineType: 1, // Stepped. See the note above.
      priceFormat: { type: "volume" },
    });
    series.setData(points);
    chart.timeScale().fitContent();

    return () => chart.remove();
  }, [points]);

  if (points.length === 0) {
    return (
      <div className="flex h-[320px] items-center justify-center rounded-lg border border-dashed border-neutral-800 text-sm text-neutral-500">
        No price has been recorded in this window.
      </div>
    );
  }

  return <div ref={container} className="h-[320px] w-full" />;
}
