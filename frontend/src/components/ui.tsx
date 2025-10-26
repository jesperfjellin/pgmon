import React from "react";
import { ResponsiveContainer, AreaChart, Area } from "recharts";

// ---------- Helper Functions ----------
function classNames(...xs: (string | false | null | undefined)[]) {
  return xs.filter(Boolean).join(" ");
}

// ---------- Badge Component ----------
interface BadgeProps {
  children: React.ReactNode;
  tone?: "slate" | "green" | "blue" | "yellow" | "red" | "purple" | "gray";
}

export function Badge({ children, tone = "slate" }: BadgeProps) {
  const tones: Record<string, string> = {
    slate: "bg-slate-100 text-slate-700",
    green: "bg-emerald-100 text-emerald-700",
    blue: "bg-blue-100 text-blue-700",
    yellow: "bg-amber-100 text-amber-800",
    red: "bg-rose-100 text-rose-700",
    purple: "bg-violet-100 text-violet-700",
    gray: "bg-gray-100 text-gray-700",
  };
  return (
    <span className={classNames("px-2 py-0.5 text-xs rounded-full font-medium shadow-sm", tones[tone])}>
      {children}
    </span>
  );
}

// ---------- Card Components ----------
interface CardProps {
  children: React.ReactNode;
  className?: string;
}

export function Card({ children, className = "" }: CardProps) {
  return (
    <div className={classNames("bg-white border border-slate-200 rounded-2xl shadow-sm", className)}>
      {children}
    </div>
  );
}

interface CardHeaderProps {
  title: string;
  icon?: React.ReactNode;
  actions?: React.ReactNode;
}

export function CardHeader({ title, icon, actions }: CardHeaderProps) {
  return (
    <div className="flex items-center justify-between px-4 sm:px-5 pt-4">
      <div className="flex items-center gap-2">
        {icon}
        <h3 className="text-sm sm:text-base font-semibold text-slate-800">{title}</h3>
      </div>
      <div className="flex items-center gap-2">{actions}</div>
    </div>
  );
}

interface CardBodyProps {
  children: React.ReactNode;
  className?: string;
}

export function CardBody({ children, className = "" }: CardBodyProps) {
  return <div className={classNames("px-4 sm:px-5 py-4", className)}>{children}</div>;
}

// ---------- MetricCard Component ----------
interface MetricCardProps {
  title: string;
  value: string | number;
  unit?: string;
  icon?: React.ReactNode;
  tone?: "blue" | "green" | "amber" | "rose" | "violet" | "slate" | "red";
  status?: "warn" | "crit";
  series?: { value: number }[]; // expects pre-shaped small series
  onClick?: () => void;
  isActive?: boolean;
  change?: number; // absolute change from 24h avg
  changePercent?: number; // percentage change from 24h avg
  average24h?: number; // 24h average value
}

export function MetricCard({ title, value, unit, /* icon unused now */ tone = "blue", status, series, onClick, isActive, change, changePercent, average24h }: MetricCardProps) {
  const toneColor = {
    blue: "text-sky-600",
    green: "text-emerald-600",
    amber: "text-amber-600",
    rose: "text-rose-600",
    violet: "text-violet-600",
    slate: "text-slate-600",
    red: "text-rose-600",
  }[tone];
  // SVG gradient id must be a valid XML ID (no spaces). Use a slugified title.
  const gradientId = `mc-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-spark`;

  const cardClass = onClick
    ? "cursor-pointer transition-all hover:shadow-md hover:-translate-y-0.5 active:scale-98"
    : "";

  const borderClass = isActive
    ? `ring-2 ring-${tone === 'blue' ? 'sky' : tone === 'green' ? 'emerald' : tone === 'amber' ? 'amber' : tone === 'rose' ? 'rose' : tone === 'violet' ? 'violet' : tone === 'red' ? 'rose' : 'slate'}-400 shadow-md`
    : "";

  return (
    <div onClick={onClick} className={cardClass}>
      <Card className={borderClass}>
        <CardBody className="py-3">
          <div className="flex flex-col items-center text-center gap-1">
            <div className="text-xs text-slate-500">{title}</div>
            <div className="text-lg sm:text-xl font-semibold text-slate-900 flex items-center gap-2">
              {value}
              {unit && <span className="text-slate-400 text-sm">{unit}</span>}
              {status && <Badge tone={status === "crit" ? "red" : "yellow"}>{status}</Badge>}
            </div>
            {average24h !== undefined && (
              <div className="text-xs text-slate-500">
                Avg {average24h.toFixed(1)} last 24h
              </div>
            )}
            {series && series.length > 0 && (
              <div className="w-full mt-1 h-10">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={series} margin={{ left: 0, right: 0, top: 2, bottom: 0 }}>
                    <defs>
                      <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#f97316" stopOpacity={0.35} />
                        <stop offset="95%" stopColor="#f97316" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <Area type="monotone" dataKey="value" stroke="#f97316" fill={`url(#${gradientId})`} strokeWidth={2} />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

// ---------- Section Component ----------
interface SectionProps {
  title: string;
  subtitle?: string;
  icon?: React.ReactNode;
}

export function Section({ title, subtitle, icon }: SectionProps) {
  return (
    <div className="flex items-end justify-between mb-3">
      <div className="flex items-center gap-2">
        {icon}
        <h2 className="text-lg sm:text-xl font-semibold text-slate-900">{title}</h2>
      </div>
      {subtitle && <p className="text-sm text-slate-500">{subtitle}</p>}
    </div>
  );
}

// ---------- SqlSnippet Component ----------
interface SqlSnippetProps {
  sql: string;
}

export function SqlSnippet({ sql }: SqlSnippetProps) {
  const normalized = sql.trim();
  return (
    <details className="mt-4 border-t border-slate-100 pt-3">
      <summary className="cursor-pointer text-sm text-slate-600 hover:text-slate-900 font-medium">
        View SQL
      </summary>
      <pre className="mt-2 p-3 bg-slate-50 rounded-lg text-xs overflow-x-auto">
        <code className="text-slate-700">{normalized}</code>
      </pre>
    </details>
  );
}

// ---------- Formatting Helpers ----------
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let v = bytes;
  let unitIndex = -1;
  while (v >= 1024 && unitIndex < units.length - 1) {
    v /= 1024;
    unitIndex++;
  }
  return `${v.toFixed(1)} ${units[unitIndex]}`;
}

export function formatPercentMaybe(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${value.toFixed(digits)}%`;
}
