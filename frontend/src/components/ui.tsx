import React from "react";

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
    <span className={classNames("px-2 py-0.5 text-xs rounded-full font-medium", tones[tone])}>
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
    <div className={classNames("bg-white/70 backdrop-blur border border-slate-200 rounded-2xl shadow-sm", className)}>
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
  tone?: "blue" | "green" | "amber" | "rose" | "violet" | "slate";
  status?: "warn" | "crit";
}

export function MetricCard({ title, value, unit, icon, tone = "blue", status }: MetricCardProps) {
  const toneColor = {
    blue: "text-sky-600",
    green: "text-emerald-600",
    amber: "text-amber-600",
    rose: "text-rose-600",
    violet: "text-violet-600",
    slate: "text-slate-600",
  }[tone];

  return (
    <Card>
      <CardBody>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            {icon && <div className={classNames("p-2 rounded-xl bg-slate-50", toneColor)}>{icon}</div>}
            <div>
              <div className="text-xs text-slate-500">{title}</div>
              <div className="text-lg sm:text-xl font-semibold text-slate-900 flex items-center gap-2">
                {value}
                {unit && <span className="text-slate-400 text-sm">{unit}</span>}
                {status && (
                  <Badge tone={status === "crit" ? "red" : "yellow"}>
                    {status}
                  </Badge>
                )}
              </div>
            </div>
          </div>
        </div>
      </CardBody>
    </Card>
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
