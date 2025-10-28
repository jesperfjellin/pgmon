import { ReactNode, useState, useMemo } from 'react';

export type SortDirection = 'asc' | 'desc';

export interface ColumnDef<T> {
  key: string;
  label: string;
  width?: string; // e.g., "w-32", "w-48", "flex-1"
  sortable?: boolean;
  align?: 'left' | 'right' | 'center';
  render?: (row: T) => ReactNode;
  sortValue?: (row: T) => string | number;
  className?: string;
}

export interface DataTableProps<T> {
  data: T[];
  columns: ColumnDef<T>[];
  defaultSortKey?: string;
  defaultSortDir?: SortDirection;
  maxRows?: number;
  onRowClick?: (row: T) => void;
  rowKey: (row: T) => string | number;
}

export function DataTable<T>({
  data,
  columns,
  defaultSortKey,
  defaultSortDir = 'desc',
  maxRows,
  onRowClick,
  rowKey,
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | undefined>(defaultSortKey);
  const [sortDir, setSortDir] = useState<SortDirection>(defaultSortDir);

  const sortedData = useMemo(() => {
    if (!sortKey) {
      return maxRows ? data.slice(0, maxRows) : data;
    }

    const column = columns.find((col) => col.key === sortKey);
    if (!column) {
      return maxRows ? data.slice(0, maxRows) : data;
    }

    const sorted = [...data].sort((a, b) => {
      let aVal: string | number;
      let bVal: string | number;

      if (column.sortValue) {
        aVal = column.sortValue(a);
        bVal = column.sortValue(b);
      } else {
        // Fallback to direct property access
        aVal = (a as any)[sortKey];
        bVal = (b as any)[sortKey];
      }

      // Handle null/undefined
      if (aVal == null) return 1;
      if (bVal == null) return -1;

      // String comparison
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc'
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }

      // Number comparison
      const aNum = aVal as number;
      const bNum = bVal as number;
      return sortDir === 'asc' ? aNum - bNum : bNum - aNum;
    });

    return maxRows ? sorted.slice(0, maxRows) : sorted;
  }, [data, columns, sortKey, sortDir, maxRows]);

  const handleSort = (key: string) => {
    const column = columns.find((col) => col.key === key);
    if (!column?.sortable) return;

    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  };

  const getAlignClass = (align?: 'left' | 'right' | 'center') => {
    if (align === 'right') return 'text-right';
    if (align === 'center') return 'text-center';
    return 'text-left';
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm table-fixed">
        <thead>
          <tr className="text-left text-slate-500 border-b border-slate-100">
            {columns.map((column) => (
              <th
                key={column.key}
                className={`py-2 pr-4 ${column.width || 'w-auto'} ${
                  column.sortable
                    ? 'cursor-pointer select-none hover:bg-slate-50'
                    : ''
                } ${getAlignClass(column.align)}`}
                onClick={() => column.sortable && handleSort(column.key)}
              >
                <div className="flex items-center gap-1">
                  <span className="truncate">{column.label}</span>
                  {column.sortable && sortKey === column.key && (
                    <span className="text-slate-400 flex-shrink-0">
                      {sortDir === 'asc' ? '↑' : '↓'}
                    </span>
                  )}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedData.map((row, index) => (
            <tr
              key={rowKey(row)}
              className={`border-b border-slate-50 hover:bg-slate-50/60 ${
                index % 2 === 0 ? 'bg-[#fff1f2]' : 'bg-white'
              } ${onRowClick ? 'cursor-pointer' : ''}`}
              onClick={() => onRowClick?.(row)}
            >
              {columns.map((column) => (
                <td
                  key={column.key}
                  className={`py-2 pr-4 ${getAlignClass(column.align)} ${
                    column.className || ''
                  }`}
                >
                  {column.render
                    ? column.render(row)
                    : String((row as any)[column.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
