import { useState, useRef, useEffect } from 'react';
import { X } from 'lucide-react';
import type { ColumnMetadata } from '@/lib/types';

interface MultiSelectColumnsProps {
  selectedColumns: string[];
  availableColumns: ColumnMetadata[];
  disabled: boolean;
  placeholder?: string;
  onUpdate: (columns: string[]) => void;
}

export function MultiSelectColumns({
  selectedColumns,
  availableColumns,
  disabled,
  placeholder = 'Type to search columns...',
  onUpdate,
}: MultiSelectColumnsProps) {
  const [searchTerm, setSearchTerm] = useState('');
  const [isOpen, setIsOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Filter available columns that aren't already selected
  const unselectedColumns = availableColumns.filter(
    (col) => !selectedColumns.includes(col.columnName)
  );

  // Filter by search term
  const filteredColumns = unselectedColumns.filter((col) =>
    col.columnName.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  function handleRemove(columnName: string) {
    if (disabled) return;
    onUpdate(selectedColumns.filter((c) => c !== columnName));
  }

  function handleAdd(columnName: string) {
    if (disabled) return;
    onUpdate([...selectedColumns, columnName]);
    setSearchTerm('');
    inputRef.current?.focus();
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'Backspace' && searchTerm === '' && selectedColumns.length > 0) {
      // Remove last selected column on backspace when input is empty
      onUpdate(selectedColumns.slice(0, -1));
    } else if (e.key === 'Enter' && filteredColumns.length > 0) {
      // Add first filtered column on enter
      e.preventDefault();
      handleAdd(filteredColumns[0].columnName);
    } else if (e.key === 'Escape') {
      setIsOpen(false);
    }
  }

  return (
    <div ref={containerRef} className="relative">
      <div
        className={`min-h-10 w-full rounded-md border bg-background px-3 py-2 text-sm ${
          disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-text'
        }`}
        onClick={() => !disabled && inputRef.current?.focus()}
      >
        <div className="flex flex-wrap gap-1.5">
          {selectedColumns.map((columnName) => {
            const col = availableColumns.find((c) => c.columnName === columnName);
            return (
              <span
                key={columnName}
                className="inline-flex items-center gap-1 rounded-md bg-primary/10 px-2 py-0.5 text-xs font-medium"
              >
                <span className="font-mono">
                  {columnName}
                  {col && <span className="ml-1 text-muted-foreground">({col.dataType})</span>}
                </span>
                {!disabled && (
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleRemove(columnName);
                    }}
                    className="hover:bg-primary/20 rounded-sm p-0.5"
                  >
                    <X className="h-3 w-3" />
                  </button>
                )}
              </span>
            );
          })}
          <input
            ref={inputRef}
            type="text"
            value={searchTerm}
            disabled={disabled}
            placeholder={selectedColumns.length === 0 ? placeholder : ''}
            className="flex-1 min-w-[120px] bg-transparent outline-none placeholder:text-muted-foreground"
            onChange={(e) => setSearchTerm(e.target.value)}
            onFocus={() => setIsOpen(true)}
            onKeyDown={handleKeyDown}
          />
        </div>
      </div>

      {/* Dropdown */}
      {isOpen && !disabled && filteredColumns.length > 0 && (
        <div className="absolute z-50 mt-1 max-h-60 w-full overflow-auto rounded-md border bg-popover p-1 shadow-md">
          {filteredColumns.map((col) => (
            <button
              key={col.columnName}
              type="button"
              className="w-full rounded-sm px-2 py-1.5 text-left text-sm hover:bg-accent hover:text-accent-foreground"
              onClick={() => handleAdd(col.columnName)}
            >
              <span className="font-mono">
                {col.columnName} <span className="text-muted-foreground">({col.dataType})</span>
              </span>
            </button>
          ))}
        </div>
      )}

      {isOpen && !disabled && searchTerm && filteredColumns.length === 0 && (
        <div className="absolute z-50 mt-1 w-full rounded-md border bg-popover p-2 shadow-md">
          <p className="text-sm text-muted-foreground">No columns found</p>
        </div>
      )}
    </div>
  );
}
