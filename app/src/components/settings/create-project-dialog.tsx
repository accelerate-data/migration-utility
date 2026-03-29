import { useEffect, useState } from 'react';
import { FolderOpen, Loader2, RefreshCw, Search } from 'lucide-react';
import { toast } from 'sonner';
import { open as openDialog } from '@tauri-apps/plugin-dialog';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { projectCreateFull, projectDetectDatabases, tauriErrorMessage } from '@/lib/tauri';
import { TECHNOLOGY_LABEL } from '@/lib/types';
import type { Technology } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectInit } from '@/hooks/use-project-init';
import { useProjectStore } from '@/stores/project-store';
import { toSlugPreview, localTodayString, localDateToUtc } from '@/lib/date-utils';

const TECHNOLOGIES: { value: Technology; label: string }[] = [
  { value: 'sql_server', label: TECHNOLOGY_LABEL['sql_server'] },
  { value: 'fabric_warehouse', label: TECHNOLOGY_LABEL['fabric_warehouse'] },
  { value: 'fabric_lakehouse', label: TECHNOLOGY_LABEL['fabric_lakehouse'] },
  { value: 'snowflake', label: TECHNOLOGY_LABEL['snowflake'] },
];

interface CreateDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: () => void;
}

export default function CreateProjectDialog({ open, onOpenChange, onCreated }: CreateDialogProps) {
  const [name, setName] = useState('');
  const [technology, setTechnology] = useState<Technology>('sql_server');
  const [sourcePath, setSourcePath] = useState('');
  const [dbName, setDbName] = useState('');
  const [detectedDbs, setDetectedDbs] = useState<string[]>([]);
  const [detecting, setDetecting] = useState(false);
  const [detectError, setDetectError] = useState('');
  const [extractionDate, setExtractionDate] = useState(localTodayString);
  const [creating, setCreating] = useState(false);
  const { loadProjects } = useProjectStore();
  const { runInit } = useProjectInit();

  const isSqlServer = technology === 'sql_server';

  function reset() {
    setName('');
    setTechnology('sql_server');
    setSourcePath('');
    setDbName('');
    setDetectedDbs([]);
    setDetecting(false);
    setDetectError('');
    setExtractionDate(localTodayString());
  }

  async function handleDetect() {
    if (!sourcePath) return;
    setDetecting(true);
    setDetectError('');
    setDetectedDbs([]);
    setDbName('');
    try {
      logger.debug('projects-tab: detecting databases', sourcePath);
      const dbs = await projectDetectDatabases(sourcePath);
      setDetectedDbs(dbs);
      if (dbs.length === 1) setDbName(dbs[0]);
    } catch (err) {
      logger.error('projects-tab: detect databases failed', err);
      setDetectError(tauriErrorMessage(err));
    } finally {
      setDetecting(false);
    }
  }

  async function pickSourceFile() {
    const filters = isSqlServer
      ? [{ name: 'DacPac', extensions: ['dacpac'] }]
      : [{ name: 'DDL Archive', extensions: ['zip'] }];
    const selected = await openDialog({ filters, multiple: false });
    if (typeof selected === 'string') {
      setSourcePath(selected);
      setDetectedDbs([]);
      setDbName('');
      setDetectError('');
    }
  }

  async function handleCreate() {
    if (!name.trim() || !sourcePath || !dbName.trim() || !extractionDate) {
      toast.error('All fields are required', { duration: Infinity });
      return;
    }
    setCreating(true);
    try {
      logger.debug('projects-tab: creating project', name, technology);
      const project = await projectCreateFull(
        name.trim(),
        technology,
        sourcePath,
        dbName.trim(),
        localDateToUtc(extractionDate),
      );
      toast.success(`Project "${project.name}" created`);
      await loadProjects();
      onOpenChange(false);
      reset();
      onCreated();

      await runInit(project.id);
      toast.success('Project initialized successfully');
    } catch (err) {
      logger.error('projects-tab: create failed', err);
      toast.error(tauriErrorMessage(err), { duration: Infinity });
    } finally {
      setCreating(false);
    }
  }

  // Auto-detect DB name when a DacPac is selected (SQL Server only).
  useEffect(() => {
    if (sourcePath && isSqlServer) {
      void handleDetect();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourcePath]);

  // Clear source path when technology changes (different file type).
  useEffect(() => {
    setSourcePath('');
    setDetectedDbs([]);
    setDbName('');
    setDetectError('');
  }, [technology]);

  const busy = detecting || creating;
  const canCreate = !!name.trim() && !!sourcePath && !!dbName.trim() && !!extractionDate && !busy;

  return (
    <Dialog open={open} onOpenChange={(v) => { if (!busy) { onOpenChange(v); if (!v) reset(); } }}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>New project</DialogTitle>
        </DialogHeader>

        {detecting && (
          <div className="absolute inset-0 z-10 rounded-lg flex flex-col items-center justify-center gap-3 bg-background/80 backdrop-blur-sm">
            <Loader2 className="h-8 w-8 animate-spin" style={{ color: 'var(--color-pacific)' }} />
            <div className="text-center">
              <p className="text-sm font-semibold text-foreground">Reading DacPac…</p>
              <p className="text-xs text-muted-foreground mt-1">Extracting database name from metadata.</p>
            </div>
          </div>
        )}

        <div className="flex flex-col gap-4 py-2">
          {/* Row 1: Name + Technology */}
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="proj-name" className="text-xs font-medium text-muted-foreground">Project name</Label>
              <Input
                id="proj-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. Contoso Migration"
                disabled={busy}
              />
              {name.trim() && (
                <p className="text-xs text-muted-foreground font-mono">
                  slug: {toSlugPreview(name.trim())}
                </p>
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs font-medium text-muted-foreground">Technology</Label>
              <Select value={technology} onValueChange={(v) => setTechnology(v as Technology)} disabled={busy}>
                <SelectTrigger data-testid="project-technology-select">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TECHNOLOGIES.map((t) => (
                    <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Row 2: Source file */}
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs font-medium text-muted-foreground">
              {isSqlServer ? 'DacPac file' : 'DDL archive (.zip)'}
            </Label>
            <div className="flex items-center gap-2">
              <Input
                value={sourcePath}
                readOnly
                placeholder={isSqlServer ? 'Select a .dacpac file…' : 'Select a .zip archive…'}
                className="cursor-pointer"
                onClick={pickSourceFile}
                disabled={busy}
              />
              <Button variant="outline" size="icon" onClick={pickSourceFile} disabled={busy}>
                <FolderOpen className="h-4 w-4" />
              </Button>
            </div>
          </div>

          {/* Row 3: Database name */}
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs font-medium text-muted-foreground">Database name</Label>
            <div className="flex flex-col gap-1.5">
              {isSqlServer && detectedDbs.length === 0 ? (
                <div className="flex items-center gap-2">
                  <Input
                    value={dbName}
                    onChange={(e) => setDbName(e.target.value)}
                    placeholder="e.g. Contoso_DW"
                    disabled={busy}
                  />
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handleDetect}
                    disabled={busy || !sourcePath}
                    title="Detect from DacPac"
                    data-testid="project-detect-databases"
                  >
                    <Search className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ) : isSqlServer ? (
                <div className="flex items-center gap-1.5">
                  <Select value={dbName} onValueChange={setDbName} disabled={busy}>
                    <SelectTrigger data-testid="project-dbname-select">
                      <SelectValue placeholder="Select database…" />
                    </SelectTrigger>
                    <SelectContent>
                      {detectedDbs.map((db) => (
                        <SelectItem key={db} value={db}>{db}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={handleDetect}
                    disabled={busy}
                    title="Re-detect from DacPac"
                    data-testid="project-redetect-databases"
                  >
                    <RefreshCw className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ) : (
                <Input
                  value={dbName}
                  onChange={(e) => setDbName(e.target.value)}
                  placeholder="e.g. Contoso_DW"
                  disabled={busy}
                />
              )}
              {detectError && (
                <p className="text-xs text-destructive break-all" data-testid="project-detect-error">{detectError}</p>
              )}
            </div>
          </div>

          {/* Row 4: Extraction date */}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="proj-extraction" className="text-xs font-medium text-muted-foreground">Extraction date</Label>
            <Input
              id="proj-extraction"
              type="date"
              value={extractionDate}
              onChange={(e) => setExtractionDate(e.target.value)}
              disabled={busy}
            />
          </div>
        </div>

        <DialogFooter>
          <Button variant="ghost" size="sm" onClick={() => onOpenChange(false)} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" onClick={handleCreate} disabled={!canCreate}>
            {creating ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" /> : null}
            {creating ? 'Creating…' : 'Create'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
