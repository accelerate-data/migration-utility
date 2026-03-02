import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';

interface FieldMetadata {
  value?: unknown;
  confidence?: number;
  reasoning?: string;
}

interface AgentRationaleSectionProps {
  analysisMetadataJson: string | null;
  manualOverrides?: string[];
}

function confidenceStyle(confidence: number): React.CSSProperties {
  if (confidence >= 80) {
    return {
      backgroundColor: 'color-mix(in oklch, var(--color-seafoam), transparent 85%)',
      color: 'var(--color-seafoam)',
    };
  }
  if (confidence >= 60) {
    return {
      backgroundColor: 'color-mix(in oklch, var(--color-pacific), transparent 85%)',
      color: 'var(--color-pacific)',
    };
  }
  return {};
}

export function AgentRationaleSection({ analysisMetadataJson, manualOverrides = [] }: AgentRationaleSectionProps) {
  if (!analysisMetadataJson) return null;

  let metadata: Record<string, FieldMetadata>;
  try {
    const parsed: unknown = JSON.parse(analysisMetadataJson);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    metadata = parsed as Record<string, FieldMetadata>;
  } catch {
    return null;
  }

  const visibleEntries = Object.entries(metadata).filter(([field, data]) => {
    if (!data || typeof data !== 'object') return false;
    if (manualOverrides.includes(field)) return false;
    return true;
  });

  if (visibleEntries.length === 0) return null;

  return (
    <div className="space-y-3">
      <p className="text-sm font-medium">Agent Analysis Rationale</p>
      <Accordion type="multiple" className="w-full">
        {visibleEntries.map(([field, data]) => {
          const { value, confidence, reasoning } = data;

          return (
            <AccordionItem key={field} value={field}>
              <AccordionTrigger className="hover:no-underline">
                <div className="flex w-full items-center justify-between pr-2">
                  <span className="text-sm font-medium text-foreground">
                    {field.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase())}
                  </span>
                  {typeof confidence === 'number' && (
                    <span
                      className="rounded-full px-2 py-0.5 text-[10px] font-medium"
                      style={confidenceStyle(confidence)}
                    >
                      {confidence}% confidence
                    </span>
                  )}
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <div className="space-y-2 pl-1 text-xs">
                  {value !== undefined && (
                    <p className="text-muted-foreground">
                      <span className="font-medium">Value:</span> {String(value)}
                    </p>
                  )}
                  {reasoning && (
                    <p className="text-muted-foreground">
                      <span className="font-medium">Reasoning:</span> {reasoning}
                    </p>
                  )}
                </div>
              </AccordionContent>
            </AccordionItem>
          );
        })}
      </Accordion>
    </div>
  );
}
