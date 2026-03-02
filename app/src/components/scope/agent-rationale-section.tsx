interface AgentRationaleSectionProps {
  analysisMetadataJson: string | null;
}

export function AgentRationaleSection({ analysisMetadataJson }: AgentRationaleSectionProps) {
  if (!analysisMetadataJson) return null;

  try {
    const metadata = JSON.parse(analysisMetadataJson);
    const hasMetadata = metadata && typeof metadata === 'object' && Object.keys(metadata).length > 0;

    if (!hasMetadata) return null;

    return (
      <div className="space-y-3">
        <p className="text-sm font-medium">Agent Analysis Rationale</p>
        {Object.entries(metadata).map(([field, data]: [string, any]) => {
          if (!data || typeof data !== 'object') return null;
          const { value, confidence, reasoning } = data;
          return (
            <div key={field} className="rounded-md border border-border bg-background p-3 text-xs">
              <div className="mb-1 flex items-center justify-between">
                <span className="font-medium text-foreground">
                  {field.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase())}
                </span>
                {typeof confidence === 'number' && (
                  <span
                    className="rounded-full px-2 py-0.5 text-[10px] font-medium"
                    style={{
                      backgroundColor:
                        confidence >= 80
                          ? 'color-mix(in oklch, var(--color-seafoam), transparent 85%)'
                          : confidence >= 60
                            ? 'color-mix(in oklch, var(--color-pacific), transparent 85%)'
                            : 'bg-muted',
                      color:
                        confidence >= 80
                          ? 'var(--color-seafoam)'
                          : confidence >= 60
                            ? 'var(--color-pacific)'
                            : 'text-muted-foreground',
                    }}
                  >
                    {confidence}% confidence
                  </span>
                )}
              </div>
              {value !== undefined && (
                <p className="mb-1 text-muted-foreground">
                  <span className="font-medium">Value:</span> {String(value)}
                </p>
              )}
              {reasoning && (
                <p className="text-muted-foreground">
                  <span className="font-medium">Reasoning:</span> {reasoning}
                </p>
              )}
            </div>
          );
        })}
      </div>
    );
  } catch (e) {
    return null;
  }
}
