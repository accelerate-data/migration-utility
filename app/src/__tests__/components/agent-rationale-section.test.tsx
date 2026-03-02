import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { AgentRationaleSection } from '@/components/scope/agent-rationale-section';

const sampleMetadata = JSON.stringify({
  table_type: { value: 'fact', confidence: 90, reasoning: 'Has many numeric columns' },
  load_strategy: { value: 'incremental', confidence: 75, reasoning: 'Has updated_at column' },
  date_column: { value: 'created_at', confidence: 60, reasoning: 'Most likely date column' },
});

describe('AgentRationaleSection', () => {
  it('renders nothing when analysisMetadataJson is null', () => {
    const { container } = render(<AgentRationaleSection analysisMetadataJson={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing when analysisMetadataJson is empty object', () => {
    const { container } = render(<AgentRationaleSection analysisMetadataJson="{}" />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing when analysisMetadataJson is invalid JSON', () => {
    const { container } = render(<AgentRationaleSection analysisMetadataJson="not-json" />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders section heading when metadata is present', () => {
    render(<AgentRationaleSection analysisMetadataJson={sampleMetadata} />);
    expect(screen.getByText('Agent Analysis Rationale')).toBeInTheDocument();
  });

  it('renders accordion items for each field', () => {
    render(<AgentRationaleSection analysisMetadataJson={sampleMetadata} />);
    expect(screen.getByText('Table Type')).toBeInTheDocument();
    expect(screen.getByText('Load Strategy')).toBeInTheDocument();
    expect(screen.getByText('Date Column')).toBeInTheDocument();
  });

  it('renders confidence scores', () => {
    render(<AgentRationaleSection analysisMetadataJson={sampleMetadata} />);
    expect(screen.getByText('90% confidence')).toBeInTheDocument();
    expect(screen.getByText('75% confidence')).toBeInTheDocument();
    expect(screen.getByText('60% confidence')).toBeInTheDocument();
  });

  describe('hide logic for manually-overridden fields', () => {
    it('hides rationale for overridden fields', () => {
      render(
        <AgentRationaleSection
          analysisMetadataJson={sampleMetadata}
          manualOverrides={['table_type']}
        />,
      );
      expect(screen.queryByText('Table Type')).not.toBeInTheDocument();
      expect(screen.getByText('Load Strategy')).toBeInTheDocument();
      expect(screen.getByText('Date Column')).toBeInTheDocument();
    });

    it('hides multiple overridden fields', () => {
      render(
        <AgentRationaleSection
          analysisMetadataJson={sampleMetadata}
          manualOverrides={['table_type', 'load_strategy']}
        />,
      );
      expect(screen.queryByText('Table Type')).not.toBeInTheDocument();
      expect(screen.queryByText('Load Strategy')).not.toBeInTheDocument();
      expect(screen.getByText('Date Column')).toBeInTheDocument();
    });

    it('shows all fields when manualOverrides is empty', () => {
      render(
        <AgentRationaleSection
          analysisMetadataJson={sampleMetadata}
          manualOverrides={[]}
        />,
      );
      expect(screen.getByText('Table Type')).toBeInTheDocument();
      expect(screen.getByText('Load Strategy')).toBeInTheDocument();
      expect(screen.getByText('Date Column')).toBeInTheDocument();
    });

    it('shows all fields when manualOverrides is not provided', () => {
      render(<AgentRationaleSection analysisMetadataJson={sampleMetadata} />);
      expect(screen.getByText('Table Type')).toBeInTheDocument();
      expect(screen.getByText('Load Strategy')).toBeInTheDocument();
    });

    it('hides all fields when all are overridden', () => {
      render(
        <AgentRationaleSection
          analysisMetadataJson={sampleMetadata}
          manualOverrides={['table_type', 'load_strategy', 'date_column']}
        />,
      );
      expect(screen.queryByText('Table Type')).not.toBeInTheDocument();
      expect(screen.queryByText('Load Strategy')).not.toBeInTheDocument();
      expect(screen.queryByText('Date Column')).not.toBeInTheDocument();
    });
  });
});
