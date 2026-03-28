# Resolution Rules

| Condition | status | selected_writer |
|---|---|---|
| `technology` absent or unsupported | `error` | absent |
| Cross-database reference on any candidate | `error` | absent |
| Exactly one candidate with confidence > 0.7 | `resolved` | that candidate |
| Two or more candidates with confidence > 0.7 | `ambiguous_multi_writer` | absent |
| Candidates exist but none exceed 0.7 | `partial` | absent |
| No candidates found | `no_writer_found` | absent |
| Analysis or runtime failure | `error` | absent |
