# T-SQL Confidence Scoring

Assign confidence in [0.0, 1.0] using these deterministic rules:

| Signal | Effect |
|---|---|
| Direct write evidence (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`) | base 0.90 |
| Indirect write (callee is a confirmed direct writer) | base 0.75 |
| Shorter call path (per hop shorter than the deepest path in the candidate set) | +0.02 |
| Multiple independent paths all show write evidence | +0.05 |
| Dynamic SQL present alongside static write evidence (`EXEC(@sql)`, `sp_executesql`) | −0.20 |
| Only dynamic SQL evidence — no static write statement found | cap at 0.45 |

Clamp final score to [0.0, 1.0].
