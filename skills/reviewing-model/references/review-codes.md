# Review Codes

Use shared review codes in summary issue arrays and warning/error arrays. Use stable standards codes for detailed standards fixes.

| Finding type | `checks.*.issues[]` code | `feedback_for_model_generator[].code` |
|---|---|---|
| Standards/style/naming/YAML | `REVIEW_STANDARDS_VIOLATION` | Stable standard code: `SQL_*`, `CTE_*`, `MDL_*`, or `YML_*` |
| Transformation correctness | `REVIEW_CORRECTNESS_GAP` | `REVIEW_CORRECTNESS_GAP` |
| Unit-test/spec integration | `REVIEW_TEST_INTEGRATION_GAP` | `REVIEW_TEST_INTEGRATION_GAP` |
| Prerequisite/read/parse failure | n/a | Put the shared error code in `errors[]` |

| Location | Allowed codes |
|---|---|
| `checks.standards.issues[]` | `REVIEW_STANDARDS_VIOLATION` |
| `checks.correctness.issues[]` | `REVIEW_CORRECTNESS_GAP` |
| `checks.test_integration.issues[]` | `REVIEW_TEST_INTEGRATION_GAP` |
| `warnings[]` | shared codes from `../../lib/shared/generate_model_error_codes.md` |
| `errors[]` | shared codes from `../../lib/shared/generate_model_error_codes.md` |
| `feedback_for_model_generator[]` standards items | `SQL_*`, `CTE_*`, `MDL_*`, `YML_*` |
| `feedback_for_model_generator[]` correctness items | `REVIEW_CORRECTNESS_GAP` |
| `feedback_for_model_generator[]` test-integration items | `REVIEW_TEST_INTEGRATION_GAP` |

Rules:

- Do not invent local correctness or test-integration codes.
- Report every directly observable stable standards code that applies.
- Keep summary issue arrays short and actionable.
- Put detailed remediation in `feedback_for_model_generator[]`.
