# Security Review

Review this change for security risks before merge.

Focus on:

- leaked secrets or credentials
- unsafe shell execution
- untrusted input reaching SQL, filesystem, network, or subprocess boundaries
- changes to authentication, authorization, or permissions
- dependency, workflow, or release changes that weaken supply-chain controls

Return findings by severity with file references. If no issues are found, say so and note any residual risk.
