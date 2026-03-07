# T-SQL ResolveCallGraph

For each candidate procedure:

1. Call `get_procedure_body(name: <proc>, ddl_path: <ddl_path>)` to fetch the body (if not already fetched).
2. Identify procedure calls using the call syntax in [call-graph.md](call-graph.md).
3. For each called procedure, call `get_procedure_body(name: <callee>, ddl_path: <ddl_path>)` to fetch its body.
4. Repeat recursively up to `search_depth` hops from the original candidate.

Track the call path for every reached procedure using the format in [call-graph.md](call-graph.md).
