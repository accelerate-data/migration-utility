# YAML Style

Source: https://docs.getdbt.com/best-practices/how-we-style/5-how-we-style-our-yaml

| code | rule | severity |
|------|------|----------|
| YML_001 | Use 2-space indentation throughout YAML files | warning |
| YML_002 | Every model must have a `description` field | error |
| YML_003 | Primary key columns must have a `description` field | warning |
| YML_004 | `version: 2` must appear at the top of every schema YAML file | error |
| YML_005 | String values should be quoted | info |
| YML_006 | Lines of YAML should be no longer than 80 characters | info |
| YML_007 | Use a blank line to separate list items that are dictionaries for readability | info |
| YML_008 | List items must be indented consistently | warning |
