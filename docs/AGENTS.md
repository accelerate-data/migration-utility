# Documentation

## Index Maintenance

Documentation directories expected to contain multiple child folders must keep a `README.md` index.
`docs/design/`, `docs/functional/`, and `docs/reference/` are indexed documentation directories.

When adding a file or removing a folder under an indexed documentation tree, update the nearest
relevant index in the same change.

`docs/plans/` and `docs/wiki/` do not require folder indexes unless they are expected to contain
multiple child folders or a maintainer explicitly asks for one.
