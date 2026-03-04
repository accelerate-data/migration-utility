# Branding

## Canonical Assets

- Bundle/app icon source (master): `app/src-tauri/icons/icon_opt1.png` (required `2048x2048`)
- Bundle outputs (generated): `app/src-tauri/icons/`
- In-app brand assets: `app/public/branding/`
  - `icon-dark-256.png`
  - `icon-light-256.png`
  - `logo-dark.svg`
  - `logo-light.svg`
- Canonical source of truth: `~/src/vd-gtm/branding/logo/product/ui/`

## Command-Tab And Dock Icon Rule

- macOS app switcher and dock icon are build-time assets from `icon.icns`.
- Runtime theme switching does not apply to `icon.icns`.
- `icon.icns` must be generated from `icon_opt1.png` after resizing/normalizing the master to `2048x2048`.

## Icon Generation Workflow

Run from `app/`:

```bash
npm run icons:opt1
```

This command:

1. Verifies/normalizes `app/src-tauri/icons/icon_opt1.png` to `2048x2048`
2. Regenerates all Tauri platform icons in `app/src-tauri/icons/`
3. Rebuilds `icon.icns` for macOS app switcher/dock usage

Optional themed generation:

```bash
npm run icons:dark
npm run icons:light
```

## In-App Theme Asset Rule

- Use dark asset variant on light surfaces.
- Use light asset variant on dark surfaces.
- Do not mix ad-hoc icon/logo files outside `app/public/branding/`.

## Accessibility And Frontend Design QA

Validation baseline executed on February 28, 2026:

1. Focus indicators:
   - Keyboard focus uses visible `focus-visible` ring styles on navigation and interactive controls.
2. Icon-only controls:
   - Icon-only nav items expose accessible names via `aria-label`.
3. Contrast:
   - Primary text and muted text use theme tokens with AA-level contrast targets in light and dark themes.
4. Visual consistency:
   - Navigation brand mark, hierarchy, spacing, and type scale are consistent across core surfaces.
