# MIZUNO PHOTO MEMBER — CREATE Shell Integration

Baseline: 2026-09-25 JST

## Purpose

This phase connects the existing CREATE UI foundation to the canonical Member app shell.

It does not add a new Creative API or duplicate the Creative pipeline.

The integration reuses the already-established sequence:

1. Creative catalog
2. CREATE view model
3. exact `template_id + media_ids` composition planning
4. browser execution readiness
5. browser renderer
6. local Blob preview
7. explicit user-gesture local save

## Canonical mount

The controller accepts only the shell destination:

`[data-member-mount="create"]`

It does not mount into HOME, MEMORIES, SHOP, MY, the owner CRM, or an arbitrary selector.

## Interaction boundary

The controller wires the existing CREATE controls:

- template select
- photo select
- preview
- save

There is no automatic preview and no automatic download.

A locked template is rejected even when the controller API is called directly.

Changing template or media selection invalidates and revokes the previous local Blob preview.

Destroying the controller also revokes the current Blob preview.

## Data boundary

The integration receives already-authorized presentation data.

It does not accept Customer ID or Family ID from browser interaction.

It does not expose:

- Customer ID
- Family ID
- storage key
- signed provider URL
- arbitrary external navigation

The composition planner still receives only:

```json
{
  "template_id": "...",
  "media_ids": ["..."]
}
```

## Runtime boundary

The planner, renderer and browser runtime are injected dependencies.

This keeps the browser mount separate from server identity/session authorization.

The existing CREATE preview function remains fail-closed when the authoritative browser execution plan reports `runtime_ready=false`.

Production private-media delivery is still inactive, so this source integration does not make Production Creative rendering available.

## Production boundary

Not included:

- Production Member route wiring
- Production authentication activation
- Production private media activation
- Production D1/schema changes
- customer photo upload
- generated output server persistence
- automatic Creative generation
- paid processing

## Safety status

- canonical CREATE mount = implemented
- existing CREATE UI reuse = implemented
- existing plan/renderer chain reuse = implemented
- explicit save = implemented
- Blob revocation lifecycle = implemented
- automatic fetch = 0
- automatic preview = 0
- automatic download = 0
- generated output server write = 0
- Customer ID exposure = 0
- Family ID exposure = 0
- Production route wiring = 0
- Production write = 0
