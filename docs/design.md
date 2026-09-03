# Interface design contract

The production interface combines two of the static directions in `design/mocks`:

- `03-runbook.html` supplies the sequential workflow, metrics strip, settings rail, topology editor, traffic controls, and packet-evidence table.
- `01-bench-console.html` supplies the warm neutral palette, compact native-process diagnostics, monospace evidence values, and restrained borders.

`02-signal-sheet.html` remains a useful dark alternative, but it makes the lifecycle and scenario-editing sequence less obvious for a first release.

## Product-specific rules

Meshtastic Lab is an experiment console, not a monitoring landing page. The interface therefore prioritizes scenario inputs, native-node status, directed topology, traffic definition, and packet evidence in that order. Metrics use a continuous strip rather than detached cards. Tables retain exact identifiers and units. The RF profile and encrypted logical channel are visually separated.

The interface uses IBM Plex Sans and IBM Plex Mono when locally available, with system fallbacks. Warm paper and graphite colors distinguish the application from generic blue SaaS dashboards. Orange is reserved for the active workflow edge. Green, amber, and red communicate state only. There are no decorative pills, gradients, shadows, or continuously animated status indicators.

## Required states

- Loading shows stable skeleton rows with text labels and no repainting animation.
- API or WebSocket failure remains visible in a dismissible notice. REST polling remains authoritative if the stream reconnects.
- Native collision capability is visible in the runtime facts. Start remains disabled when the capability is unavailable.
- Firmware-owned scenario fields explain that they are locked outside `STOPPED`.
- Percentiles with insufficient samples render `Unavailable`, never zero.
- Copy controls report the copied endpoint in a dismissible notice.
- Keyboard focus uses a two-pixel graphite outline with an offset. Every matrix control has a complete accessible label.
- At widths below 900 pixels, the settings rail becomes a full-width first section, the metrics strip scrolls horizontally, and tables retain horizontal scrolling. No controls become inaccessible.

## Finish gate

Before release, verify the real backend path at desktop and narrow widths, keyboard access to lifecycle and topology controls, loading and failure notices, copy feedback, runtime field locking, slow WebSocket behavior, and packet-table filtering. Do not replace native evidence with frontend fixtures in the primary smoke test.
