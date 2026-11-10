// Pool widget and tool renderers. The TUI helpers are injected so the core can
// run under plain Node in tests.

export interface TuiDeps {
	Text: new (text: string, paddingX: number, paddingY: number) => any;
	truncateToWidth: (text: string, width: number) => string;
}

export interface PoolRow {
	name: string;
	model: string;
	color: string;
	purpose: string;
	pct: number | null;
	pending: boolean;
	stale: boolean;
}

export function hexFg(hex: string, s: string): string {
	const r = parseInt(hex.slice(1, 3), 16);
	const g = parseInt(hex.slice(3, 5), 16);
	const b = parseInt(hex.slice(5, 7), 16);
	return `\x1b[38;2;${r};${g};${b}m${s}\x1b[39m`;
}

function abbreviateModel(model: string): string {
	let m = model || "";
	if (m.startsWith("claude-")) m = m.slice("claude-".length);
	return m.slice(0, 14);
}

export function renderPool(
	tui: TuiDeps,
	rows: PoolRow[],
	self: { name: string; color: string } | null,
	width: number,
	theme: any,
): string[] {
	const safeWidth = Math.max(0, width);
	let top: string;
	let bottom: string;
	if (safeWidth < 12) {
		top = theme.fg("dim", "━".repeat(safeWidth));
		bottom = top;
	} else {
		const left = theme.fg("dim", "┏━") + theme.fg("border", " coms ");
		const tagLen = self ? self.name.length + 4 : 0;
		const remaining = safeWidth - 9 - tagLen - 1;
		if (self && remaining >= 1) {
			top = left + theme.fg("dim", "━" + "━".repeat(remaining) + " ")
				+ hexFg(self.color, self.name) + theme.fg("dim", " ━┓");
		} else {
			top = left + theme.fg("dim", "━".repeat(Math.max(0, safeWidth - 9)) + "┓");
		}
		bottom = theme.fg("dim", "┗" + "━".repeat(safeWidth - 2) + "┛");
	}

	if (rows.length === 0) {
		return [top, tui.truncateToWidth(" " + theme.fg("muted", "no peers connected"), width), bottom];
	}

	const out = [top];
	for (const r of [...rows].sort((a, b) => a.name.localeCompare(b.name))) {
		const filled = Math.max(0, Math.min(15, Math.round(((r.pct ?? 0) / 100) * 15)));
		const pctLabel = r.pct == null ? "--%" : `${r.pct}%`;
		if (r.stale) {
			const row = `✗ ${r.name.padEnd(12)} ${abbreviateModel(r.model).padEnd(14)} [${"-".repeat(15)}] ${pctLabel.padStart(4)}  —  ${r.purpose}`;
			out.push(tui.truncateToWidth(" " + theme.fg("dim", row), width));
			continue;
		}
		const swatch = r.pending ? theme.fg("dim", "●") : hexFg(r.color, "●");
		const bar = r.pending
			? theme.fg("dim", "-".repeat(15))
			: hexFg(r.color, "#".repeat(filled)) + theme.fg("dim", "-".repeat(15 - filled));
		out.push(tui.truncateToWidth(
			" " + swatch + " " + theme.fg("accent", r.name.padEnd(12)) + " "
				+ theme.fg("dim", abbreviateModel(r.model).padEnd(14)) + " "
				+ theme.fg("warning", "[") + bar + theme.fg("warning", "]")
				+ " " + theme.fg("accent", pctLabel.padStart(4))
				+ theme.fg("dim", "  —  ") + theme.fg("muted", r.purpose),
			width,
		));
	}
	out.push(bottom);
	return out;
}

function preview(text: unknown, max = 60): string {
	const s = String(text ?? "").replace(/\s+/g, " ");
	return s.length > max ? s.slice(0, max - 3) + "..." : s;
}

/** renderCall/renderResult pairs keyed by tool name. */
export function toolRenderers(tui: TuiDeps) {
	const { Text } = tui;
	const title = (theme: any, name: string) => theme.fg("toolTitle", theme.bold(name));
	const errorOr = (result: any, theme: any, ok: () => string) => {
		const d = result.details as any;
		if (result.isError || d?.error) {
			const t = result.content?.[0];
			return new Text(theme.fg("error", `✗ ${d?.error ?? (t?.type === "text" ? t.text : "error")}`), 0, 0);
		}
		return new Text(ok(), 0, 0);
	};
	return {
		coms_list: {
			renderCall: (args: any, theme: any) =>
				new Text(title(theme, "coms_list") + theme.fg("dim", args.project ? ` ${args.project}` : ""), 0, 0),
			renderResult: (result: any, options: any, theme: any) => {
				const agents: any[] = result.details?.agents ?? [];
				const header = theme.fg("accent", `📡 ${agents.length} peer(s)`);
				if (!options.expanded || agents.length === 0) return new Text(header, 0, 0);
				const rows = agents.map((a) =>
					`${a.alive ? theme.fg("success", "●") : theme.fg("error", "✗")} ${theme.fg("accent", a.name)} ${theme.fg("dim", a.model)}`
				);
				return new Text([header, ...rows].join("\n"), 0, 0);
			},
		},
		coms_send: {
			renderCall: (args: any, theme: any) =>
				new Text(title(theme, "coms_send ") + theme.fg("accent", args.target ?? "?")
					+ theme.fg("dim", " — ") + theme.fg("muted", preview(args.message)), 0, 0),
			renderResult: (result: any, _options: any, theme: any) =>
				errorOr(result, theme, () => {
					const d = result.details;
					return theme.fg("success", "→ ") + theme.fg("accent", d.target) + theme.fg("dim", "  msg_id ") + theme.fg("warning", d.msg_id);
				}),
		},
		coms_reply: {
			renderCall: (args: any, theme: any) =>
				new Text(title(theme, "coms_reply ") + theme.fg("warning", args.msg_id ?? "?")
					+ theme.fg("dim", " — ") + theme.fg("muted", preview(args.response)), 0, 0),
			renderResult: (result: any, _options: any, theme: any) =>
				errorOr(result, theme, () => theme.fg("success", "✓ reply sent to ") + theme.fg("accent", result.details.target)),
		},
		coms_await: {
			renderCall: (args: any, theme: any) => new Text(title(theme, "coms_await ") + theme.fg("warning", args.msg_id ?? "?"), 0, 0),
			renderResult: (result: any, _options: any, theme: any) =>
				errorOr(result, theme, () => theme.fg("success", "✓ reply received")),
		},
	};
}
