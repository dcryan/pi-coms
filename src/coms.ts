// coms: peer-to-peer messaging between pi sessions on the same machine.
//
// Every message gets an id. The receiver answers by calling coms_reply with
// that id, so a reply can only ever go to the message it answers. Replies the
// sender is not blocking on are delivered back into its conversation.

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import type * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { createRegistry, type RegistryEntry } from "./registry.ts";
import { type PoolRow, renderPool, toolRenderers, type TuiDeps } from "./render.ts";
import { bindEndpoint, isDeadEndpointError, LINE_CAP_BYTES, makeEndpoint, nowIso, readOneLine, sendEnvelope, ulid } from "./transport.ts";

export interface ComsOptions {
	comsDir?: string;
	maxHops?: number;
	timeoutMs?: number;
	pingIntervalMs?: number;
	pingTimeoutMs?: number;
	/** A heartbeat older than this, plus a failed ping, means the agent is hung. */
	staleMs?: number;
	tui?: TuiDeps;
}

interface Envelope {
	type: "prompt" | "response" | "ping";
	msg_id: string;
	sender_session: string;
	sender_endpoint: string;
	hops: number;
	timestamp: string;
}

interface PromptEnvelope extends Envelope {
	type: "prompt";
	prompt: string;
	sender_name: string;
	sender_cwd: string;
	response_schema?: object | null;
}

interface ResponseEnvelope extends Envelope {
	type: "response";
	sender_name?: string;
	response: unknown;
	error?: string | null;
}

interface AgentCard {
	name: string;
	purpose: string;
	model: string;
	color: string;
	context_used_pct: number;
	queue_depth: number;
}

interface Result {
	response?: unknown;
	error?: string | null;
}

/** A message this agent sent and is waiting on. */
interface Outbound {
	msg_id: string;
	target_name: string;
	target_session: string;
	hops: number;
	created_at: string;
	promise: Promise<Result>;
	resolve: (r: Result) => void;
	timer: ReturnType<typeof setTimeout> | null;
	result?: Result;
	/** coms_await calls currently blocking on this message. */
	awaiting: number;
}

/** A message this agent received and has not answered yet. */
interface Inbound {
	msg_id: string;
	hops: number;
	sender_name: string;
	sender_session: string;
	sender_endpoint: string;
	response_schema: object | null;
	/** The message has reached the model's context. */
	delivered: boolean;
	/** The agent was already told once to answer it. */
	reminded: boolean;
}

const FALLBACK_PALETTE = ["#72F1B8", "#36F9F6", "#FF7EDB", "#FEDE5D", "#C792EA", "#FF8B39", "#4D9DE0", "#FFAA8B"];
const KEEPALIVE_INTERVAL_MS = 30_000;

function isValidHex(hex: string | undefined): hex is string {
	return !!hex && /^#[0-9a-fA-F]{6}$/.test(hex);
}

function fallbackColor(sessionId: string): string {
	const h = crypto.createHash("sha256").update(sessionId).digest();
	return FALLBACK_PALETTE[h[0] % FALLBACK_PALETTE.length];
}

function stringify(value: unknown): string {
	return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function text(t: string) {
	return [{ type: "text" as const, text: t }];
}

/** name/description/color from the frontmatter of a --system-prompt or --append-system-prompt .md file. */
function frontmatterFromArgv(argv: string[]): { name?: string; description?: string; color?: string } {
	for (const flag of ["--system-prompt", "--append-system-prompt"]) {
		const i = argv.indexOf(flag);
		const file = i >= 0 ? argv[i + 1] : undefined;
		if (!file?.endsWith(".md")) continue;
		try {
			const match = fs.readFileSync(file, "utf-8").match(/^---\n([\s\S]*?)\n---\n/);
			if (!match) return {};
			const fm: Record<string, string> = {};
			for (const line of match[1].split("\n")) {
				const idx = line.indexOf(":");
				if (idx <= 0) continue;
				fm[line.slice(0, idx).trim()] = line.slice(idx + 1).trim().replace(/^(["'])(.*)\1$/, "$2");
			}
			return { name: fm.name, description: fm.description, color: fm.color };
		} catch {
			return {};
		}
	}
	return {};
}

export function installComs(pi: any, options: ComsOptions = {}) {
	const comsDir = options.comsDir ?? process.env.PI_COMS_DIR ?? path.join(os.homedir(), ".pi", "coms");
	const maxHops = options.maxHops ?? (Number(process.env.PI_COMS_MAX_HOPS) || 8);
	const timeoutMs = options.timeoutMs ?? (Number(process.env.PI_COMS_TIMEOUT_MS) || 1_800_000);
	const pingIntervalMs = options.pingIntervalMs ?? (Number(process.env.PI_COMS_PING_INTERVAL_MS) || 10_000);
	const pingTimeoutMs = options.pingTimeoutMs ?? 2_000;
	const staleMs = options.staleMs ?? 3 * KEEPALIVE_INTERVAL_MS;
	const tui = options.tui;
	const registry = createRegistry(comsDir);
	const renderers = tui ? toolRenderers(tui) : null;

	pi.registerFlag("coms-name", { description: "coms agent name (default: system prompt frontmatter, then agent-XXXXXX)", type: "string" });
	pi.registerFlag("coms-purpose", { description: "coms agent purpose shown to peers", type: "string" });
	pi.registerFlag("coms-project", { description: "coms project namespace for peer discovery", type: "string", default: "default" });
	pi.registerFlag("coms-color", { description: "coms agent color as #RRGGBB", type: "string" });
	pi.registerFlag("coms-explicit", { description: "Hide this agent from discovery; peers must address it by exact name", type: "boolean", default: false });

	let identity: {
		session_id: string;
		name: string;
		purpose: string;
		color: string;
		project: string;
		explicit: boolean;
		cwd: string;
		endpoint: string;
		started_at: string;
		registryFile: string;
	} | null = null;
	let currentCtx: any = null;
	let server: net.Server | null = null;
	let pingTimer: ReturnType<typeof setInterval> | null = null;
	let keepaliveTimer: ReturnType<typeof setInterval> | null = null;
	const peerCards = new Map<string, AgentCard & { staleCount: number }>();
	const outbound = new Map<string, Outbound>();
	const inbound = new Map<string, Inbound>();
	// Hops of replies delivered during the current run; cleared when it settles.
	const replyHops = new Map<string, number>();

	function log(event: string, data: Record<string, unknown> = {}): void {
		try { pi.appendEntry("coms-log", { event, ...data }); } catch { /* best-effort */ }
	}

	function updateStatus(): void {
		if (!identity || !currentCtx?.hasUI) return;
		const unanswered = inbound.size;
		const suffix = unanswered > 0 ? ` · ${unanswered} unanswered` : "";
		try { currentCtx.ui.setStatus("coms", `📡 ${identity.name}@${identity.project}${suffix}`); } catch { /* no UI */ }
	}

	/**
	 * Hop count for a message sent now. A send made while handling a message
	 * continues that message's chain; replies count as a hop, so two agents
	 * bouncing messages back and forth also hit the limit.
	 */
	function chainHops(onBehalfOf?: string): number {
		if (onBehalfOf) {
			const parent = inbound.get(onBehalfOf);
			if (parent) return parent.hops + 1;
		}
		const active = [
			...[...inbound.values()].filter((i) => i.delivered).map((i) => i.hops),
			...replyHops.values(),
		];
		return active.length === 0 ? 0 : Math.max(...active) + 1;
	}

	// ━━ Delivery into this conversation ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

	function deliverInbound(msg: Inbound, prompt: string, senderCwd: string): void {
		const schema = msg.response_schema
			? `\n\nYour response must be JSON matching this schema:\n${JSON.stringify(msg.response_schema, null, 2)}`
			: "";
		pi.sendMessage(
			{
				customType: "coms-inbound",
				content: `📨 coms message from ${msg.sender_name} (msg_id ${msg.msg_id}, cwd ${senderCwd})\n\n${prompt}${schema}\n\n`
					+ `Answer with coms_reply(msg_id: "${msg.msg_id}"). Only coms_reply reaches ${msg.sender_name}.`,
				display: true,
				details: { msg_id: msg.msg_id, sender: msg.sender_name },
			},
			{ deliverAs: "followUp", triggerTurn: true },
		);
	}

	function deliverReply(out: Outbound): void {
		const r = out.result!;
		const body = r.error ? `✗ ${r.error}` : stringify(r.response);
		pi.sendMessage(
			{
				customType: "coms-reply",
				content: `📬 coms reply from ${out.target_name} to msg_id ${out.msg_id}\n\n${body}`,
				display: true,
				details: { msg_id: out.msg_id, hops: out.hops + 1, error: r.error ?? null },
			},
			{ deliverAs: "followUp", triggerTurn: true },
		);
	}

	function settleOutbound(out: Outbound, result: Result): void {
		if (out.result) return;
		out.result = result;
		if (out.timer) clearTimeout(out.timer);
		out.timer = null;
		out.resolve(result);
		if (out.awaiting === 0) deliverReply(out);
	}

	async function sendResponse(msg: Inbound, response: unknown, error: string | null): Promise<void> {
		const env: ResponseEnvelope = {
			type: "response",
			msg_id: msg.msg_id,
			sender_session: identity!.session_id,
			sender_endpoint: identity!.endpoint,
			sender_name: identity!.name,
			hops: msg.hops,
			timestamp: nowIso(),
			response,
			error,
		};
		await sendEnvelope(msg.sender_endpoint, env);
	}

	/** Tells the sender this message will not be answered, then forgets it. */
	async function failInbound(msg: Inbound, error: string): Promise<void> {
		inbound.delete(msg.msg_id);
		updateStatus();
		try {
			await sendResponse(msg, null, error);
			log("inbound_failed", { msg_id: msg.msg_id, error });
		} catch (e: any) {
			log("inbound_failed_undeliverable", { msg_id: msg.msg_id, error, reason: e?.message });
		}
	}

	// ━━ Socket handlers ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

	function answer(socket: net.Socket, record: object): void {
		try { socket.end(JSON.stringify(record) + "\n"); } catch { /* peer gone */ }
	}

	function handlePrompt(socket: net.Socket, env: PromptEnvelope): void {
		if (typeof env.hops !== "number" || env.hops >= maxHops) {
			return answer(socket, { type: "nack", msg_id: env.msg_id, error: `hop limit reached (${env.hops} >= ${maxHops})` });
		}
		if (typeof env.prompt !== "string" || typeof env.sender_name !== "string") {
			return answer(socket, { type: "nack", msg_id: env.msg_id, error: "malformed prompt" });
		}
		if (inbound.has(env.msg_id)) return answer(socket, { type: "ack", msg_id: env.msg_id });
		const msg: Inbound = {
			msg_id: env.msg_id,
			hops: env.hops,
			sender_name: env.sender_name,
			sender_session: env.sender_session,
			sender_endpoint: env.sender_endpoint,
			response_schema: env.response_schema ?? null,
			delivered: false,
			reminded: false,
		};
		inbound.set(msg.msg_id, msg);
		try {
			deliverInbound(msg, env.prompt, env.sender_cwd ?? "?");
		} catch {
			inbound.delete(msg.msg_id);
			return answer(socket, { type: "nack", msg_id: env.msg_id, error: "receiver could not queue the message" });
		}
		answer(socket, { type: "ack", msg_id: env.msg_id });
		updateStatus();
		log("inbound_prompt", { msg_id: env.msg_id, sender: env.sender_name, hops: env.hops });
	}

	function handleResponse(socket: net.Socket, env: ResponseEnvelope): void {
		const out = outbound.get(env.msg_id);
		answer(socket, { type: "ack", msg_id: env.msg_id });
		if (!out || out.result) {
			log("orphan_response", { msg_id: env.msg_id });
			return;
		}
		settleOutbound(out, { response: env.response, error: env.error ?? null });
		log("outbound_settled", { msg_id: env.msg_id, error: env.error ?? null });
	}

	function handlePing(socket: net.Socket, env: Envelope): void {
		const card: AgentCard = {
			name: identity?.name ?? "unknown",
			purpose: identity?.purpose ?? "",
			model: currentCtx?.model?.id ?? "unknown",
			color: identity?.color ?? FALLBACK_PALETTE[0],
			context_used_pct: Math.round(currentCtx?.getContextUsage?.()?.percent ?? 0),
			queue_depth: inbound.size,
		};
		answer(socket, { type: "pong", msg_id: env.msg_id, agent_card: card });
	}

	async function connHandler(socket: net.Socket): Promise<void> {
		socket.on("error", () => socket.destroy());
		let parsed: any;
		try {
			parsed = JSON.parse(await readOneLine(socket, LINE_CAP_BYTES));
		} catch {
			return answer(socket, { type: "nack", msg_id: "", error: "malformed envelope" });
		}
		if (!parsed || typeof parsed.msg_id !== "string" || typeof parsed.sender_endpoint !== "string") {
			return answer(socket, { type: "nack", msg_id: parsed?.msg_id ?? "", error: "malformed envelope" });
		}
		try {
			if (parsed.type === "prompt") handlePrompt(socket, parsed);
			else if (parsed.type === "response") handleResponse(socket, parsed);
			else if (parsed.type === "ping") handlePing(socket, parsed);
			else answer(socket, { type: "nack", msg_id: parsed.msg_id, error: "unknown type" });
		} catch {
			answer(socket, { type: "nack", msg_id: parsed.msg_id, error: "internal error" });
		}
	}

	// ━━ Pool widget ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

	function poolRows(): PoolRow[] {
		if (!identity) return [];
		const rows: PoolRow[] = [];
		for (const [sid, card] of peerCards) {
			if (sid === identity.session_id) continue;
			rows.push({ ...card, pct: card.context_used_pct, pending: false, stale: card.staleCount >= 3 });
		}
		const seen = new Set(rows.map((r) => r.name));
		for (const entry of registry.read(identity.project)) {
			if (entry.session_id === identity.session_id || entry.explicit || peerCards.has(entry.session_id) || seen.has(entry.name)) continue;
			rows.push({ name: entry.name, model: entry.model, color: entry.color, purpose: entry.purpose, pct: null, pending: true, stale: false });
		}
		return rows;
	}

	function installPoolWidget(): void {
		if (!tui || !currentCtx?.hasUI) return;
		try {
			currentCtx.ui.setWidget("coms-pool", (_tui: unknown, theme: any) => ({
				invalidate() {},
				render: (width: number) => renderPool(tui, poolRows(), identity, width, theme),
			}), { placement: "belowEditor" });
		} catch { /* non-fatal */ }
	}

	function heartbeatStale(entry: RegistryEntry): boolean {
		const at = Date.parse(entry.heartbeat_at ?? entry.started_at);
		return Number.isNaN(at) || Date.now() - at > staleMs;
	}

	/**
	 * Pings a peer and removes its registry entry if it's gone: nothing listens
	 * on its socket, or it doesn't answer and its heartbeat is stale. A stale
	 * heartbeat alone isn't enough, since a sleeping laptop pauses every
	 * agent's heartbeat at once.
	 */
	async function pingPeer(entry: RegistryEntry): Promise<AgentCard | null> {
		if (!identity) return null;
		try {
			const pong = await sendEnvelope(entry.endpoint, {
				type: "ping",
				msg_id: ulid(),
				sender_session: identity.session_id,
				sender_endpoint: identity.endpoint,
				hops: 0,
				timestamp: nowIso(),
			}, pingTimeoutMs);
			return pong?.type === "pong" ? pong.agent_card : null;
		} catch (err) {
			if (isDeadEndpointError(err) || heartbeatStale(entry)) {
				registry.remove(entry);
				log("peer_removed", { session_id: entry.session_id, name: entry.name, reason: (err as any)?.code ?? "stale" });
			}
			return null;
		}
	}

	/** Fails messages still waiting on a peer that is no longer registered. */
	function failOrphanedOutbound(liveSessions: Set<string>): void {
		for (const out of outbound.values()) {
			if (out.result || liveSessions.has(out.target_session)) continue;
			settleOutbound(out, { error: `${out.target_name} is no longer running` });
			log("outbound_orphaned", { msg_id: out.msg_id, target: out.target_name });
		}
	}

	async function refreshPool(): Promise<void> {
		if (!identity) return;
		const peers = registry.live(identity.project).filter((e) => e.session_id !== identity!.session_id);
		const cards = await Promise.all(peers.map((p) => pingPeer(p)));
		failOrphanedOutbound(new Set(registry.liveAll().map((e) => e.session_id)));
		let changed = false;
		const seen = new Set<string>();
		peers.forEach((peer, i) => {
			const card = cards[i];
			if (!card || peer.explicit) return;
			seen.add(peer.session_id);
			const next = { ...card, staleCount: 0 };
			const prev = peerCards.get(peer.session_id);
			if (!prev || JSON.stringify(prev) !== JSON.stringify(next)) {
				peerCards.set(peer.session_id, next);
				changed = true;
			}
		});
		for (const [sid, card] of peerCards) {
			if (seen.has(sid)) continue;
			card.staleCount++;
			if (card.staleCount > 6) peerCards.delete(sid);
			changed = true;
		}
		if (changed) installPoolWidget();
	}

	function writeRegistryEntry(): void {
		if (!identity) return;
		const entry: RegistryEntry = {
			session_id: identity.session_id,
			project: identity.project,
			name: identity.name,
			purpose: identity.purpose,
			model: currentCtx?.model?.id ?? "unknown",
			color: identity.color,
			pid: process.pid,
			endpoint: identity.endpoint,
			cwd: identity.cwd,
			started_at: identity.started_at,
			explicit: identity.explicit,
			version: 2,
			context_used_pct: Math.round(currentCtx?.getContextUsage?.()?.percent ?? 0),
			queue_depth: inbound.size,
			heartbeat_at: nowIso(),
		};
		identity.registryFile = registry.write(entry, identity.project);
	}

	// ━━ Lifecycle ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

	pi.on("session_start", async (_event: unknown, ctx: any) => {
		currentCtx = ctx;
		if (identity) return;
		const fm = frontmatterFromArgv(process.argv);
		const flag = (name: string) => {
			const v = pi.getFlag(name);
			return typeof v === "string" && v.length > 0 ? v : undefined;
		};
		const project = flag("coms-project") ?? "default";
		const session_id = ulid();
		const desired = flag("coms-name") ?? fm.name ?? `agent-${session_id.slice(-6).toLowerCase()}`;
		const name = registry.uniqueName(project, desired);
		const color = [flag("coms-color"), fm.color].find(isValidHex) ?? fallbackColor(session_id);
		const endpoint = makeEndpoint(comsDir, session_id);

		try {
			fs.mkdirSync(comsDir, { recursive: true, mode: 0o700 });
			fs.chmodSync(comsDir, 0o700);
			if (process.platform !== "win32") fs.mkdirSync(path.join(comsDir, "sockets"), { recursive: true, mode: 0o700 });
			server = await bindEndpoint(endpoint, (socket) => void connHandler(socket));
		} catch (err: any) {
			ctx.ui?.notify?.(`📡 coms: could not start — ${err?.message ?? err}`, "error");
			return;
		}

		identity = {
			session_id,
			name,
			purpose: flag("coms-purpose") ?? fm.description ?? "",
			color,
			project,
			explicit: pi.getFlag("coms-explicit") === true,
			cwd: ctx.cwd ?? process.cwd(),
			endpoint,
			started_at: nowIso(),
			registryFile: "",
		};
		try {
			writeRegistryEntry();
		} catch (err: any) {
			ctx.ui?.notify?.(`📡 coms: registry write failed — ${err?.message ?? err}`, "error");
			server.close();
			server = null;
			identity = null;
			return;
		}
		if (name !== desired) log("name_collision", { desired, assigned: name, project });
		log("boot", { session_id, name, project });

		updateStatus();
		installPoolWidget();
		pingTimer = setInterval(() => void refreshPool().catch(() => {}), pingIntervalMs);
		pingTimer.unref?.();
		keepaliveTimer = setInterval(() => { try { writeRegistryEntry(); } catch { /* best-effort */ } }, KEEPALIVE_INTERVAL_MS);
		keepaliveTimer.unref?.();
		void refreshPool().catch(() => {});
	});

	// A message has reached the model once pi emits it; only then can the agent
	// be expected to answer it.
	pi.on("message_end", async (event: any) => {
		const m = event?.message;
		if (m?.role !== "custom") return;
		if (m.customType === "coms-inbound") {
			const msg = inbound.get(m.details?.msg_id);
			if (msg) msg.delivered = true;
		} else if (m.customType === "coms-reply" && typeof m.details?.hops === "number") {
			replyHops.set(m.details.msg_id, m.details.hops);
		}
	});

	// Before going idle, remind the agent once about messages it has read but
	// not answered.
	pi.on("agent_before_settle", async (event: any) => {
		const unanswered = [...inbound.values()].filter((m) => m.delivered && !m.reminded);
		if (unanswered.length === 0) return;
		for (const m of unanswered) m.reminded = true;
		const list = unanswered.map((m) => `${m.msg_id} from ${m.sender_name}`).join(", ");
		return {
			entries: [...(event.entries ?? []), {
				type: "custom_message",
				customType: "coms-reminder",
				content: `You have not answered coms message(s): ${list}. Call coms_reply for each now. `
					+ "Your normal replies are not sent to peers. If you cannot help, use coms_reply to say so.",
				display: true,
			}],
			continue: true,
		};
	});

	// A run that ignored its reminder fails the message so the sender is not
	// left waiting. An aborted run leaves messages pending for the next run.
	pi.on("agent_settled", async (event: any) => {
		replyHops.clear();
		for (const m of [...inbound.values()]) {
			if (!m.delivered || !m.reminded) continue;
			if (event?.aborted) m.reminded = false;
			else await failInbound(m, `${identity?.name ?? "receiver"} finished its turn without replying`);
		}
	});

	pi.on("session_shutdown", async () => {
		if (pingTimer) clearInterval(pingTimer);
		if (keepaliveTimer) clearInterval(keepaliveTimer);
		pingTimer = keepaliveTimer = null;
		if (identity) {
			await Promise.all([...inbound.values()].map((m) => failInbound(m, `${identity!.name} shut down before replying`)));
		}
		for (const out of outbound.values()) if (out.timer) clearTimeout(out.timer);
		server?.close();
		server = null;
		if (identity) {
			registry.remove(identity);
			log("shutdown", { session_id: identity.session_id });
		}
		if (currentCtx?.hasUI) {
			try { currentCtx.ui.setWidget("coms-pool", undefined); } catch { /* no UI */ }
		}
		identity = null;
	});

	// ━━ Tools ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

	function requireIdentity() {
		if (!identity) throw new Error("coms is not running in this session");
		return identity;
	}

	pi.registerTool({
		name: "coms_list",
		label: "Coms List",
		description: "List peer agents reachable through coms, with their purpose, model and context usage.",
		promptSnippet: "List peer pi agents you can message with coms_send",
		parameters: {
			type: "object",
			properties: {
				project: { type: "string", description: "Project namespace, or \"*\" for all. Defaults to your own." },
				include_explicit: { type: "boolean", description: "Include agents hidden with --coms-explicit." },
			},
		},
		async execute(_id: string, params: any) {
			const self = requireIdentity();
			const project = params.project ?? self.project;
			const entries = (project === "*" ? registry.liveAll() : registry.live(project))
				.filter((e) => e.session_id !== self.session_id && (params.include_explicit || !e.explicit));
			const cards = await Promise.all(entries.map((e) => pingPeer(e)));
			const agents = entries.map((e, i) => ({
				name: e.name,
				session_id: e.session_id,
				purpose: e.purpose,
				model: cards[i]?.model ?? e.model,
				cwd: e.cwd,
				alive: cards[i] != null,
				context_used_pct: cards[i]?.context_used_pct ?? null,
			}));
			const lines = agents.map((a) =>
				`${a.alive ? "●" : "✗"} ${a.name} (${a.model}) ${a.context_used_pct ?? "?"}% · ${a.cwd}${a.purpose ? ` — ${a.purpose}` : ""}`
			);
			return {
				content: text(agents.length === 0 ? "No peer agents found." : `${agents.length} peer(s):\n${lines.join("\n")}`),
				details: { agents, project },
			};
		},
		...renderers?.coms_list,
	});

	pi.registerTool({
		name: "coms_send",
		label: "Coms Send",
		description:
			"Send a message to a peer agent. Returns a msg_id once the peer has queued it. "
			+ "The peer's reply arrives later as a 'coms reply' message in this conversation.",
		promptSnippet: "Send a message to a peer pi agent; its reply arrives as a message",
		promptGuidelines: [
			"Replies to coms_send arrive automatically as 'coms reply' messages. Call coms_await only when you cannot continue without the answer.",
			"When you call coms_send while handling a coms message, set on_behalf_of to that message's msg_id.",
		],
		parameters: {
			type: "object",
			properties: {
				target: { type: "string", description: "Peer name (from coms_list) or session id." },
				message: { type: "string", description: "The message to send." },
				on_behalf_of: { type: "string", description: "msg_id of the coms message you are handling, if any." },
				response_schema: { type: "object", description: "Optional JSON Schema the reply must match." },
			},
			required: ["target", "message"],
		},
		async execute(_id: string, params: any) {
			const self = requireIdentity();
			const target = registry.resolve(params.target, self.project);
			if (!target) throw new Error(`no live agent named "${params.target}"; call coms_list to see peers`);
			if (target.session_id === self.session_id) throw new Error("cannot message yourself");
			const hops = chainHops(params.on_behalf_of);
			if (hops >= maxHops) throw new Error(`hop limit reached (${hops} >= ${maxHops}); stop relaying and report back instead`);

			const msg_id = ulid();
			let resolve!: (r: Result) => void;
			const out: Outbound = {
				msg_id,
				target_name: target.name,
				target_session: target.session_id,
				hops,
				created_at: nowIso(),
				promise: new Promise<Result>((r) => { resolve = r; }),
				resolve: (r) => resolve(r),
				timer: null,
				awaiting: 0,
			};
			// Registered before sending so an instant reply still finds it.
			outbound.set(msg_id, out);
			const env: PromptEnvelope = {
				type: "prompt",
				msg_id,
				sender_session: self.session_id,
				sender_endpoint: self.endpoint,
				sender_name: self.name,
				sender_cwd: self.cwd,
				hops,
				timestamp: nowIso(),
				prompt: params.message,
				response_schema: params.response_schema ?? null,
			};
			try {
				await sendEnvelope(target.endpoint, env);
			} catch (err: any) {
				outbound.delete(msg_id);
				if (isDeadEndpointError(err)) {
					registry.remove(target);
					throw new Error(`${target.name} is no longer running`);
				}
				throw new Error(`${target.name} did not accept the message: ${err?.message ?? err}`);
			}
			out.timer = setTimeout(() => settleOutbound(out, { error: `no reply from ${target.name} within ${Math.round(timeoutMs / 1000)}s` }), timeoutMs);
			out.timer.unref?.();
			log("outbound_prompt", { msg_id, target: target.name, hops });
			return {
				content: text(`Sent to ${target.name} (msg_id ${msg_id}). The reply will arrive as a 'coms reply' message.`),
				details: { msg_id, target: target.name, target_session: target.session_id, hops },
			};
		},
		...renderers?.coms_send,
	});

	pi.registerTool({
		name: "coms_reply",
		label: "Coms Reply",
		description: "Answer a coms message. This is the only way a reply reaches the sender.",
		promptSnippet: "Answer a coms message from a peer agent by msg_id",
		promptGuidelines: [
			"When a 'coms message' arrives, answer it with coms_reply using its msg_id. Your normal assistant text is never sent to the sender.",
			"Call coms_reply exactly once per coms message. If you cannot help, use coms_reply to say why.",
		],
		parameters: {
			type: "object",
			properties: {
				msg_id: { type: "string", description: "msg_id of the coms message you are answering." },
				response: { type: "string", description: "Your reply. JSON text when the message asked for a schema." },
			},
			required: ["msg_id", "response"],
		},
		async execute(_id: string, params: any) {
			requireIdentity();
			const msg = inbound.get(params.msg_id);
			if (!msg) throw new Error(`no unanswered coms message with msg_id ${params.msg_id}`);
			let response: unknown = params.response;
			if (msg.response_schema) {
				try {
					response = JSON.parse(params.response);
				} catch {
					throw new Error("this message asked for JSON; send the response as valid JSON text");
				}
			}
			inbound.delete(msg.msg_id);
			updateStatus();
			try {
				await sendResponse(msg, response, null);
			} catch (err: any) {
				log("reply_undeliverable", { msg_id: msg.msg_id, reason: err?.message });
				throw new Error(`${msg.sender_name} is no longer reachable; the reply was not delivered`);
			}
			log("reply_sent", { msg_id: msg.msg_id, target: msg.sender_name });
			return { content: text(`Reply sent to ${msg.sender_name}.`), details: { msg_id: msg.msg_id, target: msg.sender_name } };
		},
		...renderers?.coms_reply,
	});

	pi.registerTool({
		name: "coms_await",
		label: "Coms Await",
		description: "Wait for the reply to a message sent with coms_send.",
		parameters: {
			type: "object",
			properties: {
				msg_id: { type: "string", description: "msg_id returned by coms_send." },
				timeout_ms: { type: "number", description: "How long to wait (default: until the message times out)." },
			},
			required: ["msg_id"],
		},
		async execute(_id: string, params: any, signal?: AbortSignal) {
			const out = outbound.get(params.msg_id);
			if (!out) throw new Error(`unknown msg_id ${params.msg_id}`);
			let result = out.result;
			if (!result) {
				out.awaiting++;
				let timer: ReturnType<typeof setTimeout> | undefined;
				let onAbort: (() => void) | undefined;
				try {
					result = await Promise.race([
						out.promise,
						new Promise<Result>((resolve) => {
							const ms = params.timeout_ms > 0 ? params.timeout_ms : timeoutMs;
							timer = setTimeout(() => resolve({ error: "still waiting; the reply will arrive as a message" }), ms);
						}),
						new Promise<Result>((resolve) => {
							onAbort = () => resolve({ error: "aborted; the reply will arrive as a message" });
							if (signal?.aborted) onAbort();
							else signal?.addEventListener("abort", onAbort, { once: true });
						}),
					]);
				} finally {
					clearTimeout(timer);
					if (onAbort) signal?.removeEventListener("abort", onAbort);
					out.awaiting--;
				}
			}
			if (result.error) return { content: text(`error: ${result.error}`), details: { error: result.error } };
			return { content: text(stringify(result.response)), details: { response: result.response } };
		},
		...renderers?.coms_await,
	});

	pi.registerCommand("coms", {
		description: "Show coms identity, peers and unanswered messages",
		handler: async (_args: string, ctx: any) => {
			if (!identity) return ctx.ui.notify("coms is not running in this session", "warning");
			await refreshPool();
			const waiting = [...outbound.values()].filter((o) => !o.result).map((o) => `→ ${o.target_name} ${o.msg_id}`);
			const unanswered = [...inbound.values()].map((m) => `← ${m.sender_name} ${m.msg_id}`);
			ctx.ui.notify(
				[`📡 ${identity.name}@${identity.project} · ${peerCards.size} peer(s)`, ...waiting, ...unanswered].join("\n"),
				"info",
			);
		},
	});

	// Exposed for tests.
	return {
		get identity() { return identity; },
		inbound,
		outbound,
	};
}
