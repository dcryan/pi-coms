import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, test } from "node:test";
import { installComs } from "../src/coms.ts";
import { createRegistry } from "../src/registry.ts";

let comsDir: string;
let agents: Agent[] = [];
// Session files: custom entries an extension appended, by file path.
const sessions = new Map<string, any[]>();

beforeEach(() => {
	// Short base path: unix socket paths are limited to ~104 bytes on macOS.
	comsDir = fs.mkdtempSync("/tmp/coms-");
});

afterEach(async () => {
	for (const a of agents) await a.shutdown();
	agents = [];
	sessions.clear();
	fs.rmSync(comsDir, { recursive: true, force: true });
});

type Agent = Awaited<ReturnType<typeof agent>>;

/**
 * A session with the extension loaded. With a name it joins at startup like
 * `--coms-name`; with null it starts dormant.
 */
async function agent(
	name: string | null,
	opts: Record<string, unknown> = {},
	extraFlags: Record<string, unknown> = {},
	session: { file?: string; start?: Record<string, unknown>; branch?: () => any[] } = {},
) {
	const handlers = new Map<string, Function[]>();
	const tools = new Map<string, any>();
	const commands = new Map<string, any>();
	const notices: { text: string; level: string }[] = [];
	let stale = false;
	let activeTools = new Set(["read", "bash"]);
	const flags: Record<string, unknown> = { "coms-project": "test", ...(name ? { "coms-name": name } : {}), ...extraFlags };
	// Messages pi has queued for the model but not yet emitted.
	const queued: any[] = [];
	const transcript: any[] = [];
	const entries = session.file ? (sessions.get(session.file) ?? sessions.set(session.file, []).get(session.file)!) : [];
	const pi = {
		on: (event: string, fn: Function) => handlers.set(event, [...(handlers.get(event) ?? []), fn]),
		registerTool: (def: any) => {
			tools.set(def.name, def);
			if (def.defaultActive !== false) activeTools.add(def.name);
		},
		getActiveTools: () => [...activeTools],
		setActiveTools: (names: string[]) => { activeTools = new Set(names.filter((n) => tools.has(n) || ["read", "bash"].includes(n))); },
		registerFlag: () => {},
		getFlag: (n: string) => flags[n],
		registerCommand: (n: string, def: any) => commands.set(n, def),
		appendEntry: (customType: string, data: unknown) => entries.push({ type: "custom", customType, data }),
		sendMessage: (message: any, options: any) => queued.push({ message, options }),
	};
	const ctx = {
		hasUI: false,
		cwd: "/tmp",
		model: { id: "test-model" },
		getContextUsage: () => ({ percent: 10 }),
		ui: {
			// Like pi, a ctx from before the session was replaced throws when used.
			notify: (text: string, level: string) => {
				if (stale) throw new Error("this extension ctx is stale after session replacement");
				notices.push({ text, level });
			},
			setStatus() {},
			setWidget() {},
		},
		sessionManager: {
			getSessionFile: () => session.file,
			getSessionId: () => `id:${session.file ?? "ephemeral"}`,
			getEntries: () => entries,
			// The current branch; tests can make it differ from the whole file.
			getBranch: session.branch ?? (() => entries),
		},
	};
	const state = installComs(pi, { comsDir, pingIntervalMs: 60_000, ...opts });
	const emit = async (event: string, payload: any = {}) => {
		let result;
		for (const fn of handlers.get(event) ?? []) result = (await fn(payload, ctx)) ?? result;
		return result;
	};
	await emit("session_start", session.start ?? { reason: "startup" });

	return {
		get name() { return state.identity?.name ?? name; },
		state,
		notices,
		activeComsTools: () => [...activeTools].filter((t) => t.startsWith("coms_")).sort(),
		command: (args: string) => commands.get("coms").handler(args, ctx),
		complete: (prefix: string) => commands.get("coms").getArgumentCompletions(prefix)?.map((i: any) => i.value) ?? null,
		queued,
		transcript,
		tool: (toolName: string, params: any, signal?: AbortSignal) =>
			tools.get(toolName).execute("call-1", params, signal, undefined, ctx),
		/** pi emits queued messages to the model at the start of a run. */
		deliver: async () => {
			for (const q of queued.splice(0)) {
				transcript.push(q.message);
				await emit("message_end", { message: { role: "custom", ...q.message } });
			}
		},
		/** Ends a run the way pi does: before_settle (maybe continue), then settled. */
		settle: async (aborted = false) => {
			if (aborted) return emit("agent_settled", { aborted: true });
			const decision = await emit("agent_before_settle", { entries: [] });
			if (!decision?.continue) await emit("agent_settled", { aborted: false });
			return decision;
		},
		settled: (aborted = false) => emit("agent_settled", { aborted }),
		shutdown: () => emit("session_shutdown", { reason: "quit" }),
		/**
		 * /new, /resume, /fork or /reload: pi shuts this extension instance
		 * down and starts a fresh one in the same process.
		 */
		replace: async (reason: "new" | "resume" | "fork" | "reload", nextFile = session.file) => {
			await emit("session_shutdown", { reason });
			stale = true;
			// A fork starts as a copy of the conversation, records included.
			if (reason === "fork") sessions.set(nextFile!, [...entries]);
			const start = reason === "reload" ? { reason } : { reason, previousSessionFile: session.file };
			// A reload continues the same branch; the others start elsewhere.
			const branch = reason === "reload" ? session.branch : undefined;
			return agent(name, opts, extraFlags, { file: nextFile, start, branch });
		},
	};
}

async function until(cond: () => boolean, ms = 2000) {
	const end = Date.now() + ms;
	while (!cond()) {
		if (Date.now() > end) throw new Error("timed out waiting for condition");
		await new Promise((r) => setTimeout(r, 5));
	}
}

async function pair(opts = {}) {
	const a = await agent("alice", opts);
	const b = await agent("bob", opts);
	agents.push(a, b);
	return { a, b };
}

test("a reply goes back to the message it answers", async () => {
	const { a, b } = await pair();
	const sent = await a.tool("coms_send", { target: "bob", message: "what is 2+2?" });
	const msgId = sent.details.msg_id;

	await until(() => b.queued.length === 1);
	assert.equal(b.queued[0].message.customType, "coms-inbound");
	assert.equal(b.queued[0].message.details.msg_id, msgId);
	assert.match(b.queued[0].message.content, /what is 2\+2\?/);
	await b.deliver();

	await b.tool("coms_reply", { msg_id: msgId, response: "4" });
	await until(() => a.queued.length === 1);
	const reply = a.queued[0].message;
	assert.equal(reply.customType, "coms-reply");
	assert.equal(reply.details.msg_id, msgId);
	assert.match(reply.content, /\n4$/);
	assert.equal(b.state.inbound.size, 0);
});

test("coms_await returns the reply instead of delivering it as a message", async () => {
	const { a, b } = await pair();
	const { details } = await a.tool("coms_send", { target: "bob", message: "ping" });
	const waiting = a.tool("coms_await", { msg_id: details.msg_id });
	await until(() => b.queued.length === 1);
	await b.deliver();
	await b.tool("coms_reply", { msg_id: details.msg_id, response: "pong" });
	const result = await waiting;
	assert.equal(result.content[0].text, "pong");
	assert.equal(a.queued.length, 0);
});

test("the receiver's normal turns never answer a pending message", async () => {
	const { a, b } = await pair();
	const { details } = await a.tool("coms_send", { target: "bob", message: "hello" });
	await until(() => b.queued.length === 1);

	// A run the user started ends before the message reaches the model:
	// no reminder, no reply.
	assert.equal(await b.settle(), undefined);
	assert.equal(a.queued.length, 0);
	assert.equal(b.state.inbound.size, 1);

	// Once delivered and ignored, the agent is reminded exactly once.
	await b.deliver();
	const decision = await b.settle();
	assert.equal(decision.continue, true);
	assert.equal(decision.entries[0].customType, "coms-reminder");
	assert.match(decision.entries[0].content, new RegExp(details.msg_id));

	// Ignoring the reminder fails the message so the sender stops waiting.
	assert.equal(await b.settle(), undefined);
	await until(() => a.queued.length === 1);
	assert.match(a.queued[0].message.content, /bob finished its turn without replying/);
	assert.equal(a.queued[0].message.details.error, "bob finished its turn without replying");
	assert.equal(b.state.inbound.size, 0);
});

test("simultaneous messages are answered independently", async () => {
	const { a, b } = await pair();
	const first = (await a.tool("coms_send", { target: "bob", message: "first" })).details.msg_id;
	const second = (await a.tool("coms_send", { target: "bob", message: "second" })).details.msg_id;
	await until(() => b.queued.length === 2);
	await b.deliver();

	await b.tool("coms_reply", { msg_id: second, response: "answer to second" });
	await b.tool("coms_reply", { msg_id: first, response: "answer to first" });
	await until(() => a.queued.length === 2);
	const byId = Object.fromEntries(a.queued.map((q) => [q.message.details.msg_id, q.message.content]));
	assert.match(byId[first], /answer to first$/);
	assert.match(byId[second], /answer to second$/);
});

test("a message can only be answered once", async () => {
	const { a, b } = await pair();
	const { details } = await a.tool("coms_send", { target: "bob", message: "hi" });
	await until(() => b.queued.length === 1);
	await b.tool("coms_reply", { msg_id: details.msg_id, response: "one" });
	await assert.rejects(b.tool("coms_reply", { msg_id: details.msg_id, response: "two" }), /no unanswered coms message/);
});

test("an aborted run leaves the message pending for the next run", async () => {
	const { a, b } = await pair();
	const { details } = await a.tool("coms_send", { target: "bob", message: "hello" });
	await until(() => b.queued.length === 1);
	await b.deliver();
	await b.settle(true);
	assert.equal(b.state.inbound.size, 1);

	// Aborting the reminder run doesn't fail the message either.
	assert.equal((await b.settle()).continue, true);
	await b.settle(true);
	assert.equal(b.state.inbound.size, 1);
	assert.equal(a.queued.length, 0);

	// The next run reminds again.
	const decision = await b.settle();
	assert.equal(decision.continue, true);
	await b.tool("coms_reply", { msg_id: details.msg_id, response: "late but here" });
	await until(() => a.queued.length === 1);
	assert.match(a.queued[0].message.content, /late but here$/);
});

test("relays continue the hop count of the message being handled", async () => {
	const a = await agent("alice", { maxHops: 3 });
	const b = await agent("bob", { maxHops: 3 });
	const c = await agent("carol", { maxHops: 3 });
	agents.push(a, b, c);

	const m1 = await a.tool("coms_send", { target: "bob", message: "ask carol" });
	assert.equal(m1.details.hops, 0);
	await until(() => b.queued.length === 1);
	await b.deliver();
	const m2 = await b.tool("coms_send", { target: "carol", message: "q", on_behalf_of: m1.details.msg_id });
	assert.equal(m2.details.hops, 1);
	await until(() => c.queued.length === 1);
	await c.deliver();
	const m3 = await c.tool("coms_send", { target: "alice", message: "q", on_behalf_of: m2.details.msg_id });
	assert.equal(m3.details.hops, 2);
	await until(() => a.queued.length === 1);
	await a.deliver();
	await assert.rejects(
		a.tool("coms_send", { target: "bob", message: "q", on_behalf_of: m3.details.msg_id }),
		/hop limit reached/,
	);
});

test("back-and-forth replies count towards the hop limit", async () => {
	const { a, b } = await pair({ maxHops: 4 });
	let hops = 0;
	for (let round = 0; ; round++) {
		assert.ok(round < 10, "hop limit never reached");
		let sent;
		try {
			sent = await a.tool("coms_send", { target: "bob", message: `round ${round}` });
		} catch (err: any) {
			assert.match(err.message, /hop limit reached/);
			break;
		}
		hops = sent.details.hops;
		await until(() => b.queued.length === 1);
		await b.deliver();
		await b.tool("coms_reply", { msg_id: sent.details.msg_id, response: "ok" });
		await until(() => a.queued.length === 1);
		// The reply starts alice's next run without settling the previous one.
		await a.deliver();
		await b.settle();
	}
	assert.equal(hops, 2);

	// A run that settles resets the chain; the next user-driven send starts at 0.
	await a.settled();
	const fresh = await a.tool("coms_send", { target: "bob", message: "new topic" });
	assert.equal(fresh.details.hops, 0);
});

test("replies far larger than 64KB arrive intact", async () => {
	const { a, b } = await pair();
	const big = "x".repeat(2_000_000) + "é";
	const { details } = await a.tool("coms_send", { target: "bob", message: "send a lot" });
	await until(() => b.queued.length === 1);
	await b.tool("coms_reply", { msg_id: details.msg_id, response: big });
	const result = await a.tool("coms_await", { msg_id: details.msg_id });
	assert.equal(result.content[0].text, big);
});

test("a schema request rejects non-JSON replies without consuming the message", async () => {
	const { a, b } = await pair();
	const schema = { type: "object", properties: { ok: { type: "boolean" } } };
	const { details } = await a.tool("coms_send", { target: "bob", message: "status?", response_schema: schema });
	await until(() => b.queued.length === 1);
	assert.match(b.queued[0].message.content, /must be JSON matching this schema/);
	await assert.rejects(b.tool("coms_reply", { msg_id: details.msg_id, response: "yes" }), /valid JSON/);
	await b.tool("coms_reply", { msg_id: details.msg_id, response: '{"ok": true}' });
	const result = await a.tool("coms_await", { msg_id: details.msg_id });
	assert.deepEqual(result.details.response, { ok: true });
});

test("shutting down fails unanswered messages instead of leaving senders waiting", async () => {
	const { a, b } = await pair();
	const { details } = await a.tool("coms_send", { target: "bob", message: "hello" });
	await until(() => b.queued.length === 1);
	await b.shutdown();
	const result = await a.tool("coms_await", { msg_id: details.msg_id, timeout_ms: 2000 });
	assert.equal(result.details.error, "bob shut down before replying");
	await assert.rejects(a.tool("coms_send", { target: "bob", message: "again" }), /no live agent named "bob"/);
});

test("aborting coms_await hands the reply over to message delivery", async () => {
	const { a, b } = await pair();
	const { details } = await a.tool("coms_send", { target: "bob", message: "slow question" });
	const controller = new AbortController();
	const waiting = a.tool("coms_await", { msg_id: details.msg_id }, controller.signal);
	controller.abort();
	assert.match((await waiting).details.error, /aborted/);

	await until(() => b.queued.length === 1);
	await b.tool("coms_reply", { msg_id: details.msg_id, response: "eventually" });
	await until(() => a.queued.length === 1);
	assert.match(a.queued[0].message.content, /eventually$/);
});

test("an unanswered message times out on the sender's side", async () => {
	const { a, b } = await pair({ timeoutMs: 50 });
	await a.tool("coms_send", { target: "bob", message: "anyone?" });
	await until(() => a.queued.length === 1);
	assert.match(a.queued[0].message.details.error, /no reply from bob/);
	assert.equal(b.state.inbound.size, 1);
});

test("coms_list shows live peers but not the caller", async () => {
	const { a } = await pair();
	const result = await a.tool("coms_list", {});
	assert.deepEqual(result.details.agents.map((x: any) => [x.name, x.alive]), [["bob", true]]);
});

// ━━ Crashes and dead peers ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function registryFile(name: string) {
	return path.join(comsDir, "projects", "test", "agents", `${name}.json`);
}

function readEntry(name: string) {
	return JSON.parse(fs.readFileSync(registryFile(name), "utf-8"));
}

function writeEntry(fields: Record<string, unknown> & { name: string }) {
	const now = new Date().toISOString();
	const entry = {
		session_id: `S-${fields.name}`, project: "test", purpose: "", model: "m", color: "#ffffff", cwd: "/tmp",
		pid: process.pid, started_at: now, heartbeat_at: now, explicit: false, version: 2, ...fields,
	};
	fs.mkdirSync(path.dirname(registryFile(fields.name)), { recursive: true });
	fs.writeFileSync(registryFile(fields.name), JSON.stringify(entry));
	return entry;
}

/** Starts an agent in a child process and resolves once it has registered. */
async function spawnAgent(name: string): Promise<ChildProcess> {
	const child = spawn(process.execPath, [path.join(import.meta.dirname, "fixtures", "agent.ts"), comsDir, name], {
		stdio: ["ignore", "pipe", "inherit"],
	});
	await new Promise<void>((resolve, reject) => {
		child.stdout!.on("data", (d) => String(d).includes("ready") && resolve());
		child.once("exit", (code) => reject(new Error(`agent exited with ${code}`)));
	});
	return child;
}

async function kill9(child: ChildProcess) {
	const exited = new Promise((r) => child.once("exit", r));
	child.kill("SIGKILL");
	await exited;
}

/** A unix socket server; `answer` decides what it does with each connection. */
async function fakePeer(name: string, answer: (socket: net.Socket) => void) {
	const endpoint = path.join(comsDir, "sockets", `${name}.sock`);
	const sockets: net.Socket[] = [];
	const server = net.createServer((s) => { sockets.push(s); answer(s); });
	await new Promise<void>((r) => server.listen(endpoint, r));
	return {
		endpoint,
		close: () => { sockets.forEach((s) => s.destroy()); server.close(); },
	};
}

test("a crashed agent is removed and its senders stop waiting", async () => {
	const a = await agent("alice", { pingIntervalMs: 50 });
	agents.push(a);
	const child = await spawnAgent("crashy");
	const { endpoint } = readEntry("crashy");
	const sent = await a.tool("coms_send", { target: "crashy", message: "are you there?" });
	await until(() => a.state.poolRows().some((r: any) => r.name === "crashy" && !r.pending && !r.stale));

	await kill9(child);
	assert.ok(fs.existsSync(endpoint), "the crash left its socket file behind");

	await until(() => a.queued.length === 1);
	assert.equal(a.queued[0].message.details.msg_id, sent.details.msg_id);
	assert.equal(a.queued[0].message.details.error, "crashy is no longer running");
	// Gone from the widget in the same ping cycle, not after several missed pings.
	assert.equal(a.state.poolRows().some((r: any) => r.name === "crashy"), false);
	assert.equal(fs.existsSync(registryFile("crashy")), false);
	assert.equal(fs.existsSync(endpoint), false);
});

test("an entry whose socket refuses connections is removed even if its PID is alive", async () => {
	const a = await agent("alice", { pingIntervalMs: 50 });
	agents.push(a);
	const child = await spawnAgent("crashy");
	await kill9(child);
	// The OS handed the dead agent's PID to another process.
	const entry = writeEntry({ ...readEntry("crashy"), pid: process.pid, name: "crashy" });

	await until(() => !fs.existsSync(registryFile("crashy")));
	assert.equal(fs.existsSync(entry.endpoint), false);
});

test("a hung agent is removed only once its heartbeat is stale", async () => {
	const a = await agent("alice", { pingIntervalMs: 50, pingTimeoutMs: 50 });
	agents.push(a);
	const hung = await fakePeer("hung", () => { /* accepts, never answers */ });
	try {
		writeEntry({ name: "hung", endpoint: hung.endpoint });
		await new Promise((r) => setTimeout(r, 300));
		assert.ok(fs.existsSync(registryFile("hung")), "a slow answer alone doesn't make an agent dead");

		writeEntry({ name: "hung", endpoint: hung.endpoint, heartbeat_at: new Date(Date.now() - 10 * 60_000).toISOString() });
		await until(() => !fs.existsSync(registryFile("hung")));
	} finally {
		hung.close();
	}
});

test("a stale heartbeat alone doesn't remove an agent that still answers", async () => {
	// After a laptop sleeps, every heartbeat is stale but every agent answers.
	const a = await agent("alice", { pingIntervalMs: 50, pingTimeoutMs: 200 });
	agents.push(a);
	const sleepy = await fakePeer("sleepy", async (socket) => {
		const line = JSON.parse(await new Promise<string>((r) => socket.once("data", (d) => r(String(d)))));
		socket.end(JSON.stringify({ type: "pong", msg_id: line.msg_id, agent_card: { name: "sleepy" } }) + "\n");
	});
	try {
		writeEntry({ name: "sleepy", endpoint: sleepy.endpoint, heartbeat_at: new Date(Date.now() - 10 * 60_000).toISOString() });
		await new Promise((r) => setTimeout(r, 300));
		assert.ok(fs.existsSync(registryFile("sleepy")));
	} finally {
		sleepy.close();
	}
});

test("sending to a dead endpoint fails fast and removes the entry", async () => {
	const a = await agent("alice");
	agents.push(a);
	writeEntry({ name: "ghost", endpoint: path.join(comsDir, "sockets", "ghost.sock") });
	await assert.rejects(a.tool("coms_send", { target: "ghost", message: "hi" }), /ghost is no longer running/);
	assert.equal(fs.existsSync(registryFile("ghost")), false);
});

test("removing a dead entry leaves a newer agent with the same name alone", () => {
	const registry = createRegistry(comsDir);
	const sockets = path.join(comsDir, "sockets");
	fs.mkdirSync(sockets, { recursive: true });
	const old = writeEntry({ name: "bob", session_id: "OLD", endpoint: path.join(sockets, "OLD.sock") });
	writeEntry({ name: "bob", session_id: "NEW", endpoint: path.join(sockets, "NEW.sock") });
	fs.writeFileSync(old.endpoint, "");
	registry.remove({ ...old, project: "test" });
	assert.equal(readEntry("bob").session_id, "NEW");
	assert.equal(fs.existsSync(old.endpoint), false);
});

// ━━ Opting in and out ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function agentFiles(project = "test") {
	try { return fs.readdirSync(path.join(comsDir, "projects", project, "agents")); } catch { return []; }
}

test("a session without --coms or --coms-name stays out of coms", async () => {
	const d = await agent(null);
	agents.push(d);
	assert.equal(d.state.identity, null);
	assert.deepEqual(agentFiles(), []);
	assert.deepEqual(d.activeComsTools(), []);
	await assert.rejects(d.tool("coms_list", {}), /coms is off in this session/);
	await d.command("");
	assert.match(d.notices.at(-1)!.text, /coms is off.*\/coms join/);
});

test("--coms joins at startup with a generated name", async () => {
	const c = await agent(null, {}, { coms: true });
	agents.push(c);
	assert.match(c.state.identity!.name, /^agent-[0-9a-z]{6}$/);
	assert.deepEqual(c.activeComsTools(), ["coms_await", "coms_list", "coms_reply", "coms_send"]);
});

test("/coms join brings a running session into coms", async () => {
	const a = await agent("alice");
	const d = await agent(null);
	agents.push(a, d);
	await d.command('join carol --project test --purpose "Reviews code"');
	assert.equal(d.notices.at(-1)!.text, "📡 joined coms as carol@test");
	assert.deepEqual(d.activeComsTools(), ["coms_await", "coms_list", "coms_reply", "coms_send"]);
	assert.equal(readEntry("carol").purpose, "Reviews code");

	const list = await a.tool("coms_list", {});
	assert.deepEqual(list.details.agents.map((x: any) => [x.name, x.purpose]), [["carol", "Reviews code"]]);
	const { details } = await a.tool("coms_send", { target: "carol", message: "hi" });
	await until(() => d.queued.length === 1);
	await d.tool("coms_reply", { msg_id: details.msg_id, response: "hello" });
	assert.equal((await a.tool("coms_await", { msg_id: details.msg_id })).content[0].text, "hello");
});

test("/coms leave fails unanswered messages and switches the tools off", async () => {
	const { a, b } = await pair();
	const { endpoint } = readEntry("bob");
	const toBob = await a.tool("coms_send", { target: "bob", message: "hello" });
	await until(() => b.queued.length === 1);
	await b.tool("coms_send", { target: "alice", message: "a question bob won't wait for" });
	await until(() => a.queued.length === 1);
	b.queued.length = 0;

	await b.command("leave");
	assert.equal(b.notices.at(-1)!.text, "📡 bob left coms");
	assert.equal((await a.tool("coms_await", { msg_id: toBob.details.msg_id, timeout_ms: 2000 })).details.error, "bob left coms before replying");
	assert.equal(fs.existsSync(registryFile("bob")), false);
	assert.equal(fs.existsSync(endpoint), false);
	assert.deepEqual(b.activeComsTools(), []);
	// Bob's own unanswered question is dropped rather than delivered.
	assert.equal(b.queued.length, 0);
});

test("joining again under a new name replaces the old registration", async () => {
	const { a, b } = await pair();
	const toBob = await a.tool("coms_send", { target: "bob", message: "hello" });
	await until(() => b.queued.length === 1);
	await b.command("join robert");
	assert.equal(b.state.identity!.name, "robert");
	assert.deepEqual(agentFiles().sort(), ["alice.json", "robert.json"]);
	assert.equal((await a.tool("coms_await", { msg_id: toBob.details.msg_id, timeout_ms: 2000 })).details.error, "bob left coms before replying");
	await a.tool("coms_send", { target: "robert", message: "are you bob?" });
});

test("names and projects that aren't safe file names are rejected", async () => {
	const d = await agent(null);
	agents.push(d);
	await d.command("join ../escape");
	assert.match(d.notices.at(-1)!.text, /name "\.\.\/escape" may only use/);
	await d.command("join ok --project a/b");
	assert.match(d.notices.at(-1)!.text, /project "a\/b" may only use/);
	await d.command('join ok extra words');
	assert.match(d.notices.at(-1)!.text, /quote a purpose/);
	assert.equal(d.state.identity, null);
});

// ━━ Projects ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test("agents in different projects can't see or message each other", async () => {
	const a = await agent("alice");
	const other = await agent("bob", {}, { "coms-project": "other" });
	const sameName = await agent("bob");
	agents.push(a, other, sameName);
	const otherId = other.state.identity!.session_id;

	const list = await a.tool("coms_list", {});
	assert.deepEqual(list.details.agents.map((x: any) => x.session_id), [sameName.state.identity!.session_id]);
	// "bob" resolves to the bob in alice's own project, never the other one.
	await a.tool("coms_send", { target: "bob", message: "hi" });
	await until(() => sameName.queued.length === 1);
	assert.equal(other.queued.length, 0);
	await assert.rejects(a.tool("coms_send", { target: otherId, message: "hi" }), /no live agent named .* in project test/);
});

test("/coms completes subcommands but not ones already typed in full", async () => {
	const d = await agent(null);
	agents.push(d);
	assert.deepEqual(d.complete(""), ["join", "leave"]);
	assert.deepEqual(d.complete("le"), ["leave"]);
	assert.equal(d.complete("leave"), null);
	assert.equal(d.complete("join planner"), null);
});

// ━━ Review fixes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test("/reload keeps a session joined, /new starts out, and a later /resume rejoins", async () => {
	const d = await agent(null, {}, {}, { file: "/sessions/one.jsonl" });
	await d.command('join carol --project shop --purpose "Reviews code"');
	const reloaded = await d.replace("reload");
	const self = reloaded.state.identity!;
	assert.deepEqual([self.name, self.project, self.purpose], ["carol", "shop", "Reviews code"]);

	const fresh = await reloaded.replace("new", "/sessions/two.jsonl");
	assert.equal(fresh.state.identity, null);
	assert.deepEqual(agentFiles("shop"), []);
	assert.deepEqual(fresh.activeComsTools(), []);

	const resumed = await fresh.replace("resume", "/sessions/one.jsonl");
	agents.push(resumed);
	assert.equal(resumed.state.identity?.name, "carol");
	assert.equal(resumed.state.identity?.project, "shop");
});

test("startup flags don't rejoin after /new", async () => {
	const b = await agent("bob", {}, {}, { file: "/sessions/bob.jsonl" });
	const fresh = await b.replace("new", "/sessions/bob-2.jsonl");
	agents.push(fresh);
	assert.equal(fresh.state.identity, null);
	assert.deepEqual(agentFiles(), []);
});

test("a fork starts out of coms even though it copies the conversation", async () => {
	const b = await agent("bob", {}, {}, { file: "/sessions/bob.jsonl" });
	const forked = await b.replace("fork", "/sessions/bob-fork.jsonl");
	agents.push(forked);
	assert.equal(forked.state.identity, null);
});

test("pi --fork at startup also starts out of coms", async () => {
	const d = await agent(null, {}, {}, { file: "/sessions/orig.jsonl" });
	await d.command("join carol --project shop");
	await d.shutdown();
	sessions.set("/sessions/forked.jsonl", [...sessions.get("/sessions/orig.jsonl")!]);
	const forked = await agent(null, {}, {}, { file: "/sessions/forked.jsonl" });
	agents.push(forked);
	assert.equal(forked.state.identity, null);
});

test("/coms leave sticks when the conversation is resumed", async () => {
	const b = await agent("bob", {}, {}, { file: "/sessions/bob.jsonl" });
	await b.command("leave");
	const resumed = await b.replace("resume");
	agents.push(resumed);
	assert.equal(resumed.state.identity, null);
});

test("pi -c rejoins a conversation that was joined when pi quit", async () => {
	const d = await agent(null, {}, {}, { file: "/sessions/c.jsonl" });
	await d.command("join carol --project shop");
	await d.shutdown();
	// pi -c: a fresh process, startup reason, same session file, no coms flags.
	const continued = await agent(null, {}, {}, { file: "/sessions/c.jsonl" });
	agents.push(continued);
	assert.equal(continued.state.identity?.name, "carol");
	assert.equal(continued.state.identity?.project, "shop");
});

test("coms flags given with pi -c win over the conversation's record", async () => {
	const d = await agent(null, {}, {}, { file: "/sessions/c2.jsonl" });
	await d.command("join carol --project shop");
	await d.shutdown();
	const continued = await agent("dave", {}, {}, { file: "/sessions/c2.jsonl" });
	agents.push(continued);
	assert.equal(continued.state.identity?.name, "dave");
	assert.equal(continued.state.identity?.project, "test");
});

test("renaming with /coms join keeps the current project and purpose", async () => {
	const d = await agent(null);
	agents.push(d);
	await d.command('join carol --project shop --purpose "Reviews code"');
	await d.command("join caroline");
	const self = d.state.identity!;
	assert.deepEqual([self.name, self.project, self.purpose], ["caroline", "shop", "Reviews code"]);
});

test("a message arriving while a session leaves is refused, not stranded", async () => {
	const { a, b } = await pair();
	// A sender that never answers, so failing its message keeps leave() waiting.
	const hung = await fakePeer("hung", () => {});
	try {
		await sendRaw(readEntry("bob").endpoint, { type: "prompt", sender_endpoint: hung.endpoint, sender_name: "hung" });
		await until(() => b.state.inbound.size === 1);
		const leaving = b.command("leave");
		// Bob is already gone for everyone else while leave() waits on the hung sender.
		assert.equal(fs.existsSync(registryFile("bob")), false);
		await assert.rejects(a.tool("coms_send", { target: "bob", message: "hi" }), /no live agent named "bob"/);
		hung.close();
		await leaving;
		assert.equal(b.state.inbound.size, 0);
		assert.equal(b.notices.some((n) => n.level === "error"), false);
	} finally {
		hung.close();
	}
});

test("quitting while /coms leave is still finishing doesn't crash", async () => {
	const { b } = await pair();
	const hung = await fakePeer("hung", () => {});
	try {
		await sendRaw(readEntry("bob").endpoint, { type: "prompt", sender_endpoint: hung.endpoint, sender_name: "hung" });
		await until(() => b.state.inbound.size === 1);
		const leaving = b.command("leave");
		await b.shutdown();
		hung.close();
		await leaving;
		assert.deepEqual(b.notices.filter((n) => n.level === "error"), []);
	} finally {
		hung.close();
	}
});

test("rejoining another project mid-ping doesn't carry over the old project's peers", async () => {
	const a = await agent("alice");
	const b = await agent("bob", { pingTimeoutMs: 300 });
	agents.push(a, b);
	const hung = await fakePeer("hung", () => {});
	try {
		writeEntry({ name: "hung", endpoint: hung.endpoint });
		const status = b.command(""); // pings alice and the hung peer
		await new Promise((r) => setTimeout(r, 50));
		await b.command("join bob --project other");
		await status;
		assert.deepEqual(b.state.poolRows().map((r: any) => r.name), []);
	} finally {
		hung.close();
	}
});

test("shutting down during a join cancels it instead of leaving a ghost agent", async () => {
	const { b } = await pair();
	const hung = await fakePeer("hung", () => {});
	try {
		await sendRaw(readEntry("bob").endpoint, { type: "prompt", sender_endpoint: hung.endpoint, sender_name: "hung" });
		await until(() => b.state.inbound.size === 1);
		const rejoining = b.command("join robert"); // waits in leave() on the hung sender
		await b.shutdown();
		hung.close();
		await rejoining;
		await new Promise((r) => setTimeout(r, 50));
		assert.deepEqual(agentFiles().sort(), ["alice.json"]);
		assert.equal(b.state.identity, null);
		assert.deepEqual(b.notices.filter((n) => n.level === "error"), []);
	} finally {
		hung.close();
	}
});

test("a borrowed frontmatter name is tidied rather than rejected", async () => {
	const dir = fs.mkdtempSync("/tmp/coms-fm-");
	const file = path.join(dir, "reviewer.md");
	fs.writeFileSync(file, "---\nname: Code Reviewer\ndescription: Reviews code\n---\nYou review code.\n");
	process.argv.push("--system-prompt", file);
	try {
		const c = await agent(null, {}, { coms: true });
		agents.push(c);
		assert.equal(c.state.identity?.name, "Code-Reviewer");
		assert.equal(c.state.identity?.purpose, "Reviews code");
	} finally {
		process.argv.splice(-2, 2);
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

/** Sends a hand-built envelope, filling in the fields every envelope needs. */
function sendRaw(endpoint: string, fields: Record<string, unknown>) {
	return new Promise<void>((resolve, reject) => {
		const sock = net.createConnection({ path: endpoint });
		sock.once("error", reject);
		sock.once("connect", () => {
			sock.write(JSON.stringify({
				msg_id: `raw-${Math.random().toString(36).slice(2)}`, sender_session: "raw", hops: 0,
				timestamp: new Date().toISOString(), prompt: "hello", sender_cwd: "/tmp", ...fields,
			}) + "\n");
			sock.once("data", () => { sock.end(); resolve(); });
		});
	});
}

// ━━ Second review ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test("a peer that joins while pings are in flight isn't reported as gone", async () => {
	const a = await agent("alice", { pingTimeoutMs: 300 });
	agents.push(a);
	const hung = await fakePeer("hung", () => {});
	try {
		writeEntry({ name: "hung", endpoint: hung.endpoint });
		const status = a.command(""); // refreshes, waiting on the hung peer
		await new Promise((r) => setTimeout(r, 50));
		const carol = await agent("carol");
		agents.push(carol);
		await a.tool("coms_send", { target: "carol", message: "hi" });
		await status;
		assert.deepEqual(a.queued.map((q) => q.message.details?.error), []);
		assert.equal([...a.state.outbound.values()][0].result, undefined);
	} finally {
		hung.close();
	}
});

test("/coms join without a name keeps the current name", async () => {
	const d = await agent(null);
	agents.push(d);
	await d.command("join carol --project shop");
	await d.command('join --purpose "Reviews code"');
	const self = d.state.identity!;
	assert.deepEqual([self.name, self.project, self.purpose], ["carol", "shop", "Reviews code"]);
});

test("membership survives /tree moving the branch away from the join", async () => {
	let branch: any[] = [];
	const d = await agent(null, {}, {}, { file: "/sessions/tree.jsonl", branch: () => branch });
	await d.command("join carol --project shop");
	branch = []; // /tree back to before the join
	const reloaded = await d.replace("reload");
	agents.push(reloaded);
	assert.equal(reloaded.state.identity?.name, "carol");
});

test("a clash is resolved with a suffix, but the name asked for is what's recorded", async () => {
	const c = await agent("carol");
	const d = await agent(null, {}, {}, { file: "/sessions/d.jsonl" });
	agents.push(c);
	await d.command("join carol --project test");
	assert.equal(d.state.identity!.name, "carol2");
	await c.shutdown();
	const resumed = await d.replace("resume");
	agents.push(resumed);
	assert.equal(resumed.state.identity!.name, "carol");
});

test("suffixed names stay within 32 characters", () => {
	const registry = createRegistry(comsDir);
	const long = "a".repeat(32);
	writeEntry({ name: long });
	const name = registry.uniqueName("test", long);
	assert.equal(name, "a".repeat(31) + "2");
});

test("/coms leave cancels a join that is still in progress", async () => {
	const b = await agent("bob", {}, {}, { file: "/sessions/bob.jsonl" });
	agents.push(b);
	const hung = await fakePeer("hung", () => {});
	try {
		await sendRaw(readEntry("bob").endpoint, { type: "prompt", sender_endpoint: hung.endpoint, sender_name: "hung" });
		await until(() => b.state.inbound.size === 1);
		const renaming = b.command("join robert"); // waits in leave() on the hung sender
		await b.command("leave");
		hung.close();
		await renaming;
		await new Promise((r) => setTimeout(r, 50));
		assert.equal(b.state.identity, null);
		assert.deepEqual(agentFiles(), []);
		assert.deepEqual(b.notices.map((n) => n.text), ["📡 left coms"]);
		const resumed = await b.replace("reload");
		agents.push(resumed);
		assert.equal(resumed.state.identity, null);
	} finally {
		hung.close();
	}
});

test("a join cut short by /new fails quietly on the replaced session", async () => {
	const b = await agent("bob", {}, {}, { file: "/sessions/bob.jsonl" });
	const hung = await fakePeer("hung", () => {});
	try {
		await sendRaw(readEntry("bob").endpoint, { type: "prompt", sender_endpoint: hung.endpoint, sender_name: "hung" });
		await until(() => b.state.inbound.size === 1);
		const renaming = b.command("join robert");
		const fresh = await b.replace("new", "/sessions/new.jsonl");
		agents.push(fresh);
		hung.close();
		await renaming; // must not reject on the stale ctx
		assert.deepEqual(b.notices, []);
		assert.deepEqual(agentFiles(), []);
	} finally {
		hung.close();
	}
});

test("/coms leave that finishes after /new doesn't throw on the replaced session", async () => {
	const b = await agent("bob", {}, {}, { file: "/sessions/bob.jsonl" });
	const hung = await fakePeer("hung", () => {});
	try {
		await sendRaw(readEntry("bob").endpoint, { type: "prompt", sender_endpoint: hung.endpoint, sender_name: "hung" });
		await until(() => b.state.inbound.size === 1);
		const leaving = b.command("leave"); // waits on the hung sender
		const fresh = await b.replace("new", "/sessions/new.jsonl");
		agents.push(fresh);
		hung.close();
		await leaving;
		assert.deepEqual(b.notices, []);
	} finally {
		hung.close();
	}
});
