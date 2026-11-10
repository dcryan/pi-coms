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

beforeEach(() => {
	// Short base path: unix socket paths are limited to ~104 bytes on macOS.
	comsDir = fs.mkdtempSync("/tmp/coms-");
});

afterEach(async () => {
	for (const a of agents) await a.shutdown();
	agents = [];
	fs.rmSync(comsDir, { recursive: true, force: true });
});

type Agent = Awaited<ReturnType<typeof agent>>;

async function agent(name: string, opts: { maxHops?: number; timeoutMs?: number } = {}) {
	const handlers = new Map<string, Function[]>();
	const tools = new Map<string, any>();
	const flags: Record<string, unknown> = { "coms-name": name, "coms-project": "test" };
	// Messages pi has queued for the model but not yet emitted.
	const queued: any[] = [];
	const transcript: any[] = [];
	const pi = {
		on: (event: string, fn: Function) => handlers.set(event, [...(handlers.get(event) ?? []), fn]),
		registerTool: (def: any) => tools.set(def.name, def),
		registerFlag: () => {},
		getFlag: (n: string) => flags[n],
		registerCommand: () => {},
		appendEntry: () => {},
		sendMessage: (message: any, options: any) => queued.push({ message, options }),
	};
	const ctx = {
		hasUI: false,
		cwd: "/tmp",
		model: { id: "test-model" },
		getContextUsage: () => ({ percent: 10 }),
		ui: { notify() {}, setStatus() {}, setWidget() {} },
	};
	const state = installComs(pi, { comsDir, pingIntervalMs: 60_000, ...opts });
	const emit = async (event: string, payload: any = {}) => {
		let result;
		for (const fn of handlers.get(event) ?? []) result = (await fn(payload, ctx)) ?? result;
		return result;
	};
	await emit("session_start", { reason: "startup" });

	return {
		name,
		state,
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
	const result = await a.tool("coms_await", { msg_id: details.msg_id });
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
