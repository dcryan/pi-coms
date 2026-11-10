// End-to-end check with real pi processes in RPC mode and a real model.
//
//   node scripts/e2e.ts [provider/model]
//
// Starts alice and bob with this extension in a throwaway coms directory and
// runs three scenarios. Costs a few cheap model calls.

import { type ChildProcess, spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

const [provider, model] = (process.argv[2] ?? "claude-code/claude-haiku-5-5").split("/");
const repo = path.resolve(import.meta.dirname, "..");
const comsDir = fs.mkdtempSync("/tmp/coms-e2e-");
const workDir = fs.mkdtempSync("/tmp/coms-e2e-work-");

class Agent {
	events: any[] = [];
	private proc: ChildProcess;
	private buf = Buffer.alloc(0);
	private waiters: (() => void)[] = [];
	readonly name: string;

	/** Joins coms at startup unless `join` is false. */
	constructor(name: string, join = true) {
		this.name = name;
		this.proc = spawn("pi", [
			"--mode", "rpc", "--no-session", "-e", repo,
			// Keep the user's own MCP servers, skills and AGENTS.md out of the test.
			"--no-mcp", "--no-skills", "--no-context-files",
			"--provider", provider, "--model", model,
			...(join ? ["--coms-name", name, "--coms-project", "e2e"] : []),
		], { cwd: workDir, env: { ...process.env, PI_COMS_DIR: comsDir }, stdio: ["pipe", "pipe", "pipe"] });
		this.proc.stdout!.on("data", (chunk: Buffer) => {
			this.buf = Buffer.concat([this.buf, chunk]);
			let nl;
			while ((nl = this.buf.indexOf(0x0a)) >= 0) {
				const line = this.buf.subarray(0, nl).toString("utf-8").replace(/\r$/, "");
				this.buf = this.buf.subarray(nl + 1);
				try { this.events.push({ ...JSON.parse(line), receivedAt: Date.now() }); } catch { /* not a record */ }
				for (const w of this.waiters.splice(0)) w();
			}
		});
		this.proc.stderr!.on("data", (d) => process.env.E2E_DEBUG && process.stderr.write(`[${name}] ${d}`));
	}

	send(command: object) {
		this.proc.stdin!.write(JSON.stringify(command) + "\n");
	}

	prompt(message: string) {
		this.send({ type: "prompt", message });
	}

	async waitFor(pred: (events: any[]) => boolean, label: string, ms = 180_000) {
		const end = Date.now() + ms;
		while (!pred(this.events)) {
			if (Date.now() > end) throw new Error(`${this.name}: timed out waiting for ${label}`);
			await new Promise<void>((r) => {
				this.waiters.push(r);
				setTimeout(r, 500);
			});
		}
	}

	mark() {
		return this.events.length;
	}

	since(mark: number) {
		return this.events.slice(mark);
	}

	settledCount(mark = 0) {
		return this.since(mark).filter((e) => e.type === "agent_settled").length;
	}

	toolCalls(mark = 0) {
		return this.since(mark).filter((e) => e.type === "tool_execution_start").map((e) => e.toolName);
	}

	lastAssistantText(mark = 0) {
		const msgs = this.since(mark).filter((e) => e.type === "message_end" && e.message?.role === "assistant");
		const content = msgs.at(-1)?.message?.content ?? [];
		return (Array.isArray(content) ? content : [{ type: "text", text: content }])
			.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
	}

	customMessages(customType: string, mark = 0) {
		return this.since(mark).filter((e) => e.type === "message_end" && e.message?.role === "custom" && e.message.customType === customType);
	}

	errors() {
		return this.events.filter((e) => e.type === "extension_error");
	}

	stop() {
		this.proc.kill("SIGTERM");
	}

	crash() {
		this.proc.kill("SIGKILL");
	}
}

const results: { name: string; ok: boolean; detail: string }[] = [];
function check(name: string, ok: boolean, detail = "") {
	results.push({ name, ok, detail });
	console.log(`${ok ? "✔" : "✖"} ${name}${detail ? `  (${detail})` : ""}`);
}

const alice = new Agent("alice");
const bob = new Agent("bob");
const carol = new Agent("carol", false);
const registryFile = (name: string) => path.join(comsDir, "projects", "e2e", "agents", `${name}.json`);

async function started(a: Agent) {
	const id = `state-${a.name}`;
	a.send({ id, type: "get_state" });
	await a.waitFor((ev) => ev.some((e) => e.type === "response" && e.id === id), "startup", 60_000);
}

async function ready(a: Agent) {
	await started(a);
	// Registry entry written at session_start.
	await a.waitFor(() => fs.existsSync(registryFile(a.name)), "registry entry", 30_000);
}

try {
	await Promise.all([ready(alice), ready(bob), started(carol)]);
	check("0. a session without --coms-name stays out", !fs.existsSync(registryFile("carol")));
	console.log(`agents up · ${provider}/${model} · coms dir ${comsDir}\n`);

	// 1. Blocking round trip.
	{
		const a0 = alice.mark();
		const b0 = bob.mark();
		alice.prompt(
			"Use coms_send to ask bob: 'What is the capital of France? Answer in one word.' "
			+ "Then call coms_await with the msg_id and tell me exactly what bob answered.",
		);
		await alice.waitFor(() => alice.settledCount(a0) >= 1 && alice.toolCalls(a0).includes("coms_await"), "scenario 1");
		await bob.waitFor(() => bob.settledCount(b0) >= 1, "bob to finish scenario 1");
		check("1. alice used coms_send + coms_await", ["coms_send", "coms_await"].every((t) => alice.toolCalls(a0).includes(t)), alice.toolCalls(a0).join(","));
		check("1. bob answered with coms_reply", bob.toolCalls(b0).filter((t) => t === "coms_reply").length === 1, bob.toolCalls(b0).join(","));
		check("1. bob saw the message", bob.customMessages("coms-inbound", b0).length === 1);
		check("1. alice relayed Paris", /paris/i.test(alice.lastAssistantText(a0)), alice.lastAssistantText(a0).slice(0, 80));
		check("1. no reminder needed", bob.customMessages("coms-reminder", b0).length === 0);
	}

	// 2. Fire-and-forget: the reply comes back as a message and starts a turn.
	{
		const a0 = alice.mark();
		const b0 = bob.mark();
		alice.prompt(
			"Use coms_send to ask bob: 'What is 17 times 23? Answer with just the number.' "
			+ "Do NOT call coms_await. End your turn right after sending.",
		);
		// The reply may land after alice's run has ended (a new run) or just
		// before (the same run continues). Either way she must respond to it.
		const answeredReply = () => {
			const events = alice.since(a0);
			const at = events.findIndex((e) => e.type === "message_end" && e.message?.role === "custom" && e.message.customType === "coms-reply");
			if (at < 0) return false;
			const after = events.slice(at);
			return after.some((e) => e.type === "message_end" && e.message?.role === "assistant") && after.some((e) => e.type === "agent_settled");
		};
		await alice.waitFor(answeredReply, "scenario 2", 240_000);
		await bob.waitFor(() => bob.settledCount(b0) >= 1, "bob to finish scenario 2");
		const reply = alice.customMessages("coms-reply", a0)[0].message;
		check("2. alice did not block", !alice.toolCalls(a0).includes("coms_await"), alice.toolCalls(a0).join(","));
		check("2. reply delivered as a message", /391/.test(String(reply.content)), String(reply.content).split("\n").at(-1));
		check("2. alice responded to the reply", /391/.test(alice.lastAssistantText(a0)), alice.lastAssistantText(a0).slice(0, 80));
	}

	// 3. The receiver is busy with its own user's work when the message lands.
	//    Its own answer must not be sent; the coms reply must be.
	{
		const a0 = alice.mark();
		const b0 = bob.mark();
		bob.prompt("Run `sleep 15 && echo BOB-LOCAL-DONE` with bash, then reply to me with exactly: local work finished.");
		await bob.waitFor(() => bob.toolCalls(b0).includes("bash"), "bob to start local work");
		alice.prompt(
			"Use coms_send to ask bob: 'Name one primary colour. One word.' "
			+ "Then call coms_await and tell me what bob answered.",
		);
		await alice.waitFor(() => alice.settledCount(a0) >= 1 && alice.toolCalls(a0).includes("coms_await"), "scenario 3");
		await bob.waitFor(() => bob.settledCount(b0) >= 1, "bob to settle");
		const awaitEnd = alice.since(a0).find((e) => e.type === "tool_execution_end" && e.toolName === "coms_await");
		const answer = awaitEnd?.result?.content?.[0]?.text ?? "";
		const sentAt = alice.since(a0).find((e) => e.type === "tool_execution_end" && e.toolName === "coms_send")?.receivedAt;
		const bashDoneAt = bob.since(b0).find((e) => e.type === "tool_execution_end" && e.toolName === "bash")?.receivedAt;
		check("3. message arrived while bob was busy", sentAt != null && bashDoneAt != null && sentAt < bashDoneAt,
			`sent ${sentAt && bashDoneAt ? ((bashDoneAt - sentAt) / 1000).toFixed(1) + "s before bob's bash finished" : "?"}`);
		check("3. alice got bob's coms reply, not his local answer", /red|blue|yellow|green/i.test(answer) && !/local work/i.test(answer), answer.slice(0, 80));
		check("3. bob still answered his own user", bob.since(b0).some((e) => e.type === "message_end" && e.message?.role === "assistant"
			&& JSON.stringify(e.message.content).toLowerCase().includes("local work finished")));
	}

	// 4. A running session joins with /coms join and can be messaged.
	{
		const a0 = alice.mark();
		const c0 = carol.mark();
		carol.prompt('/coms join carol --project e2e --purpose "Answers trivia"');
		await carol.waitFor(() => fs.existsSync(registryFile("carol")), "carol to join", 30_000);
		const entry = JSON.parse(fs.readFileSync(registryFile("carol"), "utf-8"));
		check("4. /coms join registered carol", entry.purpose === "Answers trivia", entry.purpose);
		alice.prompt(
			"Use coms_send to ask carol: 'How many legs does a spider have? Answer with just the number.' "
			+ "Then call coms_await and tell me exactly what carol answered.",
		);
		await alice.waitFor(() => alice.settledCount(a0) >= 1 && alice.toolCalls(a0).includes("coms_await"), "scenario 4");
		await carol.waitFor(() => carol.settledCount(c0) >= 1, "carol to answer");
		check("4. carol answered with coms_reply", carol.toolCalls(c0).includes("coms_reply"), carol.toolCalls(c0).join(","));
		check("4. alice relayed 8", /\b8\b|eight/i.test(alice.lastAssistantText(a0)), alice.lastAssistantText(a0).slice(0, 80));
	}

	// 5. Bob crashes while alice waits on him: she hears about it within a ping
	//    cycle instead of after the 30-minute timeout.
	{
		const a0 = alice.mark();
		const b0 = bob.mark();
		const { endpoint } = JSON.parse(fs.readFileSync(path.join(comsDir, "projects", "e2e", "agents", "bob.json"), "utf-8"));
		alice.prompt(
			"Use coms_send to ask bob: 'Run `sleep 120` with bash, then reply done.' "
			+ "Do NOT call coms_await. End your turn right after sending.",
		);
		await bob.waitFor(() => bob.toolCalls(b0).includes("bash"), "bob to start sleeping");
		const crashedAt = Date.now();
		bob.crash();
		await alice.waitFor(() => alice.customMessages("coms-reply", a0).length >= 1, "crash notice", 60_000);
		const notice = alice.customMessages("coms-reply", a0)[0];
		check("5. alice told bob is gone", notice.message.details?.error === "bob is no longer running",
			`${((notice.receivedAt - crashedAt) / 1000).toFixed(1)}s after the crash`);
		check("5. bob's registry entry and socket removed",
			!fs.existsSync(path.join(comsDir, "projects", "e2e", "agents", "bob.json")) && !fs.existsSync(endpoint));
	}

	const errors = [...alice.errors(), ...bob.errors(), ...carol.errors()];
	check("no extension errors", errors.length === 0, JSON.stringify(errors).slice(0, 200));
} catch (err: any) {
	check("run completed", false, err.message);
	for (const a of [alice, bob, carol]) {
		console.log(`  ${a.name}: tools [${a.toolCalls().join(", ")}] · last reply: ${a.lastAssistantText().slice(0, 200)}`);
		const failed = a.events.filter((e) => e.type === "message_end" && e.message?.stopReason === "error").at(-1);
		if (failed) console.log(`  ${a.name}: model error: ${String(failed.message.errorMessage).slice(0, 300)}`);
		if (process.env.E2E_DUMP) fs.writeFileSync(`${process.env.E2E_DUMP}-${a.name}.jsonl`, a.events.map((e) => JSON.stringify(e)).join("\n"));
	}
} finally {
	alice.stop();
	bob.stop();
	carol.stop();
	await new Promise((r) => setTimeout(r, 1000));
	const leftover = fs.readdirSync(path.join(comsDir, "projects", "e2e", "agents")).filter((f) => f.endsWith(".json"));
	check("registry cleaned up on exit", leftover.length === 0, leftover.join(","));
	fs.rmSync(workDir, { recursive: true, force: true });
	if (results.every((r) => r.ok)) fs.rmSync(comsDir, { recursive: true, force: true });
	const failed = results.filter((r) => !r.ok).length;
	console.log(`\n${results.length - failed}/${results.length} checks passed`);
	process.exit(failed ? 1 : 0);
}
