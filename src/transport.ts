// Unix socket transport: one JSON envelope per connection, newline-terminated,
// answered by exactly one JSON line (ack, nack or pong).

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as net from "node:net";
import * as path from "node:path";

// Messages and replies can carry whole files or diffs, so the cap only guards
// against runaway input. Measured in bytes, not characters.
export const LINE_CAP_BYTES = 8 * 1024 * 1024;

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

export function ulid(): string {
	let time = Date.now();
	let timeStr = "";
	for (let i = 9; i >= 0; i--) {
		timeStr = CROCKFORD[time % 32] + timeStr;
		time = Math.floor(time / 32);
	}
	let randStr = "";
	let bits = 0;
	let value = 0;
	for (const byte of crypto.randomBytes(10)) {
		value = ((value << 8) | byte) & 0xffff;
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			randStr += CROCKFORD[(value >> bits) & 31];
		}
	}
	return (timeStr + randStr).slice(0, 26);
}

export function nowIso(): string {
	return new Date().toISOString();
}

export function makeEndpoint(comsDir: string, sessionId: string): string {
	if (process.platform === "win32") return `\\\\.\\pipe\\pi-coms-${sessionId}`;
	return path.join(comsDir, "sockets", `${sessionId}.sock`);
}

/** Resolves with the first line received on the socket, without the newline. */
export function readOneLine(socket: net.Socket, capBytes = LINE_CAP_BYTES): Promise<string> {
	return new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		let size = 0;
		let settled = false;
		const finish = (err: Error | null, line?: string) => {
			if (settled) return;
			settled = true;
			socket.removeListener("data", onData);
			if (err) reject(err);
			else resolve(line!);
		};
		const onData = (chunk: Buffer) => {
			const nl = chunk.indexOf(0x0a);
			if (nl >= 0) {
				chunks.push(chunk.subarray(0, nl));
				size += nl;
			} else {
				chunks.push(chunk);
				size += chunk.length;
			}
			if (size > capBytes) return finish(new Error("line too large"));
			if (nl >= 0) finish(null, Buffer.concat(chunks).toString("utf-8"));
		};
		socket.on("data", onData);
		socket.once("error", (err) => finish(err));
		socket.once("close", () => finish(new Error("connection closed before line received")));
	});
}

/** Writes one envelope and resolves with the peer's answer; rejects on nack. */
export function sendEnvelope(endpoint: string, envelope: object, timeoutMs = 10_000): Promise<any> {
	return new Promise((resolve, reject) => {
		const sock = net.createConnection({ path: endpoint });
		let settled = false;
		const timer = setTimeout(() => fail(Object.assign(new Error("peer did not answer"), { code: "ETIMEDOUT" })), timeoutMs);
		const fail = (err: Error) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			sock.destroy();
			reject(err);
		};
		sock.once("error", fail);
		sock.once("connect", async () => {
			try {
				sock.write(JSON.stringify(envelope) + "\n");
				const parsed = JSON.parse(await readOneLine(sock));
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				sock.end();
				if (parsed && parsed.type === "nack") reject(new Error(parsed.error || "nack"));
				else resolve(parsed);
			} catch (err) {
				fail(err instanceof Error ? err : new Error(String(err)));
			}
		});
	});
}

/** Nothing is listening on the endpoint: the agent behind it is gone. */
export function isDeadEndpointError(err: any): boolean {
	return err?.code === "ECONNREFUSED" || err?.code === "ENOENT" || err?.code === "ENOTSOCK";
}

function probeStaleSocket(endpoint: string): Promise<"in_use" | "stale"> {
	return new Promise((resolve) => {
		const sock = net.createConnection({ path: endpoint });
		const finish = (verdict: "in_use" | "stale") => {
			clearTimeout(timer);
			sock.destroy();
			resolve(verdict);
		};
		const timer = setTimeout(() => finish("stale"), 250);
		sock.once("connect", () => finish("in_use"));
		sock.once("error", () => finish("stale"));
	});
}

export async function bindEndpoint(endpoint: string, connHandler: (socket: net.Socket) => void): Promise<net.Server> {
	if (process.platform !== "win32" && fs.existsSync(endpoint)) {
		if ((await probeStaleSocket(endpoint)) === "in_use") {
			throw new Error(`endpoint already in use (${endpoint})`);
		}
		try { fs.unlinkSync(endpoint); } catch { /* best-effort */ }
	}
	return await new Promise<net.Server>((resolve, reject) => {
		const server = net.createServer(connHandler);
		server.once("error", reject);
		server.listen(endpoint, () => {
			server.removeListener("error", reject);
			resolve(server);
		});
	});
}
