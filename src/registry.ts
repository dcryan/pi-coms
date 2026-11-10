// Peer discovery: one JSON file per live agent under
// <comsDir>/projects/<project>/agents/<name>.json.

import * as fs from "node:fs";
import * as path from "node:path";

export interface RegistryEntry {
	session_id: string;
	project: string;
	name: string;
	purpose: string;
	model: string;
	color: string;
	pid: number;
	endpoint: string;
	cwd: string;
	started_at: string;
	explicit: boolean;
	version: number;
	context_used_pct?: number;
	queue_depth?: number;
	heartbeat_at?: string;
}

export function createRegistry(comsDir: string) {
	const projectsRoot = path.join(comsDir, "projects");
	const socketsDir = path.join(comsDir, "sockets");
	const agentsDir = (project: string) => path.join(projectsRoot, project, "agents");
	const entryPath = (project: string, name: string) => path.join(agentsDir(project), `${name}.json`);

	function write(entry: RegistryEntry, project: string): string {
		fs.mkdirSync(agentsDir(project), { recursive: true });
		const final = entryPath(project, entry.name);
		const tmp = `${final}.${process.pid}.tmp`;
		fs.writeFileSync(tmp, JSON.stringify(entry, null, 2));
		fs.renameSync(tmp, final);
		return final;
	}

	/**
	 * Deletes an agent's registry file and socket. The file is only deleted if
	 * it still belongs to the same session, so a newer agent that took the name
	 * keeps its entry.
	 */
	function remove(entry: Pick<RegistryEntry, "session_id" | "project" | "name" | "endpoint">): void {
		const file = entryPath(entry.project, entry.name);
		try {
			if (JSON.parse(fs.readFileSync(file, "utf-8")).session_id === entry.session_id) fs.unlinkSync(file);
		} catch { /* already gone or unreadable */ }
		if (process.platform !== "win32" && path.dirname(entry.endpoint) === socketsDir) {
			try { fs.unlinkSync(entry.endpoint); } catch { /* already gone */ }
		}
	}

	function read(project: string): RegistryEntry[] {
		let files: string[];
		try {
			files = fs.readdirSync(agentsDir(project));
		} catch {
			return [];
		}
		const out: RegistryEntry[] = [];
		for (const f of files) {
			if (!f.endsWith(".json")) continue;
			try {
				const parsed = JSON.parse(fs.readFileSync(path.join(agentsDir(project), f), "utf-8"));
				if (parsed && typeof parsed.session_id === "string") out.push({ ...parsed, project });
			} catch {
				// skip malformed or half-written entries
			}
		}
		return out;
	}

	function projects(): string[] {
		try {
			return fs.readdirSync(projectsRoot).filter((d) => {
				try { return fs.statSync(path.join(projectsRoot, d)).isDirectory(); } catch { return false; }
			});
		} catch {
			return [];
		}
	}

	/**
	 * Reads a project's entries and deletes those whose process has exited.
	 * Agents whose process is alive but not listening are found by pinging.
	 */
	function live(project: string): RegistryEntry[] {
		const out: RegistryEntry[] = [];
		for (const entry of read(project)) {
			try {
				process.kill(entry.pid, 0);
				out.push(entry);
			} catch (e: any) {
				// EPERM means the process exists but belongs to someone else.
				if (e?.code === "ESRCH") remove(entry);
				else out.push(entry);
			}
		}
		return out;
	}

	function liveAll(): RegistryEntry[] {
		return projects().flatMap((p) => live(p));
	}

	function uniqueName(project: string, desired: string): string {
		const taken = new Set(live(project).map((e) => e.name));
		if (!taken.has(desired)) return desired;
		let n = 2;
		while (taken.has(`${desired}${n}`)) n++;
		return `${desired}${n}`;
	}

	/** Name match in the caller's project first, then session id, then name, across projects. */
	function resolve(target: string, project: string | null): RegistryEntry | null {
		if (project) {
			const local = live(project).find((e) => e.name === target);
			if (local) return local;
		}
		const all = liveAll();
		return all.find((e) => e.session_id === target) ?? all.find((e) => e.name === target) ?? null;
	}

	return { write, remove, read, live, liveAll, projects, uniqueName, resolve };
}

export type Registry = ReturnType<typeof createRegistry>;
