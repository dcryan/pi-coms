// A coms agent in its own process, so tests can kill -9 it.
//   node tests/fixtures/agent.ts <comsDir> <name>
import { installComs } from "../../src/coms.ts";

const [comsDir, name] = process.argv.slice(2);
const handlers = new Map<string, Function>();
const flags: Record<string, unknown> = { "coms-name": name, "coms-project": "test" };
const pi = {
	on: (event: string, fn: Function) => handlers.set(event, fn),
	registerTool() {},
	registerFlag() {},
	getFlag: (n: string) => flags[n],
	registerCommand() {},
	appendEntry() {},
	sendMessage() {},
};
installComs(pi, { comsDir, pingIntervalMs: 60_000 });
await handlers.get("session_start")!({}, {
	hasUI: false,
	cwd: "/tmp",
	model: { id: "test-model" },
	getContextUsage: () => ({ percent: 0 }),
	ui: {},
});
console.log("ready");
setInterval(() => {}, 60_000);
