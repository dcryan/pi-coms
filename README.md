# pi-coms

Peer-to-peer messaging between pi sessions on the same machine. Each session listens on a Unix socket and finds its peers through registry files in `~/.pi/coms/`.

Forked from `extensions/coms.ts` in [disler/pi-vs-claude-code](https://github.com/disler/pi-vs-claude-code) (MIT, see `LICENSE`). The transport, discovery and pool widget are largely unchanged. Replies work differently; see [Changes from coms.ts](#changes-from-comsts).

## Run

```bash
pi -e ~/Development/pi-coms --coms-name planner --coms-purpose "Plans the work"
pi -e ~/Development/pi-coms --coms-name coder   --coms-purpose "Writes the code"
```

| Flag | Default |
|---|---|
| `--coms-name` | `name` in the frontmatter of the `--system-prompt`/`--append-system-prompt` file, else `agent-xxxxxx`. A taken name gets a number appended. |
| `--coms-purpose` | frontmatter `description` |
| `--coms-project` | `default`. Agents only see peers in the same project. |
| `--coms-color` | frontmatter `color`, else a colour derived from the session id |
| `--coms-explicit` | off. Hides the agent from `coms_list` and the widget; peers can still message it by exact name. |

| Environment variable | Default |
|---|---|
| `PI_COMS_DIR` | `~/.pi/coms` |
| `PI_COMS_MAX_HOPS` | `8` |
| `PI_COMS_TIMEOUT_MS` | `1800000` (30 min), how long a sender waits for a reply |
| `PI_COMS_PING_INTERVAL_MS` | `10000` |

`/coms` shows this agent's identity, its peers, messages it's waiting on and messages it hasn't answered.

## Tools

| Tool | |
|---|---|
| `coms_list` | Peers with purpose, model, working directory and context usage |
| `coms_send` | Send a message. Returns a `msg_id` once the peer has queued it. Optional `on_behalf_of` and `response_schema`. |
| `coms_reply` | Answer a received message by `msg_id`. The only way a reply reaches the sender. |
| `coms_await` | Wait for the reply to a sent message |

## How a message travels

1. Alice calls `coms_send`. Bob's extension queues the message into his conversation as a follow-up, so it waits until his current work finishes, then starts a turn if he's idle.
2. Bob's model sees `📨 coms message from alice (msg_id …)` and answers with `coms_reply(msg_id, response)`.
3. If alice is blocked in `coms_await`, the reply is the tool's result. Otherwise it arrives in her conversation as `📬 coms reply from bob …` and starts a turn.

If Bob reads a message and ends his turn without answering, he is reminded once. If he ignores the reminder too, alice gets an error saying so. If Bob's run is aborted (Esc), the message stays unanswered and he's reminded at the end of his next run. Unanswered messages show in his status line. If Bob exits, alice gets an error for each message he hadn't answered.

**Hops.** A message sent while handling another continues that message's hop count. Pass `on_behalf_of`, or the extension takes the highest count among the messages the agent is currently handling. A reply counts as a hop too, so two agents bouncing messages back and forth also hit the limit. A run started by the user begins again at zero.

## Discovery and cleanup

An agent registers at startup by opening `sockets/<session-id>.sock` and writing `projects/<project>/agents/<name>.json` with its PID, socket and heartbeat time. It rewrites the file every 30 seconds. There's no central list: `coms_list` and `coms_send` read the folder on each call, and every agent re-reads it and pings its peers every 10 seconds for the widget.

On a clean exit an agent deletes its own file and socket. After a crash, the other agents clean up. A peer is treated as dead, and its file and socket are deleted, when:

- its PID no longer exists
- nothing is listening on its socket (for example, its PID was reused by another process)
- it doesn't answer a ping and its heartbeat is more than 90 seconds old (hung). A stale heartbeat alone doesn't count, since a sleeping laptop pauses every agent's heartbeat.

Messages still waiting on a peer that has disappeared fail with `<name> is no longer running` at the next ping cycle, so senders don't wait out the 30-minute timeout.

## Changes from coms.ts

- **Explicit replies.** The original sent the agent's last assistant text to the most recent unanswered message whenever any run ended. A turn the user started could be sent to a peer, and of two messages arriving together only the newer got an answer. Now only `coms_reply` sends a reply, and each message is answered by id.
- **Failed or aborted runs** no longer send anything. Unanswered messages get a reminder, then an error to the sender (see above).
- **Replies are delivered** into the sender's conversation when it isn't waiting in `coms_await`. Previously they were only visible by polling with `coms_get`, which has been removed.
- **Hop count per message** instead of one shared variable.
- **8 MiB message cap**, measured in bytes. Was 64 KB, measured in characters.
- `coms_await` can be aborted with Esc.
- Flags are prefixed `--coms-` so they don't collide with other extensions.
- No dependency on the demo repo's `themeMap.ts`. Tool schemas are plain JSON Schema.
- Crashed agents' sockets are deleted, unreachable or hung agents are removed, and messages waiting on a vanished peer fail at the next ping cycle (see above).
- `coms-net` (the networked version) is not included.

## Limits

- Nothing is persisted. If either side restarts, messages still waiting for a reply are lost.
- Any process running as your user can send a prompt to an agent. The coms directory is `0700`, so other users can't.
- `response_schema` is shown to the receiver, and the reply must parse as JSON. It isn't validated against the schema.
- Same machine only.

## Tests

```bash
npm test             # unit tests: two in-process agents over real sockets, no model calls
npm run e2e          # two real pi processes in RPC mode; a few cheap model calls
node scripts/e2e.ts openai/gpt-5.4-mini   # pick another provider/model
```
