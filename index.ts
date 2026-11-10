import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Text, truncateToWidth } from "@earendil-works/pi-tui";
import { installComs } from "./src/coms.ts";

export default function (pi: ExtensionAPI) {
	installComs(pi, { tui: { Text, truncateToWidth } });
}
