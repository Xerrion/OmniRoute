import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-ws-compression-"));
process.env.DATA_DIR = dataDir;
process.env.APP_LOG_TO_FILE = "false";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";

const core = await import("../../src/lib/db/core.ts");
const { updateCompressionSettings } = await import("../../src/lib/db/compression.ts");
const combos = await import("../../src/lib/db/compressionCombos.ts");
const { createCombo } = await import("../../src/lib/db/combos.ts");
const { getEventHistory } = await import("../../src/lib/events/eventBus.ts");
const { DEFAULT_COMPRESSION_CONFIG } = await import("../../open-sse/services/compression/types.ts");
const { applyResponsesWsCompression } =
  await import("../../src/app/api/internal/codex-responses-ws/compression.ts");

let requestNumber = 0;
let requestId: string;

test.before(() => {
  test.mock.method(globalThis, "fetch", async () => new Response(null, { status: 204 }));
});

test.beforeEach(async () => {
  requestId = `ws-compression-${++requestNumber}`;
  await updateCompressionSettings({
    ...DEFAULT_COMPRESSION_CONFIG,
    enabled: true,
    defaultMode: "off",
    autoTriggerTokens: 0,
    engines: {},
    outputStyles: [],
    exclusions: [],
    activeComboId: null,
  });
});

test.after(async () => {
  // Output-style telemetry is deliberately best-effort and asynchronous.
  await new Promise((resolve) => setTimeout(resolve, 100));
  core.resetDbInstance();
  fs.rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

function body() {
  return {
    model: "gpt-5.5",
    instructions: "Keep the response accurate.",
    tools: [{ type: "function", name: "run_command", parameters: { type: "object" } }],
    input: [
      { type: "reasoning", id: "reasoning-1", encrypted_content: "opaque-reasoning" },
      { type: "function_call", call_id: "call-1", name: "run_command", arguments: "{}" },
      {
        type: "function_call_output",
        call_id: "call-1",
        output: JSON.stringify(
          Array.from({ length: 30 }, (_, index) => ({ file: `file-${index}`, status: "complete" })),
          null,
          2
        ),
      },
    ],
  };
}

async function run(input: Record<string, unknown> = body(), context: Record<string, unknown> = {}) {
  return applyResponsesWsCompression(input, {
    provider: "codex",
    model: "gpt-5.5",
    requestId,
    ...context,
  });
}

function analytics() {
  return core
    .getDbInstance()
    .prepare("SELECT * FROM compression_analytics WHERE request_id = ?")
    .get(requestId) as Record<string, unknown> | undefined;
}

function events() {
  return getEventHistory().filter(
    (entry) => (entry.payload as { requestId?: string }).requestId === requestId
  );
}

test("WS uses the active named pipeline and preserves native Responses items", async () => {
  const profile = combos.createCompressionCombo({
    name: "WS named profile",
    pipeline: [{ engine: "codex-responses" }],
  });
  await updateCompressionSettings({ activeComboId: profile.id });
  const input = body();
  const original = structuredClone(input);
  const result = await run(input);
  const items = result.input as typeof input.input;

  assert.ok(String(items[2].output).length < input.input[2].output!.length);
  assert.deepEqual(JSON.parse(String(items[2].output)), JSON.parse(input.input[2].output!));
  assert.deepEqual(items.slice(0, 2), input.input.slice(0, 2));
  assert.deepEqual(result.tools, input.tools);
  assert.equal(result.instructions, input.instructions);
  assert.equal(result.messages, undefined);
  assert.deepEqual(input, original, "compression must not mutate the caller's history");
  assert.equal(analytics()?.mode, "stacked");
});

test("WS executes the engines-derived pipeline and emits per-engine progress", async () => {
  await updateCompressionSettings({
    engines: { "codex-responses": { enabled: true }, headroom: { enabled: true } },
  });
  const result = await run();
  assert.notDeepEqual(result, body());
  const steps = events()
    .filter((entry) => entry.event === "compression.step")
    .map((entry) => (entry.payload as { engine: string }).engine);
  assert.deepEqual(steps, ["codex-responses", "headroom"]);
  assert.ok(events().some((entry) => entry.event === "compression.completed"));
  const breakdown = core
    .getDbInstance()
    .prepare("SELECT engine FROM compression_engine_breakdown WHERE request_id = ? ORDER BY id")
    .all(requestId) as Array<{ engine: string }>;
  assert.deepEqual(
    breakdown.map((entry) => entry.engine),
    steps
  );
});

test("WS resolves named per-request overrides even when the default is off", async () => {
  combos.createCompressionCombo({
    name: "WS header profile",
    pipeline: [{ engine: "codex-responses" }],
  });
  const result = await run(body(), { headers: { "X-OmniRoute-Compression": "WS HEADER PROFILE" } });
  assert.notDeepEqual(result, body());
  assert.equal(analytics()?.mode, "stacked");
});

test("WS honors the request off override, including output styles", async () => {
  await updateCompressionSettings({
    engines: { "codex-responses": { enabled: true } },
    outputStyles: [{ id: "terse-prose", level: "full" }],
  });
  const input = body();
  assert.deepEqual(await run(input, { headers: { "x-omniroute-compression": "off" } }), input);
  assert.equal(analytics(), undefined);
  assert.deepEqual(events(), []);
});

test("WS honors routing-combo compression assignments", async () => {
  const route = await createCombo({ name: "ws-compression-route", models: [] });
  const profile = combos.createCompressionCombo({
    name: "WS assigned profile",
    pipeline: [{ engine: "codex-responses" }],
  });
  combos.assignRoutingCombo(profile.id, String(route.id));
  const result = await run(body(), { comboName: route.name, routingComboId: route.id });
  assert.notDeepEqual(result, body());
  assert.equal(analytics()?.compression_combo_id, profile.id);
  assert.equal(analytics()?.combo_id, route.name);
});

test("WS honors routing-combo off overrides above the active profile", async () => {
  const route = await createCombo({
    name: "ws-compression-off-route",
    models: [],
    config: { compressionMode: "off" },
  });
  const profile = combos.createCompressionCombo({
    name: "WS overridden profile",
    pipeline: [{ engine: "codex-responses" }],
  });
  await updateCompressionSettings({ activeComboId: profile.id });
  assert.deepEqual(await run(body(), { comboName: route.name, routingComboId: route.id }), body());
});

test("WS applies output styles when input compression is off", async () => {
  await updateCompressionSettings({ outputStyles: [{ id: "terse-prose", level: "full" }] });
  const result = await run();
  assert.match(String(result.instructions), /\[OmniRoute Output Styles\]/);
  assert.deepEqual(result.input, body().input);
  assert.equal(analytics()?.mode, "output-caveman");
});

test("WS preserves the body when the API key disables compression", async () => {
  await updateCompressionSettings({
    engines: { "codex-responses": { enabled: true } },
    outputStyles: [{ id: "terse-prose", level: "full" }],
  });
  assert.deepEqual(await run(body(), { apiKeyInfo: { compressionEnabled: false } }), body());
});

test("WS respects provider exclusions and records the skip", async () => {
  await updateCompressionSettings({
    engines: { "codex-responses": { enabled: true } },
    exclusions: ["codex/*"],
  });
  assert.deepEqual(await run(), body());
  assert.equal(analytics()?.skip_reason, "excluded");
});

test("WS master-off overrides a named request profile and output styles", async () => {
  await updateCompressionSettings({
    enabled: false,
    outputStyles: [{ id: "terse-prose", level: "full" }],
  });
  assert.deepEqual(
    await run(body(), { headers: { "x-omniroute-compression": "WS HEADER PROFILE" } }),
    body()
  );
});

test("WS records a no-savings attempt without changing tool or reasoning items", async () => {
  const profile = combos.createCompressionCombo({
    name: "WS no-savings profile",
    pipeline: [{ engine: "codex-responses" }],
  });
  await updateCompressionSettings({ activeComboId: profile.id });
  const input = body();
  input.input[2].output = "Done.";
  assert.deepEqual(await run(input), input);
  assert.equal(analytics()?.skip_reason, "no_savings");
  assert.ok(!events().some((entry) => entry.event === "compression.completed"));
});

test("WS leaves an opaque continuation without compressible input untouched", async () => {
  await updateCompressionSettings({ engines: { "codex-responses": { enabled: true } } });
  const input = { previous_response_id: "resp_previous", input: [body().input[0]] };
  assert.deepEqual(await run(input), input);
  assert.equal(analytics(), undefined);
});

test("the shared resolver retains customized legacy defaults without mutating global settings", async () => {
  const { resolveRequestCompressionConfig } =
    await import("../../open-sse/handlers/chatCore/compressionConfig.ts");
  const profile = combos.createCompressionCombo({
    name: "Legacy default for both transports",
    pipeline: [{ engine: "codex-responses" }],
    languagePacks: ["en"],
    outputMode: true,
    outputModeIntensity: "lite",
    isDefault: true,
  });
  const config = { ...DEFAULT_COMPRESSION_CONFIG, enabled: true, defaultMode: "stacked" as const };
  const original = structuredClone(config);
  const resolved = await resolveRequestCompressionConfig({
    config,
    body: body(),
    estimatedTokens: 100,
    cachingContext: { provider: "codex", model: "gpt-5.5", targetFormat: "openai-responses" },
  });
  assert.equal(resolved.config.compressionComboId, profile.id);
  assert.deepEqual(resolved.config.stackedPipeline, profile.pipeline);
  assert.equal(resolved.config.cavemanOutputMode?.enabled, true);
  assert.equal(resolved.config.cavemanOutputMode?.intensity, "lite");
  assert.deepEqual(config, original);
});
