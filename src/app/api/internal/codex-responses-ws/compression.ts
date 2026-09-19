/** Prompt compression for each native Responses WebSocket turn. */
import { emit } from "@/lib/events/eventBus";
import { logger } from "@omniroute/open-sse/utils/logger.ts";
import { resolveConnectionCacheOverride } from "@omniroute/open-sse/utils/cacheControlPolicy.ts";
import { estimateTokens, getTokenLimit } from "@omniroute/open-sse/services/contextManager.ts";
import { getResolvedModelCapabilities } from "@omniroute/open-sse/services/modelCapabilities.ts";
import { adaptBodyForCompression } from "@omniroute/open-sse/services/compression/bodyAdapter.ts";
import { isCompressionExcluded } from "@omniroute/open-sse/services/compression/exclusions.ts";
import { resolveOmniGlyphTransport } from "@omniroute/open-sse/services/compression/imageTransportPolicy.ts";
import { ensureEngineBreakdown } from "@omniroute/open-sse/services/compression/engineBreakdown.ts";
import {
  createCompressionStats,
  trackCompressionStats,
} from "@omniroute/open-sse/services/compression/stats.ts";
import {
  selectCompressionPlan,
  applyCompressionAsync,
  resolveCacheAwareConfig,
} from "@omniroute/open-sse/services/compression/strategySelector.ts";
import type { CompressionMode } from "@omniroute/open-sse/services/compression/types.ts";
import { resolveOutputStyleSelection } from "@omniroute/open-sse/services/compression/outputStyles/backCompat.ts";
import {
  applyOutputStyles,
  resolveOutputStyleLanguage,
} from "@omniroute/open-sse/services/compression/outputStyles/apply.ts";
import { resolveCompressionSettings } from "@omniroute/open-sse/handlers/chatCore/compressionSettings.ts";
import { resolveRequestCompressionConfig } from "@omniroute/open-sse/handlers/chatCore/compressionConfig.ts";
import { resolveCompressionHeader } from "@omniroute/open-sse/handlers/chatCore/headers.ts";
import { forwardDashboardEventToLiveWs } from "@omniroute/open-sse/handlers/chatCore/telemetryHelpers.ts";
import { emitOutputStyleTelemetry } from "@omniroute/open-sse/handlers/chatCore/outputStyleTelemetry.ts";
import { writeCavemanOutputAnalytics } from "@omniroute/open-sse/handlers/chatCore/cavemanOutputAnalytics.ts";
import {
  writeCompressionAnalytics,
  writeCompressionSkip,
} from "@omniroute/open-sse/handlers/chatCore/compressionAnalyticsWrite.ts";

const log = logger("RESPONSES_WS_COMPRESSION");
type JsonRecord = Record<string, unknown>;

export type ResponsesWsCompressionContext = {
  provider: string;
  model: string;
  requestId: string;
  headers?: Headers | Record<string, unknown>;
  comboName?: string | null;
  routingComboId?: string | null;
  apiKeyInfo?: { id?: string; compressionEnabled?: boolean } | null;
  providerSpecificData?: JsonRecord;
};

/** Use HTTP's profile resolution and engines; preserve the native Responses envelope. */
export async function applyResponsesWsCompression(
  responseBody: JsonRecord,
  ctx: ResponsesWsCompressionContext
): Promise<JsonRecord> {
  try {
    const { settings, enabled } = await resolveCompressionSettings(log);
    if (!enabled || !settings || ctx.apiKeyInfo?.compressionEnabled === false) return responseBody;

    const adapter = adaptBodyForCompression(responseBody);
    if (!Array.isArray(adapter.body.messages) || adapter.body.messages.length === 0) {
      return responseBody;
    }
    let estimatedTokens = estimateTokens(adapter.body.messages);
    const writeContext = {
      provider: ctx.provider,
      effectiveModel: ctx.model,
      effectiveServiceTier:
        typeof responseBody.service_tier === "string" ? responseBody.service_tier : undefined,
      comboName: ctx.comboName,
      skillRequestId: ctx.requestId,
      cavemanOutputModeApplied: false,
      cavemanOutputModeIntensity: null as string | null,
      log,
    };
    if (isCompressionExcluded({ provider: ctx.provider, model: ctx.model }, settings.exclusions)) {
      await writeCompressionSkip(
        {
          ...writeContext,
          stats: createCompressionStats(adapter.body, adapter.body, "off", []),
          mode: "off",
          compressionComboId: null,
        },
        "excluded"
      );
      return responseBody;
    }

    const compressionHeader = resolveCompressionHeader(ctx.headers ?? null);
    if (compressionHeader?.trim().toLowerCase() === "off") return responseBody;
    const cachingContext = {
      provider: ctx.provider,
      targetFormat: "openai-responses",
      model: ctx.model,
      connectionCacheOverride: resolveConnectionCacheOverride(ctx.providerSpecificData),
    };
    const resolved = await resolveRequestCompressionConfig({
      config: settings,
      comboName: ctx.comboName,
      routingComboId: ctx.routingComboId,
      estimatedTokens,
      body: responseBody,
      cachingContext,
      compressionHeader,
      log,
    });
    let config = resolved.config;
    const selection = resolveOutputStyleSelection(config);
    const outputStyleResult =
      selection.length > 0
        ? applyOutputStyles(
            responseBody,
            selection,
            resolveOutputStyleLanguage(config.languageConfig, responseBody)
          )
        : null;
    let body: JsonRecord = outputStyleResult?.body ?? responseBody;
    if (outputStyleResult?.applied) {
      writeContext.cavemanOutputModeApplied = true;
      writeContext.cavemanOutputModeIntensity =
        outputStyleResult.appliedStyles?.map((style) => `${style.id}:${style.level}`).join(",") ??
        null;
      estimatedTokens = estimateTokens(adaptBodyForCompression(body).body.messages);
    }
    const plan = selectCompressionPlan(
      config,
      resolved.compressionComboKey,
      estimatedTokens,
      body,
      cachingContext,
      resolved.namedCombos,
      compressionHeader,
      {
        modelContextLimit: getTokenLimit(ctx.provider, ctx.model),
        requestMaxTokens:
          typeof body.max_output_tokens === "number" ? body.max_output_tokens : null,
      }
    );
    const mode = plan.mode as CompressionMode;
    if (mode === "stacked" && plan.stackedPipeline.length > 0) {
      config = {
        ...config,
        stackedPipeline: plan.stackedPipeline as typeof config.stackedPipeline,
      };
    }
    emitOutputStyleTelemetry({
      outputStyleResult,
      skillRequestId: ctx.requestId,
      traceId: ctx.requestId,
      effectiveModel: ctx.model,
      provider: ctx.provider,
      compressionComboId: config.compressionComboId,
      estimatedTokens,
      log,
    });

    // Engines own adaptation/restoration. Pass native input so worker boundaries
    // cannot discard the synthetic messages' symbol mappings and lose savings.
    const result =
      mode === "off"
        ? null
        : await applyCompressionAsync(body, mode, {
            model: ctx.model,
            supportsVision: getResolvedModelCapabilities({
              provider: ctx.provider,
              model: ctx.model,
            }).supportsVision,
            ...resolveOmniGlyphTransport(ctx.provider),
            provider: ctx.provider,
            sourceFormat: "openai-responses",
            targetFormat: "openai-responses",
            compressionStage: "pre-translation",
            config: resolveCacheAwareConfig(config, body, cachingContext),
            cachingContext,
            principalId: ctx.apiKeyInfo?.id,
            onEngineStep: (step) => {
              const payload = {
                ...step,
                requestId: ctx.requestId,
                comboId: config.compressionComboId ?? null,
                mode,
                timestamp: Date.now(),
              };
              emit("compression.step", payload);
              void forwardDashboardEventToLiveWs("compression.step", payload);
            },
          });
    if (result?.stats) {
      const writeOpts = {
        ...writeContext,
        stats: result.stats,
        mode,
        compressionComboId: config.compressionComboId,
      };
      if (result.compressed || result.stats.fallbackApplied || outputStyleResult?.applied) {
        trackCompressionStats(result.stats);
        await writeCompressionAnalytics(writeOpts);
      } else {
        await writeCompressionSkip(writeOpts, "no_savings");
      }
      if (result.compressed) {
        body = result.body;
        const payload = {
          requestId: ctx.requestId,
          comboId: config.compressionComboId ?? null,
          mode,
          originalTokens: result.stats.originalTokens,
          compressedTokens: result.stats.compressedTokens,
          savingsPercent: result.stats.savingsPercent,
          engineBreakdown: ensureEngineBreakdown(result.stats),
          validationWarnings: result.stats.validationWarnings,
          fallbackApplied: result.stats.fallbackApplied,
          timestamp: Date.now(),
        };
        emit("compression.completed", payload);
        void forwardDashboardEventToLiveWs("compression.completed", payload);
      }
    } else if (outputStyleResult?.applied) {
      await writeCavemanOutputAnalytics({
        ...writeContext,
        compressionComboId: config.compressionComboId,
        estimatedTokens,
      });
    }
    return body;
  } catch (err) {
    log.warn(
      `[codex-responses-ws] compression skipped: ${err instanceof Error ? err.message : String(err)}`
    );
    return responseBody;
  }
}
