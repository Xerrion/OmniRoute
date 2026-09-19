import type {
  CompressionConfig,
  CompressionPipelineStep,
} from "../../services/compression/types.ts";
import type { CachingDetectionContext } from "../../services/compression/cachingAware.ts";
import {
  selectCompressionStrategy,
  enginesMapDerivesStackedPipeline,
  activeComboResolves,
  buildNamedComboLookup,
} from "../../services/compression/strategySelector.ts";
import {
  isBuiltinStackedPipeline,
  isStackedCompressionCombo,
  type RuntimeCompressionCombo,
} from "./compressionComboPredicates.ts";

/** Resolve the same routing, named-profile, and legacy defaults for HTTP and WebSocket turns. */
export async function resolveRequestCompressionConfig({
  config,
  comboName = null,
  routingComboId = null,
  isCombo = Boolean(comboName),
  estimatedTokens,
  body,
  cachingContext,
  compressionHeader = null,
  log,
}: {
  config: CompressionConfig;
  comboName?: string | null;
  routingComboId?: string | null;
  isCombo?: boolean;
  estimatedTokens: number;
  body: Record<string, unknown>;
  cachingContext: CachingDetectionContext;
  compressionHeader?: string | null;
  log?: { debug?: (...args: unknown[]) => void } | null;
}) {
  let compressionComboKey = comboName ?? null;
  let compressionComboApplied = false;
  const applyCompressionComboConfig = (
    compressionCombo: RuntimeCompressionCombo | null,
    routingOverrideIds: string[] = []
  ): boolean => {
    if (!compressionCombo || compressionCombo.pipeline.length === 0) return false;
    const comboLanguagePacks = [
      ...new Set(
        compressionCombo.languagePacks.map((pack) => pack.trim()).filter((pack) => pack.length > 0)
      ),
    ];
    const comboOutputIntensity = (
      ["lite", "full", "ultra"].includes(compressionCombo.outputModeIntensity)
        ? compressionCombo.outputModeIntensity
        : (config.cavemanOutputMode?.intensity ?? "full")
    ) as "lite" | "full" | "ultra";
    const comboDefaultLanguage =
      comboLanguagePacks.find((pack) => pack === config.languageConfig?.defaultLanguage) ??
      comboLanguagePacks[0] ??
      config.languageConfig?.defaultLanguage ??
      "en";
    const comboOverrides = { ...(config.comboOverrides ?? {}) };
    for (const id of routingOverrideIds) {
      if (id) comboOverrides[id] = "stacked";
    }
    config = {
      ...config,
      compressionComboId: compressionCombo.id,
      stackedPipeline: compressionCombo.pipeline,
      languageConfig: {
        ...(config.languageConfig ?? {
          enabled: false,
          defaultLanguage: "en",
          autoDetect: true,
          enabledPacks: ["en"],
        }),
        enabled: true,
        defaultLanguage: comboDefaultLanguage,
        enabledPacks:
          comboLanguagePacks.length > 0
            ? comboLanguagePacks
            : (config.languageConfig?.enabledPacks ?? ["en"]),
      },
      cavemanOutputMode: {
        ...(config.cavemanOutputMode ?? {
          enabled: false,
          intensity: "full",
          autoClarity: true,
        }),
        enabled: compressionCombo.outputMode,
        intensity: comboOutputIntensity,
      },
      comboOverrides,
    };
    compressionComboApplied = true;
    return true;
  };
  if ((isCombo && comboName) || routingComboId) {
    try {
      const { getComboByName } = await import("@/lib/db/combos");
      let comboConfig = comboName ? await getComboByName(comboName) : null;
      if (!comboConfig && comboName?.startsWith("combo/")) {
        comboConfig = await getComboByName(comboName.substring(6));
      }
      const comboRuntimeConfig =
        comboConfig?.config && typeof comboConfig.config === "object"
          ? (comboConfig.config as Record<string, unknown>)
          : {};
      const comboMode =
        typeof comboRuntimeConfig.compressionMode === "string"
          ? comboRuntimeConfig.compressionMode
          : typeof comboConfig?.compressionOverride === "string"
            ? comboConfig.compressionOverride
            : null;
      if (
        comboMode === "off" ||
        comboMode === "lite" ||
        comboMode === "standard" ||
        comboMode === "aggressive" ||
        comboMode === "ultra" ||
        comboMode === "rtk" ||
        comboMode === "stacked"
      ) {
        config = {
          ...config,
          comboOverrides: {
            ...(config.comboOverrides ?? {}),
            ...(comboName ? { [comboName]: comboMode } : {}),
            ...(comboConfig?.id ? { [String(comboConfig.id)]: comboMode } : {}),
          },
        };
        compressionComboKey = comboName;
      }
      const routingComboIds = [
        comboConfig?.id,
        comboName,
        routingComboId,
        comboName?.startsWith("combo/") ? comboName.substring(6) : null,
      ].filter((id): id is string => typeof id === "string" && id.length > 0);
      if (routingComboIds.length > 0) {
        const { getCompressionComboForRoutingCombo } = await import("@/lib/db/compressionCombos");
        const assignedCompressionCombo =
          routingComboIds
            .map((id) => getCompressionComboForRoutingCombo(id))
            .find((combo) => combo !== null) ?? null;
        if (
          applyCompressionComboConfig(
            assignedCompressionCombo as RuntimeCompressionCombo | null,
            routingComboIds
          )
        ) {
          compressionComboKey = comboName;
        }
      }
    } catch (err) {
      log?.debug?.(
        "COMPRESSION",
        "Combo compression override lookup skipped: " +
          (err instanceof Error ? err.message : String(err))
      );
    }
  }
  let namedCombos: Record<string, CompressionPipelineStep[]> = {};
  try {
    const { listCompressionCombos } = await import("@/lib/db/compressionCombos");
    namedCombos = buildNamedComboLookup(listCompressionCombos());
  } catch (err) {
    log?.debug?.(
      "COMPRESSION",
      "Named combos load skipped: " + (err instanceof Error ? err.message : String(err))
    );
  }
  const modeBeforeOutputTransform = selectCompressionStrategy(
    config,
    compressionComboKey,
    estimatedTokens,
    body as Record<string, unknown>,
    cachingContext,
    namedCombos,
    compressionHeader
  );
  if (
    modeBeforeOutputTransform === "stacked" &&
    !compressionComboApplied &&
    !config.compressionComboId &&
    isBuiltinStackedPipeline(config.stackedPipeline) &&
    // Don't let the legacy default combo override a panel-configured engines map: when the
    // operator's explicit engines derive their own stacked pipeline, that pipeline (applied
    // below from compressionPlan.stackedPipeline) is authoritative. Legacy/backfilled
    // installs (enginesExplicit false) still fall through to the seeded default combo.
    !enginesMapDerivesStackedPipeline(config) &&
    // Never let the legacy seeded default combo shadow the operator's active profile.
    !activeComboResolves(config, namedCombos)
  ) {
    try {
      const { getDefaultCompressionCombo } = await import("@/lib/db/compressionCombos");
      const defaultCompressionCombo = getDefaultCompressionCombo();
      if (
        isStackedCompressionCombo(defaultCompressionCombo as RuntimeCompressionCombo | null) &&
        applyCompressionComboConfig(defaultCompressionCombo as RuntimeCompressionCombo | null)
      ) {
        log?.debug?.(
          "COMPRESSION",
          `Default compression combo applied: ${defaultCompressionCombo?.id}`
        );
      }
    } catch (err) {
      log?.debug?.(
        "COMPRESSION",
        "Default compression combo lookup skipped: " +
          (err instanceof Error ? err.message : String(err))
      );
    }
  }

  return { config, compressionComboKey, compressionComboApplied, namedCombos };
}
