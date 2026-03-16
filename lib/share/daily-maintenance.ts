import type { ColdStorageBucketLike } from "@/lib/share/cold-storage";
import { runShareArchive } from "@/lib/share/archive";
import { runShareViewRollup, type ShareViewRollupResult } from "@/lib/share/view-stats";

export type DailyShareMaintenanceResult = {
  archive?: Awaited<ReturnType<typeof runShareArchive>>;
  shareViews?: ShareViewRollupResult;
};

type WorkerEnvLike = Record<string, unknown> | undefined;

export async function runDailyShareMaintenance(options?: {
  coldStorageBucket?: ColdStorageBucketLike | null;
  env?: WorkerEnvLike;
  logLabel?: string;
}) {
  const result: DailyShareMaintenanceResult = {};
  const failures: string[] = [];

  try {
    result.archive = await runShareArchive({
      coldStorageBucket: options?.coldStorageBucket ?? null,
      logLabel: options?.logLabel ? `${options.logLabel}:archive` : undefined,
    });
  } catch (error) {
    failures.push(`archive=${error instanceof Error ? error.message : String(error)}`);
  }

  try {
    result.shareViews = await runShareViewRollup({
      env: options?.env,
      logLabel: options?.logLabel ? `${options.logLabel}:share-views` : undefined,
    });
  } catch (error) {
    failures.push(`shareViews=${error instanceof Error ? error.message : String(error)}`);
  }

  if (options?.logLabel) {
    console.log(`${options.logLabel} ${JSON.stringify(result)}`);
  }

  if (failures.length > 0) {
    throw new Error(failures.join("; "));
  }

  return result;
}
