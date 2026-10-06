/** Wire types mirroring fulfillment-execution's dto.go response shapes
 *  exactly (taskResponse / queueDepthResponse / stationResponse) -- kept
 *  hand-in-sync with the Go DTOs rather than code-generated for v1, same
 *  convention as order-mgmt-mfe's types.ts. */

/** Task.Type: the process path a task belongs to (see internal/domain/task/task.go). */
export type TaskType = "PICK" | "PACK" | "SLAM" | "REBIN" | "DISPATCH" | "ARRIVAL";

/** Task.Status: Pending -> Claimed(leased) -> Completed, or lease-expires
 *  back to Pending. Exact enum values from internal/domain/task/task.go. */
export type TaskStatus = "PENDING" | "CLAIMED" | "COMPLETED";

/** Mirrors taskResponse in internal/adapters/inbound/http/dto.go. */
export interface Task {
  id: string;
  type: TaskType;
  status: TaskStatus;
  cpt: string;
  orderRef: string;
  requiredCapabilities: string[];
  fragile: boolean;
  giftWrap: boolean;
  leaseStationId?: string;
  leaseExpiry?: string;
}

/** Mirrors queueDepthResponse in internal/adapters/inbound/http/dto.go. */
export interface QueueDepth {
  taskType: string;
  depth: number;
}

/** Mirrors stationResponse in internal/adapters/inbound/http/dto.go. */
export interface Station {
  id: string;
  capabilities: string[];
  occupied: boolean;
}
