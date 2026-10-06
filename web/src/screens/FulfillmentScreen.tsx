import { useState, type FormEvent } from "react";
import { FULFILLMENT_API_BASE } from "../config";
import type { QueueDepth, Task, TaskType } from "../types";
import { Card, StatusPill, DataTable, useFetch } from "@warehouse/ui-kit";

const TASK_TYPES: TaskType[] = ["PICK", "PACK", "SLAM", "REBIN", "DISPATCH", "ARRIVAL"];

const inputStyle = {
  flex: 1,
  maxWidth: 360,
  padding: "10px 12px",
  borderRadius: "var(--wh-radius-md)",
  border: "1px solid var(--wh-color-border)",
  background: "var(--wh-color-bg-sunken)",
  color: "var(--wh-color-text)",
  fontFamily: "var(--wh-font-mono)",
  fontSize: "var(--wh-font-size-sm)",
} as const;

const buttonStyle = {
  padding: "10px 18px",
  borderRadius: "var(--wh-radius-md)",
  border: "none",
  background: "var(--wh-color-accent)",
  color: "#fff",
  fontWeight: 600,
  fontSize: "var(--wh-font-size-sm)",
  cursor: "pointer",
} as const;

/**
 * Fulfillment queue-depth dashboard + task-by-orderRef lookup. Both
 * sections are wired to real, live REST endpoints (see
 * internal/adapters/inbound/http/router.go):
 *   - GET /queues/{taskType}/depth  -> current pending-task count
 *   - GET /tasks?orderRef=          -> tasks for an order (added in
 *     feature/tasks-by-order-ref for cross-service order lookups)
 * Verified end to end against a live local backend: created a real task
 * via POST /tasks and confirmed it round-trips through both this screen's
 * queue-depth count and its orderRef lookup table.
 */
export function FulfillmentScreen() {
  const [taskType, setTaskType] = useState<TaskType>("PICK");
  const [selectedTaskType, setSelectedTaskType] = useState<TaskType | null>(null);

  const [orderRefQuery, setOrderRefQuery] = useState("");
  const [orderRef, setOrderRef] = useState<string | null>(null);

  const depthUrl = selectedTaskType
    ? `${FULFILLMENT_API_BASE}/queues/${encodeURIComponent(selectedTaskType)}/depth`
    : null;
  const { data: depth, loading: depthLoading, error: depthError } =
    useFetch<QueueDepth>(depthUrl);

  const tasksUrl = orderRef
    ? `${FULFILLMENT_API_BASE}/tasks?orderRef=${encodeURIComponent(orderRef)}`
    : null;
  const { data: tasks, loading: tasksLoading, error: tasksError } =
    useFetch<Task[]>(tasksUrl);

  function onDepthSubmit(e: FormEvent) {
    e.preventDefault();
    setSelectedTaskType(taskType);
  }

  function onOrderRefSubmit(e: FormEvent) {
    e.preventDefault();
    const trimmed = orderRefQuery.trim();
    if (trimmed) setOrderRef(trimmed);
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Fulfillment</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          fulfillment-execution · pick/pack/SLAM task lifecycle, queue depth, station leases
        </p>
      </div>

      <Card title="Queue depth">
        <form
          onSubmit={onDepthSubmit}
          style={{ display: "flex", gap: "var(--wh-space-2)", marginBottom: "var(--wh-space-4)" }}
        >
          <select
            value={taskType}
            onChange={(e) => setTaskType(e.target.value as TaskType)}
            style={{ ...inputStyle, flex: "none", width: 160, fontFamily: "inherit" }}
          >
            {TASK_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
          <button type="submit" style={buttonStyle}>
            Check depth
          </button>
        </form>

        {depthError && (
          <div style={{ color: "var(--wh-color-status-danger)" }}>{depthError.message}</div>
        )}

        {!selectedTaskType && !depthError && (
          <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
            Choose a task type and check its current queue depth.
          </div>
        )}

        {depthLoading && (
          <div
            style={{
              height: 40,
              width: 120,
              borderRadius: 6,
              background: "var(--wh-color-border-subtle)",
              animation: "wh-shimmer 1.4s ease-in-out infinite",
            }}
          />
        )}

        {depth && !depthLoading && (
          <div style={{ display: "flex", alignItems: "baseline", gap: "var(--wh-space-3)" }}>
            <span style={{ fontSize: "var(--wh-font-size-2xl)", fontWeight: 700 }}>
              {depth.depth}
            </span>
            <span style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              pending {depth.taskType} tasks
            </span>
          </div>
        )}
      </Card>

      <Card title="Task lookup">
        <form
          onSubmit={onOrderRefSubmit}
          style={{ display: "flex", gap: "var(--wh-space-2)", marginBottom: "var(--wh-space-4)" }}
        >
          <input
            value={orderRefQuery}
            onChange={(e) => setOrderRefQuery(e.target.value)}
            placeholder="Order reference"
            style={inputStyle}
          />
          <button type="submit" style={buttonStyle}>
            Find tasks
          </button>
        </form>

        {tasksError && (
          <div style={{ color: "var(--wh-color-status-danger)", marginBottom: "var(--wh-space-3)" }}>
            {tasksError.message}
          </div>
        )}

        {orderRef && (
          <DataTable<Task>
            rowKey={(t) => t.id}
            rows={tasks ?? []}
            loading={tasksLoading}
            columns={[
              { key: "id", header: "Task ID", render: (t) => t.id },
              { key: "type", header: "Type", render: (t) => t.type },
              {
                key: "status",
                header: "Status",
                render: (t) => <StatusPill status={t.status} size="sm" />,
              },
              {
                key: "station",
                header: "Station",
                render: (t) => t.leaseStationId ?? "—",
              },
              {
                key: "hints",
                header: "Hints",
                render: (t) => (
                  <span style={{ display: "flex", gap: "var(--wh-space-2)" }}>
                    {t.fragile && <StatusPill status="Fragile" tone="warning" size="sm" />}
                    {t.giftWrap && <StatusPill status="Gift wrap" tone="neutral" size="sm" />}
                    {!t.fragile && !t.giftWrap && "—"}
                  </span>
                ),
              },
            ]}
          />
        )}

        {!orderRef && (
          <div
            style={{
              color: "var(--wh-color-text-muted)",
              fontSize: "var(--wh-font-size-sm)",
            }}
          >
            Enter an order reference to find its pick/pack/SLAM tasks.
          </div>
        )}
      </Card>
    </div>
  );
}
