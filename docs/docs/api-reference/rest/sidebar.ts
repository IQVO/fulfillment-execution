import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/fulfillment-execution-api",
    },
    {
      type: "category",
      label: "Tasks",
      link: {
        type: "doc",
        id: "api-reference/rest/tasks",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/create-task",
          label: "Create a task and place it in the pool",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-tasks-by-order-ref",
          label: "Look up every task recorded for an order reference",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/claim-next-task",
          label: "PULL-dispatch the best-fit pending task to this station",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/renew-lease",
          label: "Extend the active lease held by the claiming station",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/complete-task",
          label: "Complete a task on behalf of the station that claimed it",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/seal-package",
          label: "Scan contents and seal a Package for a Pack task's order",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-queue-depth",
          label: "Read the pending-task count for a process path (read model)",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/expire-leases",
          label: "Sweep every Claimed task whose lease has expired back to Pending",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/sweep-cpt-misses",
          label: "Sweep every still-open task whose CPT has passed and report it as missed",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Stations",
      link: {
        type: "doc",
        id: "api-reference/rest/stations",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/register-station",
          label: "Register or re-register a station",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/claim-next-task",
          label: "PULL-dispatch the best-fit pending task to this station",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/check-in-station",
          label: "Check a worker or robot into a station",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/check-out-station",
          label: "Check the current occupant out of a station",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-installed-capacity",
          label: "Read how many registered stations hold a given capability (read model)",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Packages",
      link: {
        type: "doc",
        id: "api-reference/rest/packages",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/seal-package",
          label: "Scan contents and seal a Package for a Pack task's order",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/run-slam",
          label: "Run the SLAM weigh-check on a sealed package",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Rebin",
      link: {
        type: "doc",
        id: "api-reference/rest/rebin",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/arrive-at-rebin",
          label: "Record one picked line arriving at the order's Rebin consolidation slot",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "System",
      link: {
        type: "doc",
        id: "api-reference/rest/system",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-healthz",
          label: "Liveness check",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-readyz",
          label: "Readiness check",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Schemas",
      items: [
        {
          type: "doc",
          id: "api-reference/rest/schemas/problem",
          label: "Problem",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/tasktype",
          label: "TaskType",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/taskstatus",
          label: "TaskStatus",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/packagestatus",
          label: "PackageStatus",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/sortlane",
          label: "SortLane",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/createtaskrequest",
          label: "CreateTaskRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/taskresponse",
          label: "TaskResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/claimnextrequest",
          label: "ClaimNextRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/renewleaserequest",
          label: "RenewLeaseRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/completetaskrequest",
          label: "CompleteTaskRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/sealpackagerequest",
          label: "SealPackageRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/packageresponse",
          label: "PackageResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/runslamrequest",
          label: "RunSlamRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/queuedepthresponse",
          label: "QueueDepthResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/installedcapacityresponse",
          label: "InstalledCapacityResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/expireleasesresponse",
          label: "ExpireLeasesResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/sweepcptmissesresponse",
          label: "SweepCPTMissesResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/registerstationrequest",
          label: "RegisterStationRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/stationresponse",
          label: "StationResponse",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/checkinstationrequest",
          label: "CheckInStationRequest",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest/schemas/arriveatrebinrequest",
          label: "ArriveAtRebinRequest",
          className: "schema",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
