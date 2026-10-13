import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest-reports/fulfillment-execution-reports-api",
    },
    {
      type: "category",
      label: "Reports",
      link: {
        type: "doc",
        id: "api-reference/rest-reports/reports",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest-reports/get-throughput-report",
          label: "Throughput & Lease-Health report, keyed by task type, station and UTC hour",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest-reports/get-throughput-freshness",
          label: "How far the read model trails the event stream",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "System",
      link: {
        type: "doc",
        id: "api-reference/rest-reports/system",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest-reports/get-reports-healthz",
          label: "Liveness probe",
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
          id: "api-reference/rest-reports/schemas/throughputreport",
          label: "ThroughputReport",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest-reports/schemas/throughputrow",
          label: "ThroughputRow",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest-reports/schemas/freshness",
          label: "Freshness",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest-reports/schemas/status",
          label: "Status",
          className: "schema",
        },
        {
          type: "doc",
          id: "api-reference/rest-reports/schemas/problem",
          label: "Problem",
          className: "schema",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
