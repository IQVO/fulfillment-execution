import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';
import restApiSidebar from './docs/api-reference/rest/sidebar';

/**
 * The six top-level categories are shared by every warehouse-systems docs
 * site so the five services read as one family. Order matters and is fixed:
 * Overview -> Business Context -> Domain-Driven Design -> API Reference ->
 * Ecosystem -> Architecture Decision Records.
 */
const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Overview',
      collapsed: false,
      link: {type: 'doc', id: 'overview/index'},
      items: [
        'overview/task-lifecycle',
        'overview/architecture',
        'overview/running-locally',
      ],
    },
    {
      type: 'category',
      label: 'Business Context',
      collapsed: false,
      items: [
        'business-context/domain-vision',
        'business-context/process-paths',
        'business-context/why-pull-not-push',
        'business-context/ubiquitous-language',
      ],
    },
    {
      type: 'category',
      label: 'Domain-Driven Design',
      collapsed: false,
      items: [
        'ddd/subdomain-classification',
        'ddd/aggregates-and-invariants',
        'ddd/domain-events',
        'ddd/use-cases',
        'ddd/context-relationships',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      collapsed: false,
      link: {type: 'doc', id: 'api-reference/index'},
      items: [
        {
          type: 'category',
          label: 'REST API (generated from apis/openapi.yaml)',
          items: restApiSidebar,
        },
        'api-reference/events',
      ],
    },
    {
      type: 'category',
      label: 'Ecosystem',
      collapsed: false,
      items: ['ecosystem/context-map', 'ecosystem/integration-contracts'],
    },
    {
      type: 'category',
      label: 'Analytics',
      collapsed: false,
      items: ['analytics/throughput-report'],
    },
    {
      type: 'category',
      label: 'AI Ecosystem (MCP)',
      collapsed: false,
      items: ['mcp/governance-charter'],
    },
    {
      type: 'category',
      label: 'Architecture Decision Records (ADR)',
      collapsed: false,
      link: {type: 'doc', id: 'adr/index'},
      items: [
        'adr/0001-hexagonal-ports-and-adapters',
        'adr/0002-pull-based-claimnext-dispatch',
        'adr/0003-lease-based-at-most-once-claiming',
        'adr/0004-kafka-integration-events-and-envelope',
        'adr/0005-rfc-7807-problem-details',
        'adr/0006-arch-go-architecture-fitness-tests',
        'adr/0007-godog-bdd-acceptance-tests',
        'adr/0008-mcp-inbound-adapter',
        'adr/0009-fragile-and-hazmat-handling-flags',
        'adr/0010-package-segregation-and-sort-lane',
        'adr/0011-gift-wrap-handling-flag',
        'adr/0012-analytical-data-product',
        'adr/0013-fulfillment-mfe-console-adoption',
        'adr/0014-labor-performance-integration-hooks',
        'adr/0015-wcs-equipment-anti-corruption-seam',
        'adr/0016-rebin-and-order-consolidation',
        'adr/0017-process-path-catalogue-as-configuration',
        'adr/0018-installed-capacity-read-endpoint',
        'adr/0019-standard-metrics-convention',
        'adr/0020-transactional-outbox',
        'adr/0021-rest-identity-static-bearer-scopes',
        'adr/0022-remove-rest-mcp-auth',
        'adr/0023-task-type-on-wire',
        'adr/0024-station-location-code-and-workcenter-role-check',
        'adr/0025-cpt-missed-sweep-and-package-manifested',
        'adr/0026-on-time-to-cpt-kpi',
        'adr/0027-cloudevents-envelope-migration',
        'adr/0028-idempotency-key-middleware',
        'adr/0029-resilience-circuit-breakers-retry-dlq-shutdown',
        'adr/0030-horizontal-autoscaling-and-pgxpool-tuning',
        'adr/0031-migrations-direct-postgres-connection',
        'adr/0032-cloudevents-mandatory-envelope',
        'adr/0033-package-read-model',
      ],
    },
  ],
};

export default sidebars;
