import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type * as OpenApiPlugin from 'docusaurus-plugin-openapi-docs';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Fulfillment Execution',
  tagline:
    'The Pick / Pack / SLAM task lifecycle — pull-based claimNext dispatch with lease semantics',
  favicon: 'img/favicon.ico',

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  url: 'https://iqvo.github.io',
  baseUrl: '/fulfillment-execution/',

  organizationName: 'IQVO',
  projectName: 'fulfillment-execution',
  deploymentBranch: 'gh-pages',

  onBrokenLinks: 'throw',

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: 'docs',
          editUrl:
            'https://github.com/IQVO/fulfillment-execution/tree/main/docs/',
          docItemComponent: '@theme/ApiItem',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: ['@docusaurus/theme-mermaid', 'docusaurus-theme-openapi-docs'],

  plugins: [
    [
      'docusaurus-plugin-openapi-docs',
      {
        id: 'api',
        docsPluginId: 'classic',
        config: {
          fulfillment: {
            specPath: '../apis/openapi.yaml',
            outputDir: 'docs/api-reference/rest',
            sidebarOptions: {
              groupPathsBy: 'tag',
              categoryLinkSource: 'tag',
            },
            hideSendButton: true,
            showSchemas: true,
          } satisfies OpenApiPlugin.Options,
        },
      },
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
    navbar: {
      title: 'Fulfillment Execution',
      logo: {
        alt: 'Fulfillment Execution',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/api-reference/rest/fulfillment-execution-api',
          label: 'API Reference',
          position: 'left',
        },
        {
          to: '/docs/adr/',
          label: 'ADRs',
          position: 'left',
        },
        {
          href: 'https://github.com/IQVO/fulfillment-execution',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Overview', to: '/docs/overview/'},
            {label: 'Business Context', to: '/docs/business-context/domain-vision'},
            {label: 'Domain-Driven Design', to: '/docs/ddd/subdomain-classification'},
            {label: 'API Reference', to: '/docs/api-reference/'},
          ],
        },
        {
          title: 'Ecosystem',
          items: [
            {label: 'Context Map', to: '/docs/ecosystem/context-map'},
            {
              label: 'wes-work-planning',
              href: 'https://github.com/IQVO/wes-work-planning',
            },
            {
              label: 'inventory-storage',
              href: 'https://github.com/IQVO/inventory-storage',
            },
            {
              label: 'workforce-management',
              href: 'https://github.com/IQVO/workforce-management',
            },
            {
              label: 'facility-layout',
              href: 'https://github.com/IQVO/facility-layout',
            },
          ],
        },
        {
          title: 'Source',
          items: [
            {
              label: 'GitHub repository',
              href: 'https://github.com/IQVO/fulfillment-execution',
            },
            {
              label: 'OpenAPI spec',
              href: 'https://github.com/IQVO/fulfillment-execution/blob/main/apis/openapi.yaml',
            },
            {
              label: 'AsyncAPI spec',
              href: 'https://github.com/IQVO/fulfillment-execution/blob/main/apis/asyncapi.yaml',
            },
            {label: 'Architecture Decision Records', to: '/docs/adr/'},
          ],
        },
      ],
      copyright: `warehouse-systems · Fulfillment Execution — WES-tier Core bounded context. Last built ${new Date().getFullYear()}.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'go', 'json', 'yaml', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
