# Fulfillment Execution — documentation site

The documentation site for the **Fulfillment Execution** bounded context of
`warehouse-systems`, published at
<https://iqvo.github.io/fulfillment-execution/>.

Content lives in `docs/` (markdown), plus an API reference generated from
the repository's real `apis/openapi.yaml` by `npm run gen-api-docs
fulfillment` and committed (CI's `docs-api-drift` job fails if it is stale).

## Local development

```sh
npm ci
npm start          # dev server with hot reload
npm run build      # production build; fails on broken internal links
npm run typecheck  # TypeScript check
npm run serve      # serve the production build locally
```

## Regenerating the API reference

The pages under `docs/api-reference/rest/` are generated from
`../apis/openapi.yaml` and are committed. Regenerate after a spec change:

```sh
npm run docusaurus clean-api-docs fulfillment
npm run docusaurus gen-api-docs fulfillment
```

## Deployment

Pushing to `main` runs `.github/workflows/docs.yml`, which builds this site and
deploys it to GitHub Pages.
