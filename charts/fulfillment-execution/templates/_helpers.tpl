{{- define "fulfillment-execution.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "fulfillment-execution.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "fulfillment-execution.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "fulfillment-execution.labels" -}}
helm.sh/chart: {{ include "fulfillment-execution.chart" . }}
{{ include "fulfillment-execution.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "fulfillment-execution.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fulfillment-execution.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "fulfillment-execution.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "fulfillment-execution.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "fulfillment-execution.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "fulfillment-execution.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Postgres is the only runtime (ADR 0052): the api and mcp binaries refuse to boot
without DATABASE_URL, so the chart refuses to render without a source for it.
*/}}
{{- define "fulfillment-execution.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "database.url or database.existingSecret is required: DATABASE_URL is mandatory, this service runs only on a real Postgres" -}}
{{- end -}}
{{- end }}

{{- define "fulfillment-execution.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "fulfillment-execution.fullname" . }}-analytics-database
{{- end }}
{{- end }}

{{- define "fulfillment-execution.projectorFullname" -}}
{{- printf "%s-projector" (include "fulfillment-execution.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "fulfillment-execution.reportsFullname" -}}
{{- printf "%s-reports" (include "fulfillment-execution.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "fulfillment-execution.mcpFullname" -}}
{{- printf "%s-mcp" (include "fulfillment-execution.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}


{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote is served by its own nginx pod and reached through warehouse-infra's
Nginx web gateway at /mfes/fulfillment-execution/. It is deliberately a separate
workload from the API: Kong never routes to it, and the OLTP Service must never
select it.
*/}}
{{- define "fulfillment-execution.frontendFullname" -}}
{{- include "fulfillment-execution.fullname" . }}-frontend
{{- end }}

{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "fulfillment-execution.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "fulfillment-execution.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end -}}
