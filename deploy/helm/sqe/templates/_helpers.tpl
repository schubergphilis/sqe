{{- define "sqe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "sqe.fullname" -}}
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

{{- define "sqe.labels" -}}
app.kubernetes.io/name: {{ include "sqe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "sqe.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sqe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "sqe.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
ISSUE-218: name of the Secret that holds the shared worker secret.
Prefer the operator-managed existingSecret; otherwise the chart-managed
"<fullname>-worker-secret" created from workerSecret.value.
*/}}
{{- define "sqe.workerSecretName" -}}
{{- if .Values.workerSecret.existingSecret -}}
{{- .Values.workerSecret.existingSecret -}}
{{- else -}}
{{- printf "%s-worker-secret" (include "sqe.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
True when a worker secret is configured (either an existing Secret or an
inline value). Renders the env wiring only when there is something to read.
*/}}
{{- define "sqe.workerSecretConfigured" -}}
{{- if or .Values.workerSecret.existingSecret .Values.workerSecret.value -}}true{{- end -}}
{{- end }}

{{/*
Default preferred podAntiAffinity by hostname for a component. Spreads
replicas across nodes without hard-blocking scheduling on small clusters.
Call with a dict: { "ctx": $, "component": "worker" }.
*/}}
{{- define "sqe.defaultAntiAffinity" -}}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            {{- include "sqe.selectorLabels" .ctx | nindent 12 }}
            app.kubernetes.io/component: {{ .component }}
{{- end }}
