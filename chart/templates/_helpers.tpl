{{/*
Expand the name of the chart.
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.fullname" -}}
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

{{/*
Chart label.
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.labels" -}}
helm.sh/chart: {{ include "gpu-node-vsphere-maintenance-controller.chart" . }}
{{ include "gpu-node-vsphere-maintenance-controller.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gpu-node-vsphere-maintenance-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gpu-node-vsphere-maintenance-controller.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding vCenter credentials (existing or rendered).
*/}}
{{- define "gpu-node-vsphere-maintenance-controller.vcenterSecretName" -}}
{{- if .Values.vcenter.existingSecret -}}
{{- .Values.vcenter.existingSecret -}}
{{- else -}}
{{- printf "%s-vcenter" (include "gpu-node-vsphere-maintenance-controller.fullname" .) -}}
{{- end -}}
{{- end }}
