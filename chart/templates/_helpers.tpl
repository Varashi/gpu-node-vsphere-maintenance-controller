{{/*
Expand the name of the chart.
*/}}
{{- define "vsphere-passthrough-node-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "vsphere-passthrough-node-controller.fullname" -}}
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
{{- define "vsphere-passthrough-node-controller.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "vsphere-passthrough-node-controller.labels" -}}
helm.sh/chart: {{ include "vsphere-passthrough-node-controller.chart" . }}
{{ include "vsphere-passthrough-node-controller.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "vsphere-passthrough-node-controller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vsphere-passthrough-node-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "vsphere-passthrough-node-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "vsphere-passthrough-node-controller.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding vCenter credentials (existing or rendered).
*/}}
{{- define "vsphere-passthrough-node-controller.vcenterSecretName" -}}
{{- if .Values.vcenter.existingSecret -}}
{{- .Values.vcenter.existingSecret -}}
{{- else -}}
{{- printf "%s-vcenter" (include "vsphere-passthrough-node-controller.fullname" .) -}}
{{- end -}}
{{- end }}
