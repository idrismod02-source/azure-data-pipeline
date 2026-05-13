// Azure Data Pipeline — Infrastructure as Code
// Deploy with: az deployment group create --resource-group rg-data-pipeline --template-file infrastructure/main.bicep

@description('Environment name: dev, staging, or prod')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Project prefix for resource naming')
param projectPrefix string = 'datapipeline'

var suffix        = '${projectPrefix}${environment}'
var storageAcct   = 'st${suffix}'
var adfName       = 'adf-${suffix}'
var databricksWS  = 'dbw-${suffix}'
var synapseWS     = 'syn-${suffix}'
var keyVaultName  = 'kv-${suffix}'
var logAnalytics  = 'law-${suffix}'

// ── Log Analytics Workspace ─────────────────────────────────────────────────
resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: logAnalytics
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// ── Azure Data Lake Storage Gen2 ────────────────────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAcct
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true          // Hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
  }
}

// ADLS containers (zones)
var containers = ['raw', 'bronze', 'silver', 'gold', 'quarantine', 'control']

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource datalakeContainers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for c in containers: {
  parent: blobServices
  name: c
  properties: { publicAccess: 'None' }
}]

// ── Azure Key Vault ──────────────────────────────────────────────────────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
  }
}

// ── Azure Data Factory ───────────────────────────────────────────────────────
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: adfName
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// ADF diagnostic settings → Log Analytics
resource adfDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'adf-diagnostics'
  scope: dataFactory
  properties: {
    workspaceId: logAnalyticsWorkspace.id
    logs: [
      { category: 'PipelineRuns',  enabled: true, retentionPolicy: { days: 30, enabled: true } }
      { category: 'ActivityRuns',  enabled: true, retentionPolicy: { days: 30, enabled: true } }
      { category: 'TriggerRuns',   enabled: true, retentionPolicy: { days: 30, enabled: true } }
    ]
  }
}

// ── Azure Databricks Workspace ───────────────────────────────────────────────
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksWS
  location: location
  sku: { name: environment == 'prod' ? 'premium' : 'standard' }
  properties: {
    managedResourceGroupId: '${subscription().id}/resourceGroups/rg-databricks-managed-${suffix}'
  }
}

// ── Azure Synapse Analytics ──────────────────────────────────────────────────
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: synapseWS
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: storageAccount.properties.primaryEndpoints.dfs
      filesystem: 'gold'
    }
    sqlAdministratorLogin: 'sqladmin'
    sqlAdministratorLoginPassword: '@Microsoft.KeyVault(SecretUri=${keyVault.properties.vaultUri}secrets/synapse-sql-password/)'
  }
}

// ── Role Assignments ─────────────────────────────────────────────────────────

// ADF → Storage: Storage Blob Data Contributor
resource adfStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, dataFactory.id, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ADF → Key Vault: Key Vault Secrets User
resource adfKeyVaultRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, dataFactory.id, '4633458b-17de-408a-b874-0445c86b69e6')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ── Outputs ──────────────────────────────────────────────────────────────────
output storageAccountName   string = storageAccount.name
output dataFactoryName      string = dataFactory.name
output databricksWorkspace  string = databricksWorkspace.name
output synapseWorkspace     string = synapseWorkspace.name
output keyVaultName         string = keyVault.name
output adfPrincipalId       string = dataFactory.identity.principalId
