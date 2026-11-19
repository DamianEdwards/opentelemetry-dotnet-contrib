// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Azure.Identity;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using OpenTelemetry.Internal;

namespace OpenTelemetry.Exporter.AzureDataExplorer;

/// <summary>
/// <see cref="IAzureDataExplorerIngestor"/> implementation backed by the Kusto ingest SDK.
/// </summary>
internal sealed class KustoIngestor : IAzureDataExplorerIngestor
{
    internal const string ConnectorName = "OpenTelemetry.Exporter.AzureDataExplorer";

    private static readonly string ConnectorVersion = typeof(KustoIngestor).Assembly.GetPackageVersion();

    private readonly IKustoIngestClient client;

    public KustoIngestor(IKustoIngestClient client, KustoIngestionProperties ingestionProperties)
    {
        this.client = client;
        this.IngestionProperties = ingestionProperties;
    }

    internal KustoIngestionProperties IngestionProperties { get; }

    public static KustoIngestor Create(AzureDataExplorerExporterOptions options, string tableName, string? jsonMappingReference)
    {
        var kcsb = CreateConnectionStringBuilder(options);
        var ingestionProperties = CreateIngestionProperties(options.DatabaseName, tableName, jsonMappingReference);

        // Managed streaming ingestion retries transient errors and falls back
        // to queued ingestion, matching the Collector's managed ingestor.
        var client = options.IngestionType == AzureDataExplorerIngestionType.Managed
            ? KustoIngestFactory.CreateManagedStreamingIngestClient(kcsb)
            : KustoIngestFactory.CreateQueuedIngestClient(kcsb);

        return new KustoIngestor(client, ingestionProperties);
    }

    public async Task IngestAsync(Stream data, CancellationToken cancellationToken)
    {
        var sourceOptions = new StreamSourceOptions
        {
            LeaveOpen = true,
            CompressionType = DataSourceCompressionType.None,
        };

        await this.client.IngestFromStreamAsync(data, this.IngestionProperties, sourceOptions, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose() => this.client.Dispose();

    internal static KustoIngestionProperties CreateIngestionProperties(string databaseName, string tableName, string? jsonMappingReference)
    {
        var properties = new KustoIngestionProperties(databaseName, tableName)
        {
            Format = DataSourceFormat.json,
        };

        if (!string.IsNullOrWhiteSpace(jsonMappingReference))
        {
            properties.IngestionMapping = new IngestionMapping
            {
                IngestionMappingKind = IngestionMappingKind.Json,
                IngestionMappingReference = jsonMappingReference,
            };
        }

        return properties;
    }

    internal static KustoConnectionStringBuilder CreateConnectionStringBuilder(AzureDataExplorerExporterOptions options)
    {
        var kcsb = new KustoConnectionStringBuilder(options.ClusterUri!.OriginalString.Trim().TrimEnd('/'));
        var isManagedIdentity = !string.IsNullOrWhiteSpace(options.ManagedIdentityId);

        // Precedence mirrors the OpenTelemetry Collector exporter, with the
        // .NET-specific TokenCredential option taking priority.
        if (options.TokenCredential != null)
        {
            kcsb = kcsb.WithAadAzureTokenCredentialsAuthentication(options.TokenCredential);
        }
        else if (options.UseAzureAuth)
        {
            kcsb = kcsb.WithAadAzureTokenCredentialsAuthentication(new DefaultAzureCredential());
        }
        else if (!isManagedIdentity)
        {
            kcsb = kcsb.WithAadApplicationKeyAuthentication(options.ApplicationId, options.ApplicationKey, options.TenantId);
        }
        else if (AzureDataExplorerExporterOptions.IsSystemManagedIdentity(options.ManagedIdentityId))
        {
            kcsb = kcsb.WithAadSystemManagedIdentity();
        }
        else
        {
            kcsb = kcsb.WithAadUserManagedIdentity(options.ManagedIdentityId!.Trim());
        }

        kcsb.SetConnectorDetails(
            ConnectorName,
            ConnectorVersion,
            additional: [("isManagedIdentity", isManagedIdentity ? "true" : "false")]);

        return kcsb;
    }
}
