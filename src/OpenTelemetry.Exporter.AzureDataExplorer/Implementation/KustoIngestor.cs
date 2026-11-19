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

        return new KustoIngestor(CreateClient(kcsb, options.IngestionType), ingestionProperties);
    }

    public async Task IngestAsync(Stream data, CancellationToken cancellationToken)
    {
        // The managed streaming client rejects LeaveOpen and disposes the source
        // stream, so hand each attempt its own view over the caller's buffer.
#pragma warning disable CA2000 // Dispose objects before losing scope
        var source = CreateSourceStream(data);
#pragma warning restore CA2000 // Dispose objects before losing scope

        var sourceOptions = new StreamSourceOptions
        {
            CompressionType = DataSourceCompressionType.None,
        };

        await this.client.IngestFromStreamAsync(source, this.IngestionProperties, sourceOptions, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose() => this.client.Dispose();

    internal static IKustoIngestClient CreateClient(KustoConnectionStringBuilder kcsb, AzureDataExplorerIngestionType ingestionType)
    {
        // Managed streaming ingestion retries transient errors and falls back
        // to queued ingestion, matching the Collector's managed ingestor.
        return ingestionType == AzureDataExplorerIngestionType.Managed
            ? KustoIngestFactory.CreateManagedStreamingIngestClient(kcsb)
            : KustoIngestFactory.CreateQueuedIngestClient(kcsb);
    }

    internal static Stream CreateSourceStream(Stream data)
    {
        if (data is MemoryStream memoryStream && memoryStream.TryGetBuffer(out var buffer))
        {
            var position = (int)memoryStream.Position;
            return new MemoryStream(buffer.Array!, buffer.Offset + position, buffer.Count - position, writable: false);
        }

        var copy = new MemoryStream();
        data.CopyTo(copy);
        copy.Position = 0;
        return copy;
    }

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
