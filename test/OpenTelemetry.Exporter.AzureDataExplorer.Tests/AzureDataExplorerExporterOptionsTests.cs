// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Azure.Identity;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;

namespace OpenTelemetry.Exporter.AzureDataExplorer.Tests;

public class AzureDataExplorerExporterOptionsTests
{
    private static readonly Uri ClusterUri = new("https://mycluster.kusto.windows.net");

    [Fact]
    public void Defaults_MatchCollectorExporter()
    {
        var options = new AzureDataExplorerExporterOptions();

        Assert.Equal("oteldb", options.DatabaseName);
        Assert.Equal("OTELMetrics", options.MetricsTableName);
        Assert.Equal("OTELLogs", options.LogsTableName);
        Assert.Equal("OTELTraces", options.TracesTableName);
        Assert.Equal(AzureDataExplorerIngestionType.Queued, options.IngestionType);
    }

    [Fact]
    public void Validate_RequiresClusterUri()
    {
        var options = new AzureDataExplorerExporterOptions { UseAzureAuth = true };
        Assert.Throws<InvalidOperationException>(options.Validate);

        options.ClusterUri = new Uri("/relative", UriKind.Relative);
        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    [Theory]
    [InlineData(null, null, null, null, false, false)]
    [InlineData("app", "key", null, null, false, false)]
    [InlineData("app", "key", "tenant", "system", false, false)]
    [InlineData(null, null, null, "system", true, false)]
    [InlineData(null, null, null, null, true, true)]
    [InlineData("app", "key", "tenant", null, false, true)]
    [InlineData(null, null, null, "not-a-guid", false, false)]
    [InlineData("app", "key", "tenant", null, false, false, true)]
    [InlineData(null, null, null, "system", false, true)]
    [InlineData(null, null, null, " SYSTEM ", false, true)]
    [InlineData(null, null, null, "8a3f0c3d-6fd8-4a52-a6b1-3a5cf53ac1d4", false, true)]
    [InlineData(null, null, null, null, false, true, true)]
    public void Validate_RequiresExactlyOneValidAuthenticationMethod(
        string? applicationId,
        string? applicationKey,
        string? tenantId,
        string? managedIdentityId,
        bool useAzureAuth,
        bool valid,
        bool useTokenCredential = false)
    {
        var options = new AzureDataExplorerExporterOptions
        {
            ClusterUri = ClusterUri,
            ApplicationId = applicationId,
            ApplicationKey = applicationKey,
            TenantId = tenantId,
            ManagedIdentityId = managedIdentityId,
            UseAzureAuth = useAzureAuth,
            TokenCredential = useTokenCredential ? new DefaultAzureCredential() : null,
        };

        if (valid)
        {
            options.Validate();
        }
        else
        {
            Assert.Throws<InvalidOperationException>(options.Validate);
        }
    }

    [Fact]
    public void Validate_RejectsInvalidIngestionTypeAndDatabase()
    {
        var options = new AzureDataExplorerExporterOptions
        {
            ClusterUri = ClusterUri,
            UseAzureAuth = true,
            IngestionType = (AzureDataExplorerIngestionType)42,
        };
        Assert.Throws<InvalidOperationException>(options.Validate);

        options.IngestionType = AzureDataExplorerIngestionType.Managed;
        options.DatabaseName = " ";
        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    [Fact]
    public void ExportTimeout_RejectsNonPositiveValues()
    {
        var options = new AzureDataExplorerExporterOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.ExportTimeout = TimeSpan.Zero);
        options.ExportTimeout = Timeout.InfiniteTimeSpan;
        Assert.Equal(Timeout.InfiniteTimeSpan, options.ExportTimeout);
    }

    [Fact]
    public void Exporter_RequiresTableName()
    {
        var ingestor = new TestIngestor();
        var options = TestIngestor.CreateOptions(ingestor);
        options.TracesTableName = string.Empty;

        Assert.Throws<InvalidOperationException>(() => new AzureDataExplorerTraceExporter(options));
    }

    [Fact]
    public void ConnectionString_ApplicationKey()
    {
        var kcsb = KustoIngestor.CreateConnectionStringBuilder(new AzureDataExplorerExporterOptions
        {
            ClusterUri = new Uri("https://mycluster.kusto.windows.net/"),
            ApplicationId = "app",
            ApplicationKey = "key",
            TenantId = "tenant",
        });

        Assert.Equal("https://mycluster.kusto.windows.net", kcsb.DataSource);
        Assert.Equal("app", kcsb.ApplicationClientId);
        Assert.Equal("key", kcsb.ApplicationKey);
        Assert.Equal("tenant", kcsb.Authority);
        Assert.True(kcsb.FederatedSecurity);
        Assert.Contains("isManagedIdentity:{false}", kcsb.ApplicationNameForTracing, StringComparison.Ordinal);
        Assert.StartsWith("Kusto.OpenTelemetry.Exporter.AzureDataExplorer:", kcsb.ApplicationNameForTracing, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("system", "system")]
    [InlineData("8a3f0c3d-6fd8-4a52-a6b1-3a5cf53ac1d4", "8a3f0c3d-6fd8-4a52-a6b1-3a5cf53ac1d4")]
    public void ConnectionString_ManagedIdentity(string managedIdentityId, string expected)
    {
        var kcsb = KustoIngestor.CreateConnectionStringBuilder(new AzureDataExplorerExporterOptions
        {
            ClusterUri = ClusterUri,
            ManagedIdentityId = managedIdentityId,
        });

        Assert.Equal(expected, kcsb.EmbeddedManagedIdentity);
        Assert.Contains("isManagedIdentity:{true}", kcsb.ApplicationNameForTracing, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionString_TokenCredential()
    {
        var kcsb = KustoIngestor.CreateConnectionStringBuilder(new AzureDataExplorerExporterOptions
        {
            ClusterUri = ClusterUri,
            TokenCredential = new DefaultAzureCredential(),
        });

        Assert.Contains("DefaultAzureCredential", kcsb.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void IngestionProperties_UseJsonFormatAndOptionalMapping()
    {
        var withoutMapping = KustoIngestor.CreateIngestionProperties("db", "table", null);
        Assert.Equal("db", withoutMapping.DatabaseName);
        Assert.Equal("table", withoutMapping.TableName);
        Assert.Equal(DataSourceFormat.json, withoutMapping.Format);
        Assert.Null(withoutMapping.IngestionMapping?.IngestionMappingReference);

        var withMapping = KustoIngestor.CreateIngestionProperties("db", "table", "my_mapping");
        Assert.Equal(IngestionMappingKind.Json, withMapping.IngestionMapping.IngestionMappingKind);
        Assert.Equal("my_mapping", withMapping.IngestionMapping.IngestionMappingReference);
    }

    [Fact]
    public void SourceStream_CanBeDisposedWithoutDisposingExporterBuffer()
    {
        using var buffer = new MemoryStream();
        var payload = System.Text.Encoding.UTF8.GetBytes("{\"a\":1}\n");
        buffer.Write(payload, 0, payload.Length);
        buffer.Position = 0;

        using (var source = KustoIngestor.CreateSourceStream(buffer))
        {
            using var reader = new StreamReader(source);
            Assert.Equal("{\"a\":1}\n", reader.ReadToEnd());
        }

        // The managed streaming client disposes the source stream; the exporter's buffer must survive for retries.
        Assert.True(buffer.CanRead);
        buffer.Position = 0;
        using var retry = KustoIngestor.CreateSourceStream(buffer);
        Assert.Equal(payload.Length, retry.Length);
    }
}
