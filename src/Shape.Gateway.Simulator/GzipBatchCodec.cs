using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>{ "Entries": [...] } → gzip (CompressionLevel.Optimal), as GzipCompressor does in the module.</summary>
public static class GzipBatchCodec
{
    public static byte[] Encode(IReadOnlyList<GatewayEntry> entries)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(new { Entries = entries });
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(json);
        return output.ToArray();
    }

    public static string DecodeJson(byte[] gzipBytes)
    {
        using var gzip = new GZipStream(new MemoryStream(gzipBytes), CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }
}
