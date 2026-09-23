using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Contracts;

public static class ShapeJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}
