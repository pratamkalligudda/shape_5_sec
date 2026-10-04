using System.Globalization;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Row key scheme for one live session partition (09-20 §5.2, 09-30 §6). Duplicated verbatim in the backend; both
/// copies carry the same tests. Timestamps are UTC "O" (7 fractional digits), so lexical order is chronological.
/// </summary>
public static class LiveSessionKeys
{
    public const string HeadRowKey = "!current";
    public const string EventPrefix = "evt";

    public static string Timestamp(DateTimeOffset ts) => ts.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    public static string ChangeRowKey(string parameter, DateTimeOffset ts)
    {
        ValidateParameter(parameter);
        return $"{parameter}|{Timestamp(ts)}";
    }

    /// <summary>[From, To) covering every change point of one parameter; '}' is '|' + 1.</summary>
    public static (string From, string To) ParamRange(string parameter, DateTimeOffset? since = null)
    {
        ValidateParameter(parameter);
        return ($"{parameter}|{(since is { } s ? Timestamp(s) : "")}", $"{parameter}}}");
    }

    public static string EventRowKey(string parameter, DateTimeOffset ts)
    {
        ValidateParameter(parameter);
        return $"{EventPrefix}|{Timestamp(ts)}|{parameter}";
    }

    public static (string From, string To) EventRange(DateTimeOffset? since = null) =>
        ($"{EventPrefix}|{(since is { } s ? Timestamp(s) : "")}", $"{EventPrefix}}}");

    public static void ValidateParameter(string parameter)
    {
        if (string.IsNullOrEmpty(parameter) || parameter.Contains('|') || parameter == EventPrefix)
            throw new ArgumentException($"'{parameter}' cannot be used as a live session parameter name.", nameof(parameter));
    }
}
