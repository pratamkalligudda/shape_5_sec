namespace Shape.Gateway.Simulator;

/// <summary>Current event register values, settable through the scenario API. Thread-safe.</summary>
public sealed class EventState
{
    private readonly object _gate = new();
    private Dictionary<string, int> _values = EventParameters.Initial();

    public IReadOnlyDictionary<string, int> Snapshot()
    {
        lock (_gate) return new Dictionary<string, int>(_values);
    }

    public bool TrySet(IReadOnlyDictionary<string, int> updates, out IReadOnlyList<string> unknown)
    {
        unknown = updates.Keys.Where(k => !EventParameters.All.Contains(k)).ToList();
        if (unknown.Count > 0) return false;
        lock (_gate)
            foreach (var (k, v) in updates) _values[k] = v;
        return true;
    }

    public void Reset()
    {
        lock (_gate) _values = EventParameters.Initial();
    }
}
