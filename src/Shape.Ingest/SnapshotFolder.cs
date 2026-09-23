using System.Text.Json;
using Shape.Contracts;

namespace Shape.Ingest;

public enum FoldOutcome { Normal, Gap, Stale, NewSession, Orphan, Invalid }

public sealed record FoldResult(FoldOutcome Outcome, SessionState? NewState, FoldedBatch? Folded);

/// <summary>Pure fold of one batch onto the stored session state (spec §4.2–4.3). Never mutates its inputs.</summary>
public static class SnapshotFolder
{
    public static FoldResult Fold(SessionState? state, TelemetryBatch batch)
    {
        if (!IsValid(batch)) return new(FoldOutcome.Invalid, null, null);

        var props = batch.Properties!;
        var samples = batch.Samples!;
        FoldOutcome outcome;
        bool gap, partial;
        Dictionary<string, JsonElement> snapshot;

        if (state is null)
        {
            var hasBody = samples[0].Body is not null;
            outcome = hasBody ? FoldOutcome.NewSession : FoldOutcome.Orphan;
            gap = partial = !hasBody;
            snapshot = new();
        }
        else
        {
            if (props.BatchSeq <= state.LastBatchSeq) return new(FoldOutcome.Stale, null, null);
            gap = props.BatchSeq > state.LastBatchSeq + 1;
            outcome = gap ? FoldOutcome.Gap : FoldOutcome.Normal;
            partial = state.Partial;
            snapshot = new(state.Snapshot);
        }

        var folded = new List<FoldedSample>(samples.Count);
        foreach (var sample in samples)
        {
            IReadOnlyList<string> changed;
            if (sample.Body is not null)
            {
                snapshot = new(sample.Body);
                changed = sample.Body.Keys.ToList();
            }
            else
            {
                foreach (var (key, value) in sample.Changes!) snapshot[key] = value;
                changed = sample.Changes.Keys.ToList();
            }
            folded.Add(new FoldedSample { Ts = sample.SnapshotTimestampUtc, Values = new(snapshot), Changed = changed });
        }

        var newState = new SessionState
        {
            SessionId = props.SessionId,
            Snapshot = snapshot,
            LastBatchSeq = props.BatchSeq,
            Partial = partial,
            ETag = state?.ETag,
        };
        var result = new FoldedBatch
        {
            SessionId = props.SessionId,
            BatchSeq = props.BatchSeq,
            Gap = gap,
            Partial = partial,
            Samples = folded,
        };
        return new(outcome, newState, result);
    }

    private static bool IsValid(TelemetryBatch batch) =>
        batch.Properties is { SessionId.Length: > 0 }
        && batch.Samples is { Count: > 0 }
        && batch.Samples.All(s => (s.Body is null) != (s.Changes is null));
}
