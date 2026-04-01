using ByTech.EmbeddedCommitLog.Pipeline;
using ByTech.EmbeddedCommitLog.Records;

namespace ByTech.EmbeddedCommitLog.Consumer;

/// <summary>
/// Routes records to sink lanes whose registered <see cref="ContentType"/> filter matches
/// the record's <see cref="RecordHeader.ContentType"/>. Records that do not match any
/// registered lane filter are dropped and the <paramref name="onDropped"/> callback is
/// invoked with sink name <c>"*"</c>.
/// </summary>
/// <remarks>
/// <para>
/// Use <see cref="RegisterFilter"/> (called by
/// <c>Pipeline.AddSink(consumerName, sinkName, sink, ContentType)</c>) to associate each
/// lane with its content-type filter before the pipeline starts.
/// </para>
/// <para>
/// Lanes registered without an explicit filter (i.e. whose name is not in the filter map)
/// receive every record regardless of content type.
/// </para>
/// </remarks>
internal sealed class ContentTypeRouter(BackpressurePolicy policy, Action<string> onDropped) : IRecordRouter
{
    private readonly Dictionary<string, ContentType> _filters = new(StringComparer.Ordinal);

    /// <summary>
    /// Associates <paramref name="sinkName"/> with <paramref name="contentType"/> so that
    /// only records carrying that content type are forwarded to the corresponding lane.
    /// </summary>
    /// <param name="sinkName">The unique name of the sink lane to filter.</param>
    /// <param name="contentType">The content type that records must carry to be routed to this lane.</param>
    internal void RegisterFilter(string sinkName, ContentType contentType)
        => _filters[sinkName] = contentType;

    /// <inheritdoc/>
    public async ValueTask RouteAsync(LogRecord record, IReadOnlyList<SinkLane> lanes, CancellationToken ct)
    {
        bool routed = false;

        foreach (SinkLane lane in lanes)
        {
            if (!_filters.TryGetValue(lane.SinkName, out ContentType filter)
                || filter == record.Header.ContentType)
            {
                if (policy == BackpressurePolicy.Drop)
                {
                    if (!lane.TryWrite(record))
                    {
                        onDropped(lane.SinkName);
                    }
                }
                else if (policy == BackpressurePolicy.Spill)
                {
                    if (lane.HasSpill || !lane.TryWrite(record))
                    {
                        lane.Spill(record);
                    }
                }
                else
                {
                    await lane.WriteAsync(record, ct);
                }

                routed = true;
            }
        }

        if (!routed)
        {
            onDropped("*");
        }
    }
}
