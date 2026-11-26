using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Makers.Core.Api.Application.Ports;
using Microsoft.Extensions.Options;

namespace Makers.Core.Api.Infrastructure.Messaging;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:29092";

    public string AuditTopic { get; set; } = "audit.events";

    /// <summary>
    /// Max time a message may wait for a broker ack before failing locally. librdkafka's default is
    /// 300 s; it MUST stay below the outbox lease, or the lease would expire while the message is
    /// still in flight (validated at startup).
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 10_000;
}

/// <summary>
/// Kafka adapter of <see cref="IEventBusPublisher"/>. Idempotent producer (no duplicates from the
/// producer's own internal retries), acks=all, zstd. Message key = AggregateId, so all events of an
/// aggregate land in the same partition and keep their relative order.
/// </summary>
public sealed partial class KafkaEventBusPublisher(IOptions<KafkaOptions> options, ILogger<KafkaEventBusPublisher> logger)
    : IEventBusPublisher, IDisposable
{
    private const string OccurredAtFormat = "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'"; // microseconds = Spark precision

    private readonly KafkaOptions _options = options.Value;
    private readonly Lock _gate = new();
    private IProducer<string, byte[]>? _producer;
    private volatile bool _fatal;

    public async Task<IReadOnlyList<PublishOutcome>> PublishAsync(IReadOnlyList<OutboundEvent> events, CancellationToken cancellationToken)
    {
        var producer = GetProducer();

        // All messages are enqueued before awaiting, so librdkafka batches them (linger.ms) into
        // few produce requests; each task completes with its own delivery report.
        var deliveries = new Task<PublishOutcome>[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            deliveries[i] = DeliverAsync(producer, events[i], cancellationToken);
        }

        return await Task.WhenAll(deliveries);
    }

    public Task EnsureAvailableAsync(CancellationToken cancellationToken)
    {
        var producer = GetProducer();
        return Task.Run(
            () =>
            {
                // A metadata request needs a live broker connection: it fails fast when Kafka is down.
                using var admin = new DependentAdminClientBuilder(producer.Handle).Build();
                try
                {
                    var metadata = admin.GetMetadata(_options.AuditTopic, TimeSpan.FromSeconds(5));
                    if (metadata.Brokers.Count == 0 || metadata.Topics.Any(t => t.Error.IsError))
                    {
                        throw new EventBusUnavailableException($"Kafka topic '{_options.AuditTopic}' is not available.");
                    }
                }
                catch (KafkaException ex)
                {
                    throw new EventBusUnavailableException($"Kafka is not reachable: {ex.Error.Reason}", ex);
                }
            },
            cancellationToken);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _producer?.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
            _producer = null;
        }
    }

    private async Task<PublishOutcome> DeliverAsync(IProducer<string, byte[]> producer, OutboundEvent evt, CancellationToken cancellationToken)
    {
        try
        {
            var message = new Message<string, byte[]>
            {
                Key = evt.AggregateId,
                Value = SerializeEnvelope(evt),
                Headers = new Headers
                {
                    { "event_id", Encoding.UTF8.GetBytes(evt.EventId.ToString()) },
                    { "event_type", Encoding.UTF8.GetBytes(evt.EventType) },
                    { "schema_version", Encoding.UTF8.GetBytes(evt.SchemaVersion.ToString(CultureInfo.InvariantCulture)) },
                },
            };

            await producer.ProduceAsync(_options.AuditTopic, message, cancellationToken);
            return new PublishOutcome(evt.EventId, Delivered: true, Error: null);
        }
        catch (ProduceException<string, byte[]> ex)
        {
            _fatal |= ex.Error.IsFatal;
            return new PublishOutcome(evt.EventId, Delivered: false, $"{ex.Error.Code}: {ex.Error.Reason}");
        }
        catch (JsonException ex)
        {
            // Payload is not valid JSON: a poison message, not an outage.
            return new PublishOutcome(evt.EventId, Delivered: false, $"InvalidPayload: {ex.Message}");
        }
    }

    /// <summary>
    /// Wire contract consumed by the Glue streaming job (step 05): snake_case, same names as the
    /// bronze columns. The payload is embedded as a JSON object (not an escaped string), byte for
    /// byte as stored in the outbox. Inside the payload, money (decimal) is a JSON STRING with its
    /// exact digits ("amount":"12345678.1234"), written by DecimalAsStringJsonConverter when the
    /// outbox row is created: consumers that read JSON numbers as doubles (Spark's from_json with a
    /// StringType payload re-serializes through Jackson) would otherwise corrupt amounts.
    /// </summary>
    private static byte[] SerializeEnvelope(OutboundEvent evt)
    {
        var buffer = new ArrayBufferWriter<byte>(512);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteString("event_id", evt.EventId);
            json.WriteString("event_type", evt.EventType);
            json.WriteString("aggregate_type", evt.AggregateType);
            json.WriteString("aggregate_id", evt.AggregateId);
            json.WriteString("tenant_id", evt.TenantId);
            json.WriteString("actor_id", evt.ActorId);
            json.WriteString("occurred_at", evt.OccurredAt.ToString(OccurredAtFormat, CultureInfo.InvariantCulture));
            json.WriteNumber("schema_version", evt.SchemaVersion);
            json.WritePropertyName("payload");
            json.WriteRawValue(evt.Payload); // validates it is well-formed JSON
            if (evt.TraceId is null)
            {
                json.WriteNull("trace_id");
            }
            else
            {
                json.WriteString("trace_id", evt.TraceId);
            }

            json.WriteEndObject();
        }

        return buffer.WrittenSpan.ToArray();
    }

    private IProducer<string, byte[]> GetProducer()
    {
        lock (_gate)
        {
            if (_producer is not null && _fatal)
            {
                // A fatal error (e.g. idempotence sequence broken) leaves the client unusable:
                // replace it instead of letting the relay die. Unacked rows are retried via lease.
                LogProducerRebuilt(logger);
                _producer.Dispose();
                _producer = null;
                _fatal = false;
            }

            return _producer ??= BuildProducer();
        }
    }

    private IProducer<string, byte[]> BuildProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = "outbox-relay",
            EnableIdempotence = true,
            Acks = Acks.All,
            CompressionType = CompressionType.Zstd,
            LingerMs = 10,
            MessageTimeoutMs = _options.MessageTimeoutMs,
        };

        return new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, error) =>
            {
                // Non-fatal errors (broker down, reconnecting) repeat every few seconds during an
                // outage; the relay already reports the outage once per backoff step, so keep them at Debug.
                _fatal |= error.IsFatal;
                if (error.IsFatal)
                {
                    LogKafkaFatalError(logger, error.Code, error.Reason);
                }
                else
                {
                    LogKafkaError(logger, error.Code, error.Reason);
                }
            })
            .SetLogHandler((_, log) => LogLibrdkafka(logger, log.Facility, log.Message))
            .Build();
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Kafka client error {Code}: {Reason}")]
    private static partial void LogKafkaError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Kafka client FATAL error {Code}: {Reason}")]
    private static partial void LogKafkaFatalError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Debug, Message = "librdkafka [{Facility}] {Message}")]
    private static partial void LogLibrdkafka(ILogger logger, string facility, string message);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka producer hit a fatal error; rebuilding it")]
    private static partial void LogProducerRebuilt(ILogger logger);
}
