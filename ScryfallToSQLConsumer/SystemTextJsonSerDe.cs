namespace ScryfallToSQLConsumer;

using System;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

public class SystemTextJsonSerDe : ISerializer<object?>, IDeserializer<object?>
{
    public SystemTextJsonSerDe(JsonSerializerOptions defaultOptions) => DefaultOptions = defaultOptions;

    JsonSerializerOptions DefaultOptions { get; }

    public byte[] Serialize(object? data, SerializationContext context)
        => JsonSerializer.SerializeToUtf8Bytes(data, GetMessageType(context), DefaultOptions);

    public object? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => JsonSerializer.Deserialize(data, GetMessageType(context), DefaultOptions);

    static Type GetMessageType(SerializationContext context)
        => Type.GetType(Encoding.UTF8.GetString(context.Headers[0].GetValueBytes()))!;
}