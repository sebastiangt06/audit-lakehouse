using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Makers.Core.Api.Infrastructure.Outbox;

/// <summary>
/// Event contract for money: decimals travel as JSON strings with their exact digits and scale
/// ("amount":"12345678.1234"), never as JSON numbers. Most consumers (Jackson in Spark/Glue,
/// JavaScript, many BI tools) read JSON numbers as IEEE-754 doubles, which silently turns
/// 12345678.1234 into 1.23456781234E7 and loses digits beyond ~15-17 significant figures.
/// Scoped to decimal only (not JsonNumberHandling.WriteAsString), so ints such as schemaVersion
/// stay numbers.
/// </summary>
public sealed class DecimalAsStringJsonConverter : JsonConverter<decimal>
{
    public override decimal Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            throw new JsonException("Decimal amounts must be JSON strings (event contract).");
        }

        return decimal.Parse(reader.GetString()!, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture);
    }

    // decimal.ToString keeps the scale: 150000.5000m -> "150000.5000". Invariant: '.' separator.
    public override void Write(Utf8JsonWriter writer, decimal value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.ToString(CultureInfo.InvariantCulture));
}
