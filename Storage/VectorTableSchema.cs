using System.Text.Json;

namespace LsmWriteDb.Storage;

public sealed record VectorTableSchema(string Table, string EmbeddingColumn, int Dimensions)
{
    public void ValidateDefinition()
    {
        TableNames.Normalize(Table);
        if (string.IsNullOrWhiteSpace(EmbeddingColumn) || !char.IsLetter(EmbeddingColumn[0]) && EmbeddingColumn[0] != '_'
            || EmbeddingColumn.Any(character => !char.IsLetterOrDigit(character) && character != '_'))
            throw new ArgumentException("Vector embedding column name is invalid.");
        if (Dimensions < 1 || Dimensions > 65_536)
            throw new ArgumentException("Vector dimension must be between 1 and 65536.");
    }

    public double[] ReadVector(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty(EmbeddingColumn, out var vector)
                || vector.ValueKind != JsonValueKind.Array)
                throw new ArgumentException($"Vector row must contain an '{EmbeddingColumn}' numeric array.");
            var values = vector.EnumerateArray().Select(value => value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number)
                ? number : throw new ArgumentException("Vector components must be numbers.")).ToArray();
            if (values.Length != Dimensions) throw new ArgumentException($"Expected a vector with {Dimensions} dimensions.");
            if (values.Any(value => !double.IsFinite(value))) throw new ArgumentException("Vector components must be finite numbers.");
            return values;
        }
        catch (JsonException exception)
        {
            throw new ArgumentException($"Vector table values must be valid JSON: {exception.Message}");
        }
    }
}
