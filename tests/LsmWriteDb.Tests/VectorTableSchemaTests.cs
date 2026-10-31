using LsmWriteDb.Storage;

namespace LsmWriteDb.Tests;

public sealed class VectorTableSchemaTests
{
    [Fact]
    public void ReadVector_ReturnsDeclaredEmbedding()
    {
        var schema = new VectorTableSchema("items", "embedding", 3);

        Assert.Equal([0.25, -1.5, 3], schema.ReadVector("{\"embedding\":[0.25,-1.5,3],\"text\":\"x\"}"));
    }

    [Theory]
    [InlineData("[]", "numeric array")]
    [InlineData("{\"embedding\":null}", "numeric array")]
    [InlineData("{\"embedding\":[1,2]}", "3 dimensions")]
    [InlineData("{\"embedding\":[1,2,\"3\"]}", "numbers")]
    [InlineData("{\"embedding\":[1,2,1e999]}", "numbers")]
    [InlineData("{bad json", "valid JSON")]
    public void ReadVector_RejectsMalformedOrInvalidEmbeddings(string json, string expectedMessage)
    {
        var schema = new VectorTableSchema("items", "embedding", 3);

        var error = Assert.Throws<ArgumentException>(() => schema.ReadVector(json));
        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("", "embedding", 3)]
    [InlineData("bad-table-name", "embedding", 3)]
    [InlineData("items", "1embedding", 3)]
    [InlineData("items", "embedding-name", 3)]
    [InlineData("items", "embedding", 0)]
    [InlineData("items", "embedding", 65537)]
    public void ValidateDefinition_RejectsInvalidNamesAndDimensions(string table, string column, int dimensions)
    {
        Assert.Throws<ArgumentException>(() => new VectorTableSchema(table, column, dimensions).ValidateDefinition());
    }

    [Fact]
    public void ValidateDefinition_AcceptsMaximumDimension()
    {
        new VectorTableSchema("vectors", "_embedding2", 65_536).ValidateDefinition();
    }
}
