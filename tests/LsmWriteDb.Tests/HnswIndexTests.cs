using LsmWriteDb.Indexes;

namespace LsmWriteDb.Tests;

public sealed class HnswIndexTests
{
    [Theory]
    [InlineData(0, 16, 200)]
    [InlineData(2, 1, 200)]
    [InlineData(2, 16, 15)]
    public void Constructor_RejectsInvalidConfiguration(int dimensions, int m, int efConstruction)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new HnswIndex(dimensions, m, efConstruction));
    }

    [Fact]
    public void Search_EmptyIndexReturnsNoNeighbors()
    {
        var index = new HnswIndex(2);

        Assert.Empty(index.Search([1, 0], 3));
        Assert.Equal(0, index.Count);
    }

    [Fact]
    public void UpsertSearchAndRemove_MaintainsNearestNeighborAndLiveCount()
    {
        var index = new HnswIndex(2, m: 4, efConstruction: 32, randomSeed: 17);
        index.Upsert("north", [1, 0]);
        index.Upsert("near", [0.9, 0.1]);
        index.Upsert("east", [0, 1]);
        index.Upsert("south", [-1, 0]);

        var closest = Assert.Single(index.Search([1, 0], 1));
        Assert.Equal("north", closest.Id);
        Assert.Equal(0, closest.Distance, precision: 12);
        Assert.Equal(4, index.Count);

        Assert.True(index.Remove("north"));
        Assert.False(index.Remove("north"));
        Assert.Equal(3, index.Count);
        Assert.DoesNotContain(index.Search([1, 0], 4, efSearch: 8), item => item.Id == "north");
    }

    [Fact]
    public void Upsert_ReplacesAnExistingIdentifier()
    {
        var index = new HnswIndex(2, randomSeed: 9);
        index.Upsert("same", [1, 0]);
        index.Upsert("same", [0, 1]);

        Assert.Equal(1, index.Count);
        var result = Assert.Single(index.Search([0, 1], 1));
        Assert.Equal("same", result.Id);
        Assert.Equal(0, result.Distance, precision: 12);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Search_RejectsNonPositiveK(int k)
    {
        var index = new HnswIndex(2);
        Assert.Throws<ArgumentOutOfRangeException>(() => index.Search([1, 0], k));
    }

    [Fact]
    public void InsertAndSearch_RejectWrongDimensionAndNonFiniteComponents()
    {
        var index = new HnswIndex(2);

        Assert.Throws<ArgumentException>(() => index.Upsert("short", [1]));
        Assert.Throws<ArgumentException>(() => index.Upsert("nan", [double.NaN, 0]));
        Assert.Throws<ArgumentException>(() => index.Search([1], 1));
        Assert.Throws<ArgumentException>(() => index.Search([double.PositiveInfinity, 0], 1));
        Assert.Throws<ArgumentException>(() => index.Upsert("", [1, 0]));
        Assert.Equal(0, index.Count);
    }

    [Fact]
    public void Search_HandlesZeroVectorsAndMissingIdentifiers()
    {
        var index = new HnswIndex(2);
        index.Upsert("zero", [0, 0]);

        Assert.Equal(0, Assert.Single(index.Search([0, 0], 1)).Distance);
        Assert.Equal(1, Assert.Single(index.Search([1, 0], 1)).Distance);
        Assert.False(index.Remove("missing"));
    }

    [Fact]
    public void InsertManyVectors_TraversesMultipleLevelsAndPrunesDenseNeighborhoods()
    {
        var index = new HnswIndex(3, m: 2, efConstruction: 8, randomSeed: 31);
        for (var i = 0; i < 120; i++)
        {
            var angle = i * (2 * Math.PI / 120);
            index.Upsert($"node-{i}", [Math.Cos(angle), Math.Sin(angle), 0.05 * Math.Sin(3 * angle)]);
        }

        Assert.Equal(120, index.Count);
        var nearest = index.Search([1, 0, 0], 5, efSearch: 120);
        Assert.Equal(5, nearest.Count);
        Assert.Equal("node-0", nearest[0].Id);
        Assert.True(nearest.Zip(nearest.Skip(1), (left, right) => left.Distance <= right.Distance).All(value => value));
    }

    [Fact]
    public void InsertCrowdedVectorCloud_KeepsIndexSearchableAfterNeighborPruning()
    {
        var index = new HnswIndex(8, m: 2, efConstruction: 64, randomSeed: 3);
        var random = new Random(3);
        for (var i = 0; i < 500; i++)
        {
            var vector = Enumerable.Range(0, 8).Select(_ => random.NextDouble() * 2 - 1).ToArray();
            index.Upsert($"cloud-{i}", vector);
        }

        Assert.Equal(500, index.Count);
        Assert.Equal(10, index.Search(Enumerable.Repeat(0.2, 8).ToArray(), 10, efSearch: 64).Count);
    }

    [Fact]
    public void Search_ClampsEfSearchToK()
    {
        var index = new HnswIndex(2, randomSeed: 2);
        index.Upsert("a", [1, 0]);
        index.Upsert("b", [0, 1]);

        Assert.Equal(2, index.Search([1, 0], 2, efSearch: 1).Count);
    }
}
