using CsCheck;
using FluentAssertions;
using LanguageExt;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace csharp.tests;

public class CsvModuleTests
{
    [Fact]
    public async Task Can_reverse_reading_and_writing()
    {
        var generator = from ignoreBlankLines in Gen.Bool
                        from rows in Generator.CsvRows
                        select (ignoreBlankLines, rows);

        await generator.SampleAsync(async x =>
        {
            // Arrange
            var (ignoreBlankLines, rows) = x;
            var cancellationToken = CancellationToken.None;

            // Act
            var binaryData = await CsvModule.WriteRows(rows, cancellationToken);
            var readRows = await CsvModule.GetRows(binaryData, ignoreBlankLines: ignoreBlankLines)
                                          .ToImmutableArray(cancellationToken);

            // Assert
            if (ignoreBlankLines)
            {
                var rowsWithData = rows.Where(row => row.Columns.Values.Any(value => string.IsNullOrWhiteSpace(value) is false));
                rowsWithData.Should().HaveSameCount(readRows);
            }
            else
            {
                rows.Should().HaveSameCount(readRows);
            }
        });
    }

    [Fact]
    public async Task Can_extract_header_dictionary()
    {
        var generator = Generator.CsvRow;

        await generator.SampleAsync(async row =>
        {
            // Act
            var data = await CsvModule.WriteRows([row], CancellationToken.None);
            var headerDictionary = await CsvModule.GetHeaderDictionary(data);

            // Assert
            var expectedColumnNames = row.Columns
                                         .Values
                                         .Where(value => string.IsNullOrWhiteSpace(value) is false)
                                         .Select(value => CsvColumnName.From(value).ThrowIfFail())
                                         .ToImmutableArray();

            var actualColumnNames = headerDictionary.Values;

            actualColumnNames.Except(expectedColumnNames).Should().BeEmpty();
            expectedColumnNames.Except(actualColumnNames).Should().BeEmpty();
        });
    }
}

file static class Generator
{
    public static Gen<CsvColumnName> CsvColumnName { get; } =
        from name in Gen.String.AlphaNumeric
        where string.IsNullOrWhiteSpace(name) is false
        select csharp.CsvColumnName.From(name).ThrowIfFail();

    public static Gen<FrozenDictionary<CsvColumnNumber, string>> CsvColumnValues { get; } =
        from values in Gen.String.AlphaNumeric.ImmutableArrayOf()
        select values.Select((value, index) => (CsvColumnNumber.From(index + 1).ThrowIfFail(), value))
                     .ToFrozenDictionary();

    public static Gen<CsvRow> CsvRow { get; } =
        from rowNumber in Gen.Int.Positive
        where rowNumber > 0
        from columns in CsvColumnValues
        select new CsvRow
        {
            Number = CsvRowNumber.From(rowNumber).ThrowIfFail(),
            Columns = columns
        };

    public static Gen<ImmutableArray<CsvRow>> CsvRows { get; } =
        from rows in CsvRow.ImmutableArrayOf()
        select rows.Select((row, index) => row with { Number = CsvRowNumber.From(index + 1).ThrowIfFail() })
                   .ToImmutableArray();

    public static Gen<Option<T>> OptionOf<T>(this Gen<T> gen) =>
        Gen.Frequency((1, Gen.Const(Option<T>.None)),
                      (4, gen.Select(Option<T>.Some)));

    public static Gen<ImmutableArray<T>> ImmutableArrayOf<T>(this Gen<T> gen) =>
        gen.List.Select(list => list.ToImmutableArray());

    public static Gen<FrozenSet<T>> FrozenSetOf<T>(this Gen<T> gen, IEqualityComparer<T>? comparer = default) =>
        gen.List.Select(list => list.ToFrozenSet(comparer));

    public static Gen<ImmutableArray<T>> SubImmutableArrayOf<T>(ICollection<T> collection) =>
        collection.Count == 0
        ? Gen.Const(ImmutableArray<T>.Empty)
        : from items in Gen.Shuffle(collection.ToArray(), collection.Count)
          select items.ToImmutableArray();

    public static Gen<ImmutableArray<T>> SequenceToImmutableArray<T>(this IEnumerable<Gen<T>> gens) =>
        from list in gens.Aggregate(Gen.Const(ImmutableArray<T>.Empty),
                                    (accumulate, gen) => from list in accumulate
                                                         from t in gen
                                                         select list.Add(t))
        select list;
}