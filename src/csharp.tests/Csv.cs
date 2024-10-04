using CsCheck;
using FluentAssertions;
using LanguageExt;
using System;
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
    public async Task Writing_then_reading_returns_the_original_without_blank_lines()
    {
        var generator = Generator.CsvRows;

        await generator.SampleAsync(async rows =>
        {
            // Arrange
            var ignoreBlankLines = true;
            var cancellationToken = CancellationToken.None;

            // Act
            var binaryData = await CsvModule.WriteRows(rows, cancellationToken);
            var readRows = await CsvModule.GetRows(binaryData, ignoreBlankLines: ignoreBlankLines)
                                          .ToImmutableArray(cancellationToken);

            // Assert
            var rowIsNotBlank = (CsvRow row) => row.Columns.Values.Any(value => string.IsNullOrWhiteSpace(value) is false);
            var expectedRows = rows.Where(rowIsNotBlank);
            readRows.Should().HaveSameCount(expectedRows);
        });
    }

    [Fact]
    public async Task Writing_then_reading_returns_the_original()
    {
        var generator = Generator.CsvRows;
        await generator.SampleAsync(async rows =>
        {
            // Arrange
            var ignoreBlankLines = false;
            var cancellationToken = CancellationToken.None;

            // Act
            var binaryData = await CsvModule.WriteRows(rows, cancellationToken);
            var readRows = await CsvModule.GetRows(binaryData, ignoreBlankLines: ignoreBlankLines)
                                          .ToImmutableArray(cancellationToken);

            // Assert
            readRows.Should().HaveSameCount(rows);
        });
    }

    [Fact]
    public async Task Can_extract_header_dictionary()
    {
        var generator = from row in Generator.CsvRow
                        where RowHasDistinctNonEmptyValues(row)
                        select row;

        await generator.SampleAsync(async row =>
        {
            // Act
            var data = await CsvModule.WriteRows([row], CancellationToken.None);
            var headerDictionary = await CsvModule.GetHeaderDictionary(data);

            // Assert
            var expectedColumnNames = row.Columns
                                         .Values
                                         .Where(value => string.IsNullOrWhiteSpace(value) is false)
                                         .Select(CsvColumnName.FromOrThrow)
                                         .ToImmutableArray();

            var actualColumnNames = headerDictionary.Keys;

            actualColumnNames.Except(expectedColumnNames).Should().BeEmpty();
            expectedColumnNames.Except(actualColumnNames).Should().BeEmpty();
        });
    }

    private static bool RowHasDistinctNonEmptyValues(CsvRow row) =>
        row.Columns
           .Values
           .Where(value => string.IsNullOrWhiteSpace(value) is false)
           .GroupBy(value => value, StringComparer.OrdinalIgnoreCase)
           .All(group => group.Count() == 1);

    [Fact]
    public void Return_none_when_finding_value_by_column_number_and_column_number_does_not_exist()
    {
        var generator = from row in Generator.CsvRow
                        from nonExistingColumnNumber in Generator.ColumnNumber
                        where ColumnNumberExists(row, nonExistingColumnNumber) is false
                        select (row, nonExistingColumnNumber);

        generator.Sample(x =>
        {
            var (row, nonExistingColumnNumber) = x;
            var option = row.FindValue(nonExistingColumnNumber);
            option.Should().BeNone();
        });
    }

    private static bool ColumnNumberExists(CsvRow row, CsvColumnNumber columnNumber) =>
        row.Columns.ContainsKey(columnNumber);

    [Fact]
    public void Return_none_when_finding_value_by_integer_column_number_and_column_number_does_not_exist()
    {
        var generator = from row in Generator.CsvRow
                        from nonExistingColumnNumber in Generator.ColumnNumber
                        where ColumnNumberExists(row, nonExistingColumnNumber) is false
                        select (row, nonExistingColumnNumber.ToInt());

        generator.Sample(x =>
        {
            var (row, nonExistingColumnNumber) = x;
            var option = row.FindValue(nonExistingColumnNumber);
            option.Should().BeNone();
        });
    }

    [Fact]
    public void Can_successfully_find_value_by_column_number()
    {
        var generator = from row in Generator.CsvRow
                        where row.Columns.Count > 0
                        from column in Gen.OneOfConst(row.Columns.ToArray())
                        select (row, column);

        generator.Sample(x =>
        {
            var (row, (columnNumber, columnValue)) = x;
            var option = row.FindValue(columnNumber);
            option.Should().BeSome(columnValue);
        });
    }

    [Fact]
    public void Can_successfully_find_value_by_integer_column_number()
    {
        var generator = from row in Generator.CsvRow
                        where row.Columns.Count > 0
                        from column in Gen.OneOfConst(row.Columns.ToArray())
                        select (row, column);
        generator.Sample(x =>
        {
            // Arrange
            var (row, (columnNumber_, columnValue)) = x;
            var columnNumber = columnNumber_.ToInt();

            // Act
            var option = row.FindValue(columnNumber);

            // Assert
            option.Should().BeSome(columnValue);
        });
    }

    [Fact]
    public void Return_none_when_finding_value_by_column_name_and_column_name_does_not_exist()
    {
        var generator = from row in Generator.CsvRow
                        where RowHasDistinctNonEmptyValues(row)
                        let columnNames = row.Columns.Values
                        let headerDictionary = CsvModule.GetHeaderDictionary(columnNames)
                        from nonExistingColumnName in Generator.CsvColumnName
                        where ColumnNameExists(row, nonExistingColumnName, headerDictionary) is false
                        select (row, nonExistingColumnName, headerDictionary);

        generator.Sample(x =>
        {
            var (row, nonExistingColumnName, headerDictionary) = x;
            var option = row.FindValue(nonExistingColumnName, headerDictionary);
            option.Should().BeNone();
        });
    }

    private static bool ColumnNameExists(CsvRow row, CsvColumnName columnName, IDictionary<CsvColumnName, CsvColumnNumber> headerDictionary) =>
        headerDictionary.Find(columnName)
                        .Where(columnNumber => ColumnNumberExists(row, columnNumber))
                        .IsSome;

    [Fact]
    public void Return_none_when_finding_value_by_string_column_name_and_column_name_does_not_exist()
    {
        var generator = from row in Generator.CsvRow
                        where RowHasDistinctNonEmptyValues(row)
                        let columnNames = row.Columns.Values
                        let headerDictionary = CsvModule.GetHeaderDictionary(columnNames)
                        from nonExistingColumnName in Generator.CsvColumnName
                        where ColumnNameExists(row, nonExistingColumnName, headerDictionary) is false
                        select (row, nonExistingColumnName.ToString());

        generator.Sample(x =>
        {
            var (row, nonExistingColumnName) = x;
            var option = row.FindValue(nonExistingColumnName, new Dictionary<CsvColumnName, CsvColumnNumber>());
            option.Should().BeNone();
        });
    }

    [Fact]
    public void Can_successfully_find_value_by_column_name()
    {
        var generator = from row in Generator.CsvRow
                        where RowHasDistinctNonEmptyValues(row)
                        let columnNames = row.Columns.Values
                        let headerDictionary = CsvModule.GetHeaderDictionary(columnNames)
                        where headerDictionary.Count > 0
                        from columnName in Gen.OneOfConst(headerDictionary.Keys.ToArray())
                        select (row, columnName, headerDictionary);

        generator.Sample(x =>
        {
            var (row, columnName, headerDictionary) = x;
            var option = row.FindValue(columnName, headerDictionary);
            option.Should().BeSome(columnName.ToString());
        });
    }

    [Fact]
    public void Can_successfully_find_value_by_string_column_name()
    {
        var generator = from row in Generator.CsvRow
                        where RowHasDistinctNonEmptyValues(row)
                        let columnNames = row.Columns.Values
                        let headerDictionary = CsvModule.GetHeaderDictionary(columnNames)
                        where headerDictionary.Count > 0
                        from columnName in Gen.OneOfConst(headerDictionary.Keys.ToArray())
                        select (row, columnName.ToString(), headerDictionary);

        generator.Sample(x =>
        {
            var (row, columnName, headerDictionary) = x;
            var option = row.FindValue(columnName, headerDictionary);
            option.Should().BeSome(columnName);
        });
    }
}

file static class Extensions
{
    public static FrozenSet<CsvColumnNumber> GetColumnNumbers(this CsvRow row) =>
        row.Columns
           .Keys
           .ToFrozenSet();
}

file static class Generator
{
    public static Gen<CsvColumnNumber> ColumnNumber { get; } =
        from number in Gen.Int.Positive
        where number > 0
        select CsvColumnNumber.FromOrThrow(number);

    public static Gen<CsvColumnName> CsvColumnName { get; } =
        from name in Gen.String.AlphaNumeric
        where string.IsNullOrWhiteSpace(name) is false
        select csharp.CsvColumnName.FromOrThrow(name);

    public static Gen<FrozenDictionary<CsvColumnNumber, string>> CsvColumnValues { get; } =
        from values in Gen.String.AlphaNumeric.ImmutableArrayOf()
        select values.Select((value, index) => (CsvColumnNumber.FromOrThrow(index + 1), value))
                     .ToFrozenDictionary();

    public static Gen<CsvRow> CsvRow { get; } =
        from rowNumber in Gen.Int.Positive
        where rowNumber > 0
        from columns in CsvColumnValues
        select new CsvRow
        {
            Number = CsvRowNumber.FromOrThrow(rowNumber),
            Columns = columns
        };

    public static Gen<ImmutableArray<CsvRow>> CsvRows { get; } =
        from rows in CsvRow.ImmutableArrayOf()
        select rows.Select((row, index) => row with { Number = CsvRowNumber.FromOrThrow(index + 1) })
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