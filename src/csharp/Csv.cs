using CsvHelper;
using CsvHelper.Configuration;
using LanguageExt;
using LanguageExt.Common;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace csharp;

/// <summary>
/// Row number in CSV file. First row is 1.
/// </summary>
public sealed record CsvRowNumber : IEquatable<CsvRowNumber>, IComparable<CsvRowNumber>
{
    private readonly uint value;

    private CsvRowNumber(uint value) =>
        this.value = value;

    public static Fin<CsvRowNumber> From(int value) =>
        value > 0
        ? new CsvRowNumber((uint)value)
        : Error.New("Row number must be greater than 0");

    public static CsvRowNumber FromOrThrow(int value) =>
        From(value).ThrowIfFail();

    public int CompareTo(CsvRowNumber? other) =>
        value.CompareTo(other?.value);

    public bool Equals(CsvRowNumber? other) =>
        other is not null && value == other.value;

    public override int GetHashCode() => value.GetHashCode();

    public override string ToString() => value.ToString();

    public static bool operator <(CsvRowNumber left, CsvRowNumber right) =>
        left?.value < right?.value;

    public static bool operator <=(CsvRowNumber left, CsvRowNumber right) =>
        left.value <= right.value;

    public static bool operator >(CsvRowNumber left, CsvRowNumber right) =>
        left.value > right.value;

    public static bool operator >=(CsvRowNumber left, CsvRowNumber right) =>
        left.value >= right.value;
}

/// <summary>
/// Column number in CSV file. First column is 1.
/// </summary>
public sealed record CsvColumnNumber : IEquatable<CsvColumnNumber>, IComparable<CsvColumnNumber>
{
    private readonly uint value;

    private CsvColumnNumber(uint value) =>
        this.value = value;

    public static Fin<CsvColumnNumber> From(int value) =>
        value > 0
        ? new CsvColumnNumber((uint)value)
        : Error.New("Column number must be greater than 0");

    public static CsvColumnNumber FromOrThrow(int value) =>
        From(value).ThrowIfFail();

    public int CompareTo(CsvColumnNumber? other) =>
        value.CompareTo(other?.value);

    public bool Equals(CsvColumnNumber? other) =>
        other is not null && value == other.value;

    public override int GetHashCode() => value.GetHashCode();

    public override string ToString() => value.ToString();

    public int ToInt() => (int)value;

    public static bool operator <(CsvColumnNumber left, CsvColumnNumber right) =>
        left?.value < right?.value;

    public static bool operator <=(CsvColumnNumber left, CsvColumnNumber right) =>
        left.value <= right.value;

    public static bool operator >(CsvColumnNumber left, CsvColumnNumber right) =>
        left.value > right.value;

    public static bool operator >=(CsvColumnNumber left, CsvColumnNumber right) =>
        left.value >= right.value;
}

#pragma warning disable CA1036 // Override methods on comparable types
public sealed class CsvColumnName : IEquatable<CsvColumnName>, IComparable<CsvColumnName>
#pragma warning restore CA1036 // Override methods on comparable types
{
    private readonly string value;

    private CsvColumnName(string value) =>
        this.value = value;

    public static Fin<CsvColumnName> From(string value) =>
        string.IsNullOrWhiteSpace(value)
        ? Error.New("Column name must not be empty")
        : new CsvColumnName(value);

    public static CsvColumnName FromOrThrow(string value) =>
        From(value).ThrowIfFail();

    public override string ToString() => value;

    public override bool Equals(object? obj) =>
        obj is CsvColumnName other && Equals(other);

    public override int GetHashCode() => value.GetHashCode(StringComparison.OrdinalIgnoreCase);

    public bool Equals(CsvColumnName? other) =>
        other is not null && value.Equals(other.value, StringComparison.OrdinalIgnoreCase);

    public int CompareTo(CsvColumnName? other) =>
        string.Compare(value, other?.value, StringComparison.OrdinalIgnoreCase);

    public static bool operator ==(CsvColumnName? left, CsvColumnName? right) =>
        (left, right) switch
        {
            (null, null) => true,
            (null, _) => false,
            (_, null) => false,
            (_, _) => left.Equals(right)
        };

    public static bool operator !=(CsvColumnName left, CsvColumnName right) =>
        !(left == right);
}

public sealed record CsvRow
{
    public required CsvRowNumber Number { get; init; }

    public required FrozenDictionary<CsvColumnNumber, string> Columns { get; init; }

    public Option<string> FindValue(int columnNumber) =>
        CsvColumnNumber.From(columnNumber)
                       .ToOption()
                       .Bind(FindValue);

    public Option<string> FindValue(CsvColumnNumber columnNumber) =>
        Columns.Find(columnNumber);

    public Option<string> FindValue(string columnName, IDictionary<CsvColumnName, CsvColumnNumber> headerDictionary) =>
        CsvColumnName.From(columnName)
                     .ToOption()
                     .Bind(columnName => FindValue(columnName, headerDictionary));

    public Option<string> FindValue(CsvColumnName columnName, IDictionary<CsvColumnName, CsvColumnNumber> headerDictionary) =>
        headerDictionary.Find(columnName)
                        .Bind(FindValue);
}

public static class CsvModule
{
    public static async ValueTask<BinaryData> WriteRows(IEnumerable<CsvRow> rows, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();

        await WriteRows(rows, stream, leaveOpen: true, cancellationToken);
        stream.Position = 0;

        return await BinaryData.FromStreamAsync(stream, cancellationToken);
    }

    public static async ValueTask WriteRows(IEnumerable<CsvRow> rows, Stream stream, bool leaveOpen = true, CancellationToken cancellationToken = default)
    {
        using var streamWriter = new StreamWriter(stream, leaveOpen: leaveOpen);
        using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);

        await rows.OrderBy(row => row.Number)
                  .Iter(async row => await WriteRow(row, csvWriter), cancellationToken);

        await csvWriter.FlushAsync();
    }

    private static async ValueTask WriteRow(CsvRow row, IWriter writer)
    {
        row.Columns
           .OrderBy(column => column.Key)
           .Select(column => column.Value)
           .Iter(writer.WriteField);

        await writer.NextRecordAsync();
    }

    public static async IAsyncEnumerable<CsvRow> GetRows(BinaryData data, TrimOptions trimOptions = TrimOptions.None, bool ignoreBlankLines = true)
    {
        var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
            MissingFieldFound = _ => { },
            TrimOptions = trimOptions,
            IgnoreBlankLines = ignoreBlankLines
        };

        using var stream = data.ToStream();
        using var streamReader = new StreamReader(stream);
        using var csvReader = new CsvReader(streamReader, configuration);

        while (await csvReader.ReadAsync())
        {
            yield return GetRow(csvReader);
        }
    }

    private static CsvRow GetRow(IReader reader) =>
        new()
        {
            Number = GetRowNumber(reader),
            Columns = GetRowColumns(reader)
        };

    private static CsvRowNumber GetRowNumber(IReader reader) =>
        CsvRowNumber.FromOrThrow(reader.Parser.RawRow);

    private static FrozenDictionary<CsvColumnNumber, string> GetRowColumns(IReader reader)
    {
        var values = reader.Parser.Record ?? [];

        return values.Select((value, index) =>
        {
            var columnNumber = CsvColumnNumber.FromOrThrow(index + 1);

            return (columnNumber, value);
        }).ToFrozenDictionary();
    }

    public static async ValueTask<FrozenDictionary<CsvColumnName, CsvColumnNumber>> GetHeaderDictionary(BinaryData data)
    {
        var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            MissingFieldFound = _ => { }
        };

        using var stream = data.ToStream();
        using var streamReader = new StreamReader(stream);
        using var reader = new CsvReader(streamReader, configuration);

        await reader.ReadAsync();

        try
        {
            reader.ReadHeader();
        }
        catch (ReaderException exception) when (exception.Message.Contains("No header record was found."))
        {
            return FrozenDictionary<CsvColumnName, CsvColumnNumber>.Empty;
        }

        return GetHeaderDictionary(reader);
    }

    private static FrozenDictionary<CsvColumnName, CsvColumnNumber> GetHeaderDictionary(IReader reader) =>
        GetHeaderDictionary(reader.Parser.Record ?? []);

    public static FrozenDictionary<CsvColumnName, CsvColumnNumber> GetHeaderDictionary(IEnumerable<string> headerValues) =>
        headerValues.Select((column, index) => (column, index))
                    .Choose(x => string.IsNullOrWhiteSpace(x.column)
                                    ? Option<(CsvColumnName, CsvColumnNumber)>.None
                                    : (CsvColumnName.FromOrThrow(x.column),
                                       CsvColumnNumber.FromOrThrow(x.index + 1)))
                    .ToFrozenDictionary();
}