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

    public int CompareTo(CsvColumnNumber? other) =>
        value.CompareTo(other?.value);

    public bool Equals(CsvColumnNumber? other) =>
        other is not null && value == other.value;

    public override int GetHashCode() => value.GetHashCode();

    public override string ToString() => value.ToString();

    public static bool operator <(CsvColumnNumber left, CsvColumnNumber right) =>
        left?.value < right?.value;

    public static bool operator <=(CsvColumnNumber left, CsvColumnNumber right) =>
        left.value <= right.value;

    public static bool operator >(CsvColumnNumber left, CsvColumnNumber right) =>
        left.value > right.value;

    public static bool operator >=(CsvColumnNumber left, CsvColumnNumber right) =>
        left.value >= right.value;
}

public sealed record CsvColumnName
{
    private readonly string value;

    private CsvColumnName(string value) =>
        this.value = value;

    public static Fin<CsvColumnName> From(string value) =>
        string.IsNullOrWhiteSpace(value)
        ? Error.New("Column name must not be empty")
        : new CsvColumnName(value);

    public override string ToString() => value;
}

public sealed record CsvRow
{
    public required CsvRowNumber Number { get; init; }

    public required FrozenDictionary<CsvColumnNumber, string> Columns { get; init; }

    public Option<string> GetValue(int columnNumber) =>
        GetValue(CsvColumnNumber.From(columnNumber)
                                .ThrowIfFail());

    public Option<string> GetValue(CsvColumnNumber columnNumber) =>
        Columns.Find(columnNumber);
}

public static class CsvModule
{
    public static async ValueTask<BinaryData> WriteRows(IEnumerable<CsvRow> rows, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();

        await WriteRows(rows, stream);
        stream.Position = 0;

        return await BinaryData.FromStreamAsync(stream, cancellationToken);
    }

    public static async ValueTask WriteRows(IEnumerable<CsvRow> rows, Stream stream)
    {
        using var streamWriter = new StreamWriter(stream, leaveOpen: true);
        using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);

        await rows.OrderBy(row => row.Number)
                  .Iter(async row => await WriteRow(row, csvWriter));

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
            Columns = GetColumnValues(reader)
        };

    private static CsvRowNumber GetRowNumber(IReader reader) =>
        CsvRowNumber.From(reader.Parser.RawRow)
                    .ThrowIfFail();

    private static FrozenDictionary<CsvColumnNumber, string> GetColumnValues(IReader reader)
    {
        var values = reader.Parser.Record ?? [];

        return values.Select((value, index) =>
        {
            var columnNumber = CsvColumnNumber.From(index + 1)
                                              .ThrowIfFail();

            return (columnNumber, value);
        }).ToFrozenDictionary();
    }

    public static async ValueTask<FrozenDictionary<CsvColumnNumber, CsvColumnName>> GetHeaderDictionary(BinaryData data)
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
            return FrozenDictionary<CsvColumnNumber, CsvColumnName>.Empty;
        }

        return GetHeaderDictionary(reader);
    }

    private static FrozenDictionary<CsvColumnNumber, CsvColumnName> GetHeaderDictionary(IReader reader)
    {
        var headerValues = reader.Parser.Record ?? [];

        return headerValues.Where(value => string.IsNullOrWhiteSpace(value) is false)
                           .Select((column, index) => (CsvColumnNumber.From(index + 1)
                                                                      .ThrowIfFail(),
                                                       CsvColumnName.From(column)
                                                                    .ThrowIfFail()))
                           .ToFrozenDictionary();
    }
}