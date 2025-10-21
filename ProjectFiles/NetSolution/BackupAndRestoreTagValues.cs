#region Using directives
using System;
using UAManagedCore;
using FTOptix.NetLogic;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using FTOptix.Core;
using System.Globalization;
using System.Reflection;
using FTOptix.HMIProject;
using FTOptix.Alarm;
using FTOptix.SQLiteStore;
using FTOptix.Store;
using FTOptix.EventLogger;
using FTOptix.System;
using FTOptix.Recipe;
#endregion

public class BackupAndRestoreTagValues : BaseNetLogic
{
    public override void Stop()
    {
        longRunningTask?.Dispose();
        longRunningTask = null;
    }

    [ExportMethod]
    public void BackupTagValues()
    {
        longRunningTask = new LongRunningTask(BackupTagValuesImpl, LogicObject);
        longRunningTask.Start();
    }

    [ExportMethod]
    public void RestoreTagValues()
    {
        longRunningTask = new LongRunningTask(RestoreTagValuesImpl, LogicObject);
        longRunningTask.Start();
    }

    private LongRunningTask longRunningTask;

    #region BackupTagValues
    private void BackupTagValuesImpl()
    {
        var csvPath = GetCSVFilePath();
        if (string.IsNullOrEmpty(csvPath))
        {
            Log.Error("BackupAndRestoreTagValues", "No CSV file chosen, please fill the CSVPath variable");
            return;
        }

        char? fieldDelimiter = GetFieldDelimiter();
        if (fieldDelimiter == null || fieldDelimiter == '\0')
            return;

        var wrapFields = GetWrapFields();
        var timeout = GetTimeout();

        // Get the parent node from which the tags must be exported
        var parentNodeVariable = LogicObject.GetVariable("ParentNode");
        if (parentNodeVariable == null)
        {
            Log.Error("BackupAndRestoreTagValues", "Unable to retrieve the ParentNode variable. Tags backup aborted");
            return;
        }

        var parentNode = InformationModel.Get(parentNodeVariable.Value);
        if (parentNode == null)
        {
            Log.Error("BackupAndRestoreTagValues", "Specified parent node is null. Tags backup aborted");
            return;
        }

        var parentNodeTags = parentNode.ChildrenRemoteRead(timeout);

        try
        {
            using (var csvWriter = new CSVFileWriter(csvPath) { FieldDelimiter = fieldDelimiter.Value,
                                                                WrapFields = wrapFields })
            {
                var header = new string[] { "Index", "RelativePath", "Value", "DataType" };
                csvWriter.WriteLine(header);

                foreach (var tag in parentNodeTags)
                {
                    var relativePath = tag.RelativePath;
                    var currentTagVariable = parentNode.GetVariable(relativePath);
                    if (!(currentTagVariable is FTOptix.CommunicationDriver.Tag))
                        continue;

                    var tagValue = tag.Value.Value;
                    if (tagValue == null)
                    {
                        Log.Error("BackupAndRestoreTagValues", $"Skipping tag {relativePath} since its value is null");
                        continue;
                    }

                    // Skip structured tag dimensions information
                    if (tag.RelativePath.Contains("ArrayDimensions"))
                        continue;

                    if (tagValue.GetType().IsArray)
                        BackupTagArrayValue(relativePath, tagValue, csvWriter);
                    else
                        BackupTagScalarValue(relativePath, tagValue, csvWriter);
                }
            }

            Log.Info("BackupAndRestoreTagValues", $"Tags backup successfully written to CSV file {csvPath}");
        }
        catch (Exception e)
        {
            Log.Error("BackupAndRestoreTagValues", $"Unable to write CSV file: {e.Message}");
        }
    }

    private void BackupTagArrayValue(string relativePath, object tagValue, CSVFileWriter csvWriter)
    {
        var tagArray = (Array)tagValue;
        if (tagArray == null)
        {
            Log.Error("BackupAndRestoreTagValues", $"Unable to retrieve array value of tag {relativePath}");
            return;
        }

        int rows = tagArray.GetLength(0);
        int columns;

        var arrayRank = tagArray.Rank;
        if (arrayRank == 1)
        {
            csvWriter.WriteLine(new string[] { $"ARRAY:{rows}", "", "", "" });
            for (int i = 0; i < rows; i++)
                BackupTagScalarValue(relativePath, tagArray.GetValue(i), csvWriter, i.ToString());
        }
        else if (arrayRank == 2)
        {
            columns = tagArray.GetLength(1);
            csvWriter.WriteLine(new string[] { $"ARRAY:{rows}x{columns}", "", "", "" });
            for (int i = 0; i < rows; i++)
            {
                for (int j = 0; j < columns; j++)
                    BackupTagScalarValue(relativePath, tagArray.GetValue(i, j), csvWriter, $"{i}.{j}");
            }
        }
        else
        {
            Log.Error("BackupAndRestoreTagValues", $"Only one- and two-dimensional arrays are supported. Tag {relativePath} will be skipped");
            return;
        }
    }

    private void BackupTagScalarValue(string relativePath, object tagValue, CSVFileWriter csvWriter, string index = "")
    {
        var currentRow = new string[4];

        // Set Index
        currentRow[0] = String.Empty;
        if (index != String.Empty)
            currentRow[0] = index;

        // Set Value and Relative Path
        currentRow[1] = relativePath;

        // Floating point values must be handled specifically. Note that System.Single is an alias for float.
        // See https://docs.microsoft.com/it-it/dotnet/standard/base-types/standard-numeric-format-strings#RFormatString
        // DateTime values are serialized using standard format ISO 8601.
        // See https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings#the-round-trip-o-o-format-specifier
        // Other values (integer, booleans, strings) has no formatting problems.
        var tagNetType = tagValue.GetType();
        switch (Type.GetTypeCode(tagNetType))
        {
            case TypeCode.Single:
                currentRow[2] = String.Format(CultureInfo.InvariantCulture, "{0:G9}", tagValue);
                break;
            case TypeCode.Double:
                currentRow[2] = String.Format(CultureInfo.InvariantCulture, "{0:G17}", tagValue);
                break;
            case TypeCode.DateTime:
                var date = (DateTime)tagValue;
                if (date.Kind != DateTimeKind.Utc)
                    date = date.ToUniversalTime();
                currentRow[2] = date.ToString("O");
                break;
            default:
                currentRow[2] = tagValue.ToString();
                break;
        }

        // Set value Type
        var tagValueString = DataTypesHelper.GetDataTypeNameByNetType(tagNetType.GetTypeInfo());
        if (string.IsNullOrEmpty(tagValueString))
        {
            Log.Error("BackupAndRestoreTagValues", $"DataTypeId for Type {tagNetType} in tag {relativePath} was not found. Tag backup will be skipped");
            return;
        }

        currentRow[3] = tagValueString;

        csvWriter.WriteLine(currentRow);
    }
    #endregion

    #region RestoreTagValues
    private void RestoreTagValuesImpl()
    {
        var csvPath = GetCSVFilePath();
        if (string.IsNullOrEmpty(csvPath))
        {
            Log.Error("BackupAndRestoreTagValues", "Unable to restore PLC tag values: please specify the input CSV file");
            return;
        }

        var fieldDelimiter = GetFieldDelimiter();
        if (fieldDelimiter == '.')
        {
            Log.Error("BackupAndRestoreTagValues", $"Unable to restore PLC tag values: CSV separator {fieldDelimiter} is not supported");
            return;
        }

        var wrapFields = GetWrapFields();
        var timeout = GetTimeout();

        if (!File.Exists(csvPath))
        {
            Log.Error("BackupAndRestoreTagValues", $"Unable to restore PLC tag values: CSV file {csvPath} not found");
            return;
        }

        var parentNodeVariable = LogicObject.GetVariable("ParentNode");
        if (parentNodeVariable == null)
        {
            Log.Error("BackupAndRestoreTagValues", "Specified parent node variable is null. Tags backup aborted");
            return;
        }

        var parentNode = InformationModel.Get(parentNodeVariable.Value);
        if (parentNode == null)
        {
            Log.Error("BackupAndRestoreTagValues", "Specified parent node is null. Tags backup aborted");
            return;
        }

        try
        {
            using (var csvReader = new CSVFileReader(csvPath) { FieldDelimiter = fieldDelimiter.Value,
                                                                WrapFields = wrapFields })
            {
                if (csvReader.EndOfFile())
                {
                    Log.Error("BackupAndRestoreTagValues", $"The CSV file {csvPath} is empty");
                    return;
                }

                try
                {
                    parentNode.ChildrenRemoteWrite(PrepareValuesToWrite(csvReader, parentNode), timeout);
                    Log.Info("BackupAndRestoreTagValues", $"Tags restored successfully to node {parentNode.BrowseName}");
                }
                catch (Exception ex)
                {
                    Log.Error("BackupAndRestoreTagValues", "ChildrenRemoteWrite failed: " + ex.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error("BackupAndRestoreTagValues", $"Unable to restore PLC tag values: {ex}");
        }

    }

    private List<RemoteChildVariableValue> PrepareValuesToWrite(CSVFileReader csvReader, IUANode parentNode)
    {
        // Ignore header line (at least one line exists)
        csvReader.ReadLine();

        var valuesToWrite = new List<RemoteChildVariableValue>();
        while (!csvReader.EndOfFile())
        {
            var fileLine = csvReader.ReadLine();

            var index = fileLine[0];
            if (index.Contains(arrayPlaceholderPrefix)) // Tag array
            {
                var arrayDimensions = index.Substring(arrayPlaceholderPrefix.Length).Split('x');

                int rows = int.Parse(arrayDimensions[0]);
                int columns;

                // Read the first value to get the datatype of the array
                fileLine = csvReader.ReadLine();
                var relativePath = fileLine[1];

                // Skip array restore since the variable could not be found
                if (parentNode.GetVariable(relativePath) == null)
                    continue;

                var valueString = fileLine[2];
                var dataTypeString = fileLine[3];

                var arrayNetType = GetNetTypeFromOPCUAType(dataTypeString);
                var arrayElementValue = CreateUAValueFromString(valueString, arrayNetType);

                Array arrayTagValues;
                var arrayRank = arrayDimensions.Length;
                if (arrayRank == 1)
                {
                    arrayTagValues = Array.CreateInstance(arrayNetType, rows);

                    // Set the already read value as the first element, then all the others
                    arrayTagValues.SetValue(arrayElementValue.Value, 0);
                    for (int i = 1; i < rows; i++)
                    {
                        fileLine = csvReader.ReadLine();
                        arrayElementValue = CreateUAValueFromString(fileLine[2], arrayNetType);
                        arrayTagValues.SetValue(arrayElementValue.Value, i);
                    }
                }
                else if (arrayRank == 2)
                {
                    columns = int.Parse(arrayDimensions[1]);
                    arrayTagValues = Array.CreateInstance(arrayNetType, new int[] { rows, columns });

                    // Set the already read value as the first element, then all the others
                    arrayTagValues.SetValue(arrayElementValue.Value, 0, 0);
                    for (int i = 0; i < rows; i++)
                    {
                        for (int j = 0; j < columns; j++)
                        {
                            if (i == 0 && j == 0) continue;
                            fileLine = csvReader.ReadLine();
                            arrayElementValue = CreateUAValueFromString(fileLine[2], arrayNetType);
                            arrayTagValues.SetValue(arrayElementValue.Value, i, j);
                        }
                    }
                }
                else
                {
                    throw new Exception($"Malformed line {fileLine}");
                }

                valuesToWrite.Add(new RemoteChildVariableValue(relativePath, new UAValue(arrayTagValues)));
            }
            else if (string.IsNullOrEmpty(index)) // Scalar tag value
            {
                var relativePath = fileLine[1];
                if (parentNode.GetVariable(relativePath) == null)
                    continue;

                var valueString = fileLine[2];
                var dataTypeString = fileLine[3];

                var tagUAValue = CreateUAValueFromString(valueString, GetNetTypeFromOPCUAType(dataTypeString));
                valuesToWrite.Add(new RemoteChildVariableValue(relativePath, tagUAValue));
            }
        }

        return valuesToWrite;
    }

    private UAValue CreateUAValueFromString(string valueString, Type netType)
    {
        switch (Type.GetTypeCode(netType))
        {
            case TypeCode.SByte: // Int8
                return new UAValue(SByte.Parse(valueString));
            case TypeCode.Int16:
                return new UAValue(Int16.Parse(valueString));
            case TypeCode.Int32:
                return new UAValue(Int32.Parse(valueString));
            case TypeCode.Int64:
                return new UAValue(Int64.Parse(valueString));
            case TypeCode.Byte: // UInt8
                return new UAValue(byte.Parse(valueString));
            case TypeCode.UInt16:
                return new UAValue(UInt16.Parse(valueString));
            case TypeCode.UInt32:
                return new UAValue(UInt32.Parse(valueString));
            case TypeCode.UInt64:
                return new UAValue(UInt64.Parse(valueString));
            case TypeCode.Boolean:
                return new UAValue(Boolean.Parse(valueString));
            case TypeCode.Double:
                return new UAValue(Double.Parse(valueString, CultureInfo.InvariantCulture));
            case TypeCode.Single:
                return new UAValue(Single.Parse(valueString, CultureInfo.InvariantCulture));
            case TypeCode.String:
                return new UAValue(valueString);
            case TypeCode.DateTime:
                var date = DateTime.Parse(valueString, null, DateTimeStyles.RoundtripKind);
                return new UAValue(new DateTime(date.Ticks, DateTimeKind.Utc));
            default:
                throw new Exception($"Unsupported data type {netType} for value {valueString}");
        }
    }

    private Type GetNetTypeFromOPCUAType(string dataTypeString)
    {
        var netType = DataTypesHelper.GetNetTypeByDataTypeName(dataTypeString);
        if (netType == null)
            throw new Exception($"Type corresponding to {dataTypeString} was not found in OPCUA namespace");
        return netType;
    }
    #endregion

    private string GetCSVFilePath()
    {
        var csvPathVariable = LogicObject.GetVariable("CSVPath");
        if (csvPathVariable == null)
        {
            Log.Error("BackupAndRestoreTagValues", "CSVPath variable not found");
            return "";
        }

        return new ResourceUri(csvPathVariable.Value).Uri;
    }

    private char? GetFieldDelimiter()
    {
        var separatorVariable = LogicObject.GetVariable("CharacterSeparator");
        if (separatorVariable == null)
        {
            Log.Error("BackupAndRestoreTagValues", "CharacterSeparator variable not found");
            return null;
        }

        string separator = separatorVariable.Value;

        if (separator.Length != 1 || separator == String.Empty)
        {
            Log.Error("BackupAndRestoreTagValues", "Wrong CharacterSeparator configuration. Please insert a char");
            return null;
        }

        if (char.TryParse(separator, out char result))
            return result;

        return null;
    }

    private bool GetWrapFields()
    {
        var wrapFieldsVariable = LogicObject.GetVariable("WrapFields");
        if (wrapFieldsVariable == null)
        {
            Log.Error("BackupAndRestoreTagValues", "WrapFields variable not found");
            return false;
        }

        return wrapFieldsVariable.Value;
    }

    private int GetTimeout()
    {
        var timeout = LogicObject.GetVariable("Timeout");
        if (timeout == null)
        {
            Log.Error("BackupAndRestoreTagValues", "Timeout variable not found");
            return 30000;
        }

        return timeout.Value;
    }

    private const string arrayPlaceholderPrefix = "ARRAY:";

    #region CSV Read/Write classes
    private class CSVFileReader : IDisposable
    {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public bool IgnoreMalformedLines { get; set; } = false;

        public CSVFileReader(string filePath, System.Text.Encoding encoding)
        {
            streamReader = new StreamReader(filePath, encoding);
        }

        public CSVFileReader(string filePath)
        {
            streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8);
        }

        public CSVFileReader(StreamReader streamReader)
        {
            this.streamReader = streamReader;
        }

        public bool EndOfFile()
        {
            return streamReader.EndOfStream;
        }

        public List<string> ReadLine()
        {
            if (EndOfFile())
                return null;

            var line = streamReader.ReadLine();

            var result = WrapFields ? ParseLineWrappingFields(line) : ParseLineWithoutWrappingFields(line);

            currentLineNumber++;
            return result;
        }

        public List<List<string>> ReadAll()
        {
            var result = new List<List<string>>();
            while (!EndOfFile())
                result.Add(ReadLine());

            return result;
        }

        private List<string> ParseLineWithoutWrappingFields(string line)
        {
            if (string.IsNullOrEmpty(line) && !IgnoreMalformedLines)
                throw new FormatException($"Error processing line {currentLineNumber}. Line cannot be empty");

            return line.Split(FieldDelimiter).ToList();
        }

        private List<string> ParseLineWrappingFields(string line)
        {
            var fields = new List<string>();
            var buffer = new StringBuilder("");
            var fieldParsing = false;

            int i = 0;
            while (i < line.Length)
            {
                if (!fieldParsing)
                {
                    if (IsWhiteSpace(line, i))
                    {
                        ++i;
                        continue;
                    }

                    // Line and column numbers must be 1-based for messages to user
                    var lineErrorMessage = $"Error processing line {currentLineNumber}";
                    if (i == 0)
                    {
                        // A line must begin with the quotation mark
                        if (!IsQuoteChar(line, i))
                        {
                            if (IgnoreMalformedLines)
                                return null;
                            else
                                throw new FormatException($"{lineErrorMessage}. Expected quotation marks at column {i + 1}");
                        }

                        fieldParsing = true;
                    }
                    else
                    {
                        if (IsQuoteChar(line, i))
                            fieldParsing = true;
                        else if (!IsFieldDelimiter(line, i))
                        {
                            if (IgnoreMalformedLines)
                                return null;
                            else
                                throw new FormatException($"{lineErrorMessage}. Wrong field delimiter at column {i + 1}");
                        }
                    }

                    ++i;
                }
                else
                {
                    if (IsEscapedQuoteChar(line, i))
                    {
                        i += 2;
                        buffer.Append(QuoteChar);
                    }
                    else if (IsQuoteChar(line, i))
                    {
                        fields.Add(buffer.ToString());
                        buffer.Clear();
                        fieldParsing = false;
                        ++i;
                    }
                    else
                    {
                        buffer.Append(line[i]);
                        ++i;
                    }
                }
            }

            return fields;
        }

        private bool IsEscapedQuoteChar(string line, int i)
        {
            return line[i] == QuoteChar && i != line.Length - 1 && line[i + 1] == QuoteChar;
        }

        private bool IsQuoteChar(string line, int i)
        {
            return line[i] == QuoteChar;
        }

        private bool IsFieldDelimiter(string line, int i)
        {
            return line[i] == FieldDelimiter;
        }

        private bool IsWhiteSpace(string line, int i)
        {
            return Char.IsWhiteSpace(line[i]);
        }

        private StreamReader streamReader;
        private int currentLineNumber = 1;

        #region IDisposable support
        private bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                streamReader.Dispose();

            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

    private class CSVFileWriter : IDisposable
    {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public CSVFileWriter(string filePath)
        {
            streamWriter = new StreamWriter(filePath, false, System.Text.Encoding.UTF8);
        }

        public CSVFileWriter(string filePath, System.Text.Encoding encoding)
        {
            streamWriter = new StreamWriter(filePath, false, encoding);
        }

        public CSVFileWriter(StreamWriter streamWriter)
        {
            this.streamWriter = streamWriter;
        }

        public void WriteLine(string[] fields)
        {
            var stringBuilder = new StringBuilder();

            for (var i = 0; i < fields.Length; ++i)
            {
                if (WrapFields)
                    stringBuilder.AppendFormat("{0}{1}{0}", QuoteChar, EscapeField(fields[i]));
                else
                    stringBuilder.AppendFormat("{0}", fields[i]);

                if (i != fields.Length - 1)
                    stringBuilder.Append(FieldDelimiter);
            }

            streamWriter.WriteLine(stringBuilder.ToString());
            streamWriter.Flush();
        }

        private string EscapeField(string field)
        {
            var quoteCharString = QuoteChar.ToString();
            return field.Replace(quoteCharString, quoteCharString + quoteCharString);
        }

        private StreamWriter streamWriter;

        #region IDisposable Support
        private bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                streamWriter.Dispose();

            disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
    #endregion
}
