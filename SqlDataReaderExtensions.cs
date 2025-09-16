using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Amop.Core.Constants;
using Microsoft.Data.SqlClient;

namespace Amop.Core.Helpers
{
    public static class SqlDataReaderExtensions
    {
        public static string StringFromReader(this SqlDataReader dataReader, List<string> columns, string columnName, string defaultValue = "")
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return defaultValue;
                }
                else
                {
                    return dataReader[columnName].ToString();
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static bool BooleanFromReader(this SqlDataReader dataReader, List<string> columns, string columnName)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return false;
                }
                else
                {
                    if (bool.TryParse(dataReader[columnName].ToString(), out bool valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static int IntFromReader(this SqlDataReader dataReader, List<string> columns, string columnName)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return 0;
                }
                else
                {
                    if (int.TryParse(dataReader[columnName].ToString(), out int valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static long LongFromReader(this SqlDataReader dataReader, List<string> columns, string columnName)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return 0L;
                }
                else
                {
                    if (long.TryParse(dataReader[columnName].ToString(), out long valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static decimal DecimalFromReader(this SqlDataReader dataReader, List<string> columns, string columnName)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return 0M;
                }
                else
                {
                    if (decimal.TryParse(dataReader[columnName].ToString(), out decimal valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static List<T> ListFromReader<T>(this SqlDataReader dataReader, List<string> columns, string columnName)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return new List<T>();
                }
                else
                {
                    string json = dataReader[columnName].ToString();
                    try
                    {
                        return JsonSerializer.Deserialize<List<T>>(json);
                    }
                    catch (JsonException)
                    {
                        return new List<T>();
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static Guid GuidFromReader(this SqlDataReader dataReader, List<string> columns, string columnName)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return Guid.Empty;
                }
                else
                {
                    if (Guid.TryParse(dataReader[columnName].ToString(), out Guid valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static DateTime DateTimeFromReader(this SqlDataReader dataReader, List<string> columns, string columnName, bool allowNullValue = false)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)) && !allowNullValue)
                {
                    throw new ArgumentException(string.Format(LogCommonStrings.COLUMN_VALUE_NULL_ERROR, columnName));
                }
                else if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)) && allowNullValue)
                {
                    return new DateTime();
                }
                else
                {
                    if (DateTime.TryParse(dataReader[columnName].ToString(), out DateTime valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static int? NullableIntFromReader(this SqlDataReader dataReader, List<string> columns, string columnName, int? defaultValue = null)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    return defaultValue;
                }
                else
                {
                    if (int.TryParse(dataReader[columnName].ToString(), out int valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static DateTime? NullableDateTimeFromReader(this SqlDataReader dataReader, List<string> columns, string columnName, bool allowDefaultValue = false, DateTime? defaultValue = null)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)) && !allowDefaultValue)
                {
                    throw new ArgumentException(string.Format(LogCommonStrings.COLUMN_VALUE_NULL_ERROR, columnName));
                }
                else if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)) && allowDefaultValue)
                {
                    return defaultValue;
                }
                else
                {
                    if (DateTime.TryParse(dataReader[columnName].ToString(), out DateTime valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }
        public static decimal? NullableDecimalFromReader(this SqlDataReader dataReader, List<string> columns, string columnName, bool allowDefaultValue = false, int? defaultValue = null)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    if (allowDefaultValue)
                    {
                        return defaultValue;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.COLUMN_VALUE_NULL_ERROR, columnName));
                    }
                }
                else
                {
                    if (decimal.TryParse(dataReader[columnName].ToString(), out decimal valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static long? NullableLongFromReader(this SqlDataReader dataReader, List<string> columns, string columnName, bool allowDefaultValue = false, long? defaultValue = null)
        {
            if (columns.Any(s => s.IndexOf(columnName, StringComparison.InvariantCultureIgnoreCase) >= 0))
            {
                if (dataReader.IsDBNull(dataReader.GetOrdinal(columnName)))
                {
                    if (allowDefaultValue)
                    {
                        return defaultValue;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.COLUMN_VALUE_NULL_ERROR, columnName));
                    }
                }
                else
                {
                    if (long.TryParse(dataReader[columnName].ToString(), out long valueFromReader))
                    {
                        return valueFromReader;
                    }
                    else
                    {
                        throw new ArgumentException(string.Format(LogCommonStrings.FAILED_TO_PARSE_FOR_ATTRIBUTE, columnName, dataReader[columnName]));
                    }
                }
            }
            else
            {
                throw new ArgumentException(string.Format(LogCommonStrings.INVALID_COLUMN_NAME, columnName));
            }
        }

        public static List<string> GetColumnsFromReader(this SqlDataReader dataReader)
        {
            return Enumerable.Range(0, dataReader.FieldCount).Select(dataReader.GetName).ToList();
        }
    }
}
