using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Models.Pond;
using Microsoft.Data.SqlClient;

namespace Amop.Core.Services.Att
{
    public class PondUsageReportReader
    {
        private readonly PondUsageRecordFactory factory;
        private readonly PondUsageNewCDRFormatRecordFactory newCDRFormatFactory;

        public PondUsageReportReader(PondUsageRecordFactory factory)
        {
            this.factory = factory;
        }
        public PondUsageReportReader(PondUsageNewCDRFormatRecordFactory factory)
        {
            this.newCDRFormatFactory = factory;
        }

        public DataTable ReadRecords(int serviceProviderId, string fileName, int linesToSkip, int fileId, StreamReader reader)
        {
            var rowNumber = 1;
            var records = InitPondDeviceUSageDataTable();
            if (reader != null)
            {
                while (!reader.EndOfStream)
                {
                    if (rowNumber <= linesToSkip)
                    {
                        reader.ReadLine();
                    }

                    // need to check b/c the header could be the only line
                    if (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            var record = records.NewRow();
                            RecordFromReader(record, serviceProviderId, fileName, rowNumber, line, fileId);
                            records.Rows.Add(record);
                        }

                        rowNumber++;
                    }
                }
            }

            return records;
        }

        public PondBatchedUsageRecords ReadBatchedRecords(int serviceProviderId, string fileName, int linesToSkip, StreamReader reader, long limit = long.MaxValue, bool isNewCDRFormat = false)
        {
            var rowNumber = 1;
            var isFullySynced = true;
            var records = InitPondDeviceUSageDataTable();
            if (reader != null)
            {
                while (!reader.EndOfStream)
                {
                    if (rowNumber <= linesToSkip)
                    {
                        reader.ReadLine();
                        rowNumber++;
                    }
                    // need to check b/c the header could be the only line
                    else if (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            var record = records.NewRow();
                            RecordFromReader(record, serviceProviderId, fileName, rowNumber, line, null, isNewCDRFormat);
                            records.Rows.Add(record);
                            if (records.Rows.Count >= limit)
                            {
                                isFullySynced = false;
                                break;
                            }
                        }

                        rowNumber++;
                    }
                }
            }

            return new PondBatchedUsageRecords(records, isFullySynced);
        }

        public void RecordFromReader(DataRow dataRow, int serviceProviderId, string fileName, int rowNumber, string line, int? fileId, bool isNewCDRFormat = false)
        {
            PondUsageRecord usageRecord;
            if (!isNewCDRFormat)
            {
                usageRecord = factory.FromLine(line);
            }
            else
            {
                usageRecord = newCDRFormatFactory.FromLine(line);
            }

            dataRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            dataRow[CommonColumnNames.FtpFileId] = fileId;
            dataRow[CommonColumnNames.FileRowNumber] = rowNumber;
            dataRow[CommonColumnNames.CDRID] = usageRecord.CDRID;
            dataRow[CommonColumnNames.ICCID] = usageRecord.ICCID;
            dataRow[CommonColumnNames.Type] = usageRecord.Type;
            dataRow[CommonColumnNames.ConnectTime] = usageRecord.ConnectTime;
            dataRow[CommonColumnNames.CloseTime] = usageRecord.CloseTime;
            dataRow[CommonColumnNames.Direction] = usageRecord.Direction;
            dataRow[CommonColumnNames.CalledParty] = usageRecord.CalledParty;
            dataRow[CommonColumnNames.CallingParty] = usageRecord.CallingParty;
            dataRow[CommonColumnNames.CountryISO3] = usageRecord.CountryISO3;
            dataRow[CommonColumnNames.CountryName] = usageRecord.CountryName;
            dataRow[CommonColumnNames.MCC] = usageRecord.MCC;
            dataRow[CommonColumnNames.MNC] = usageRecord.MNC;
            dataRow[CommonColumnNames.IMSIId] = usageRecord.IMSIId;
            dataRow[CommonColumnNames.IMSINumber] = usageRecord.IMSINumber;

            if (usageRecord.Duration != null)
            {
                dataRow[CommonColumnNames.Duration] = usageRecord.Duration.Value;
            }

            dataRow[CommonColumnNames.DateCreated] = DateTime.UtcNow;
        }

        private DataTable InitPondDeviceUSageDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add(CommonColumnNames.Id);
            table.Columns.Add(CommonColumnNames.ServiceProviderId);
            table.Columns.Add(CommonColumnNames.FtpFileId);
            table.Columns.Add(CommonColumnNames.FileRowNumber);
            table.Columns.Add(CommonColumnNames.CDRID);
            table.Columns.Add(CommonColumnNames.ICCID);
            table.Columns.Add(CommonColumnNames.Type);
            table.Columns.Add(CommonColumnNames.ConnectTime);
            table.Columns.Add(CommonColumnNames.CloseTime);
            table.Columns.Add(CommonColumnNames.Duration);
            table.Columns.Add(CommonColumnNames.Direction);
            table.Columns.Add(CommonColumnNames.CalledParty);
            table.Columns.Add(CommonColumnNames.CallingParty);
            table.Columns.Add(CommonColumnNames.CountryISO3);
            table.Columns.Add(CommonColumnNames.CountryName);
            table.Columns.Add(CommonColumnNames.MCC);
            table.Columns.Add(CommonColumnNames.MNC);
            table.Columns.Add(CommonColumnNames.IMSIId);
            table.Columns.Add(CommonColumnNames.IMSINumber);
            table.Columns.Add(CommonColumnNames.DateCreated);

            return table;
        }
    }
}
