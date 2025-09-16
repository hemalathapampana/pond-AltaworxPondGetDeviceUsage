using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS.Model;
using Amazon.SQS;
using Amop.Core.Constants;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models.Pond;
using Amop.Core.Repositories.Environment;
using Amop.Core.Models;
using Renci.SshNet;
using Amop.Core.Services.Att;
using System.Data;
using System.Text;
using System.Transactions;
using Microsoft.Data.SqlClient;
using Amop.Core.Helpers;
using Amop.Core.Resilience;
using Polly;
using System.Text.RegularExpressions;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxPondGetDeviceUsage;

public class Function : AwsFunctionBase
{
    private int LimitNumberOfFilesPerInstance;
    private int CheckFilesMissedThresholdDays;
    private long UsageRowsCountLimit;
    private bool IsNewCDRFormat;
    private string? DaysToKeep;
    private string? PondDeviceUsageQueueURL;
    private string? CleanUpQueueURL;

    private const long USAGE_FILE_HEADER_BYTE_SIZE = 139;
    private const int LATEST_USAGE_WRITE_TIME_THRESHOLD_IN_HOURS = 2;

    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {

        AmopLambdaContext? lambdaContext = null;
        try
        {
            lambdaContext = BaseAmopFunctionHandler(context);

            LogInfo(lambdaContext, CommonConstants.SUB, LogCommonStrings.STARTING_POND_DEVICE_USAGE_SYNC);

            if (lambdaContext == null)
            {
                ArgumentNullException.ThrowIfNull(lambdaContext);
            }
            TryGetAllEnvironmentVariables(lambdaContext);

            await ProcessEventAsync(lambdaContext, sqsEvent);
        }
        catch (Exception ex)
        {
            if (lambdaContext == null)
            {
                context.Logger.Log(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
            else
            {
                LogInfo(lambdaContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }
        finally
        {
            base.CleanUp(lambdaContext);
            LogInfo(lambdaContext, CommonConstants.INFO, LogCommonStrings.POND_DEVICE_USAGE_SYNC_ENDED);
        }
    }

    private void TryGetAllEnvironmentVariables(AmopLambdaContext lambdaContext)
    {
        EnvironmentRepository environmentRepo = new EnvironmentRepository();
        DaysToKeep = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.DAYS_TO_KEEP);
        PondDeviceUsageQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.POND_DEVICE_USAGE_QUEUE_URL);
        LimitNumberOfFilesPerInstance = GetIntValueFromEnvironmentVariable(lambdaContext, environmentRepo, PondHelper.CommonString.LIMIT_NUMBER_OF_FILES_PER_INSTANCE);
        CheckFilesMissedThresholdDays = GetIntValueFromEnvironmentVariable(lambdaContext, environmentRepo, PondHelper.CommonString.CHECK_FILES_MISSED_THRESHOLD_DAYS);
        UsageRowsCountLimit = GetLongValueFromEnvironmentVariable(lambdaContext, environmentRepo, PondHelper.CommonString.USAGE_ROWS_COUNT_LIMIT, PondHelper.CommonConfig.DEFAULT_USAGE_ROWS_COUNT_LIMIT);
        IsNewCDRFormat = GetBooleanValueFromEnvironmentVariable(lambdaContext, environmentRepo, PondHelper.CommonString.IS_NEW_CDR_FORMAT);
        CleanUpQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.CLEAN_UP_QUEUE_URL_VARIABLE_KEY);
    }

    private SqsValues GetMessageValues(AmopLambdaContext context, SQSEvent.SQSMessage message)
    {
        return new SqsValues(context, message);
    }

    public static new AmopLambdaContext BaseAmopFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
    {
        AmopLambdaContext lambdaContext = new AmopLambdaContext(context, skipOUSpecificLogic);
        return lambdaContext;
    }

    private async Task ProcessEventAsync(AmopLambdaContext context, SQSEvent sqsEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        if (sqsEvent?.Records != null)
        {
            var processedRecordCount = sqsEvent.Records.Count;
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
            foreach (var record in sqsEvent.Records)
            {
                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.MESSAGE_ID_VALUE, record.MessageId));
                var sqsValues = GetMessageValues(context, record);
                await ProcessEventRecordAsync(context, sqsValues);
            }
        }
        else
        {
            // Queue all Pond Mobile Providers
            await StartDailyDeviceUsageProcessingAsync(context, true);
        }
    }

    private async Task ProcessEventRecordAsync(KeySysLambdaContext context, SqsValues sqsValues)
    {
        LogInfo(context, CommonConstants.SUB);

        if (sqsValues.InitializeProcessing)
        {
            // Queue all Pond Mobile Providers
            await StartDailyDeviceUsageProcessingAsync(context, sqsValues.IsFromCloudwatchEvent);
        }
        else if (!sqsValues.InitializeProcessing && sqsValues.PondSyncDataStep > 0)
        {
            await ProcessPondUsageDataSync(context, sqsValues.ServiceProviderId, sqsValues.PondSyncDataStep, sqsValues.IsFromCloudwatchEvent);
        }
        else
        {
            await ProcessDailyUsage(context, sqsValues.ServiceProviderId, sqsValues.IsFromCloudwatchEvent, sqsValues.IsDownLoadFileAgain,
                sqsValues.IsDownloadNextInstance, sqsValues.WriteTimesNextDownload, sqsValues.FileNamesNextDownload, sqsValues.DownloadFailedIds);
        }
    }

    private async Task StartDailyDeviceUsageProcessingAsync(KeySysLambdaContext context, bool isFromCloudwatchEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        var policyFactory = new PolicyFactory(context.logger);
        var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);
        // Only truncate for hourly logic since staged data might be truncated if the hourly schedule is running at the same time as the daily sync
        if (isFromCloudwatchEvent)
        {
            TruncateDeviceUsageStaging(context, sqlRetryPolicy);
        }

        var currentServiceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.Pond, 0);
        while (currentServiceProviderId > 0)
        {
            // Add Provider to Queue
            await SendProcessMessageToQueueAsync(context, currentServiceProviderId, isFromCloudwatchEvent);

            // Get Next Provider
            currentServiceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.Pond, currentServiceProviderId);
        }
    }

    private async Task ProcessPondUsageDataSync(KeySysLambdaContext context, int serviceProviderId, int pondSyncDataStep, bool isFromCloudwatchEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        var policyFactory = new PolicyFactory(context.logger);
        var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);
        if (pondSyncDataStep == (int)PondSyncDataStepEnum.UpdatePondUsageFromStaging)
        {
            UpdatePondUsageFromStaging(context, serviceProviderId, sqlRetryPolicy);
            await SendProcessMessageToQueueAsync(context, serviceProviderId, isFromCloudwatchEvent, (int)PondSyncDataStepEnum.UpdateDeviceUsageFromPond);
        }
        else if (pondSyncDataStep == (int)PondSyncDataStepEnum.UpdateDeviceUsageFromPond)
        {
            if (isFromCloudwatchEvent)
            {
                UpdateDeviceUsageFromPond(context, serviceProviderId, sqlRetryPolicy);
            }
            else
            {
                await SendMessageToJasperDeviceCleanUpQueue(context, CleanUpQueueURL, serviceProviderId);
            }
        }
    }

    private async Task ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent, bool isDownLoadFileAgain,
            bool isDownloadNextInstance, List<string> writeTimesNextDownload, List<string> fileNamesNextDownload, List<string> downloadFailedIds)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId}");

        var settings = context.SettingsRepo.GetPondDeviceSettings(serviceProviderId, ParameterizedLog(context));

        if (!string.IsNullOrWhiteSpace(settings.PondSFTPUsername) && !string.IsNullOrWhiteSpace(settings.PondSFTPPassword))
        {
            try
            {
                var password = context.Base64Service.Base64Decode(settings.PondSFTPPassword);

                if (isDownLoadFileAgain && downloadFailedIds.Count > 0)
                {
                    var policyFactory = new PolicyFactory(context.logger);
                    var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);
                    await ProcessDownloadFileAgain(context, serviceProviderId, isFromCloudwatchEvent, settings.PondSFTPUsername, password,
                       settings.PondSFTPServer, settings.PondSFTPPath, downloadFailedIds, sqlRetryPolicy);
                }
                else if (isDownloadNextInstance && fileNamesNextDownload.Count > 0 && writeTimesNextDownload.Count > 0)
                {
                    await ProcessDownloadFileNextInstance(context, serviceProviderId, isFromCloudwatchEvent, settings.PondSFTPUsername, password,
                     settings.PondSFTPServer, settings.PondSFTPPath, fileNamesNextDownload, writeTimesNextDownload, downloadFailedIds);
                }
                else
                {
                    await ProcessDailyUsage(context, serviceProviderId, isFromCloudwatchEvent, settings.PondSFTPUsername, password,
                        settings.PondSFTPServer, settings.PondSFTPPath);
                }

                CleanUpFtp(context, settings.PondSFTPUsername, password, settings.PondSFTPServer, settings.PondSFTPPath);
            }
            catch (FormatException ex)
            {
                LogInfo(context, CommonConstants.EXCEPTION, LogCommonStrings.INPUT_STRING_IS_NOT_A_VALID_BASE64_STRING);
            }
        }
        else
        {
            LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.NO_SFTP_CREDENTIALS_FOUND_FOR_SERVICE_PROVIDER, serviceProviderId));
            await SendProcessMessageToQueueAsync(context, serviceProviderId, isFromCloudwatchEvent, (int)PondSyncDataStepEnum.UpdatePondUsageFromStaging);
        }
    }

    public async Task ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent, string username, string password, string server, string path)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId},{username},{server},{path}");
        if (!string.IsNullOrWhiteSpace(path))
        {
            var policyFactory = new PolicyFactory(context.logger);
            var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);
            var newestDataFileList = new List<UsageFile>();
            var fileNameNeedDownloadFailed = GetFileNamesDownloadFailed(context, serviceProviderId, sqlRetryPolicy);
            var fileDownloadAgainDt = InitFileNameDownloadFailedDataTable();
            var fileNamesNextDownload = new List<string>();
            var writeTimesNextDownload = new List<string>();
            var limitFilesDownloadNumber = LimitNumberOfFilesPerInstance - fileNameNeedDownloadFailed.Count;
            var today = DateTime.Now;
            var endDate = today.AddDays(1);

            if (!int.TryParse(DaysToKeep, out var daysToKeep))
            {
                daysToKeep = PondHelper.CommonConfig.DEFAULT_DAYS_TO_KEEP;
            }
            var startDate = today.AddDays(-daysToKeep);

            // Get files downloaded last week in AMOP
            var fileNameThresholdDays = GetFilesDownLoaded(context, startDate, endDate, sqlRetryPolicy);

            var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));
            using (SftpClient client = new SftpClient(connectionInfo))
            {
                client.Connect();

                // Limit to only get recent files
                var sftpStartDate = today.AddDays(-CheckFilesMissedThresholdDays);
                // Add buffer time for file uploading
                var sftpEndDate = today.AddHours(-LATEST_USAGE_WRITE_TIME_THRESHOLD_IN_HOURS);
                // Compare files download and files on SFTP in last week 
                // If file haven't downloaded -> re-download
                newestDataFileList = GetLatestUsageFileList(path, client, limitFilesDownloadNumber, fileNameThresholdDays, sftpStartDate, sftpEndDate);
            }

            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NUMBER_OF_FILES_NEED_TO_BE_DOWNLOADED, newestDataFileList.Count));

            if (newestDataFileList.Count > 0)
            {
                var deviceUsage = await GetLatestDeviceUsage(context, fileDownloadAgainDt, serviceProviderId, isFromCloudwatchEvent, username, password, server, path, newestDataFileList[0]);
                if (deviceUsage != null && deviceUsage.Rows.Count >= 1)
                {
                    LogInfo(context, CommonConstants.INFO, LogCommonStrings.POND_USAGE_SQL_BULK_COPY_START);
                    List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(deviceUsage);
                    SqlBulkCopy(context, context.CentralDbConnectionString, deviceUsage, DatabaseTableNames.POND_DEVICE_USAGE_STAGING, columnMappings);
                }

                // Because the first file downloaded, so just save files have not downloaded to send message
                if (newestDataFileList.Count > 1)
                {
                    foreach (var fileName in newestDataFileList.Select((item, index) => new { item, index }))
                    {
                        if (fileName.index > 0)
                        {
                            fileNamesNextDownload.Add(fileName.item.FilePath);
                            writeTimesNextDownload.Add(fileName.item.WriteTime.ToString(PondHelper.CommonString.POND_DATE_TIME_FORMAT));
                        }
                    }
                }
            }

            if (fileDownloadAgainDt != null && fileDownloadAgainDt.Rows.Count > 0)
            {
                LogInfo(context, CommonConstants.INFO, LogCommonStrings.INSERT_THE_FILE_DOWNLOAD_FAILED_TO_DATABASE);
                SqlBulkCopy(context, context.CentralDbConnectionString, fileDownloadAgainDt, DatabaseTableNames.POND_SFTP_FILE_DOWNLOAD_STATUS);
            }

            // Send message next sync Pond Device Usage
            if (writeTimesNextDownload.Count > 0)
            {
                var fileNamesNextDownloadString = string.Join(",", fileNamesNextDownload);
                var writeTimesNextDownloadString = string.Join(",", writeTimesNextDownload);
                var fileDownLoadFailedIds = fileNameNeedDownloadFailed.Count > 0 ? string.Join(",", fileNameNeedDownloadFailed) : "";

                await SendMessageToQueueNextDownloadAsync(context, serviceProviderId, isFromCloudwatchEvent,
                    fileNamesNextDownloadString, writeTimesNextDownloadString, fileDownLoadFailedIds);
            }
            else if (fileNameNeedDownloadFailed.Count > 0)
            {
                var fileDownLoadFailedIds = string.Join(",", fileNameNeedDownloadFailed);
                await SendMessageToQueueDownloadAgainAsync(context, serviceProviderId, isFromCloudwatchEvent, fileDownLoadFailedIds, 0);
            }
            else
            {
                await SendProcessMessageToQueueAsync(context, serviceProviderId, isFromCloudwatchEvent, (int)PondSyncDataStepEnum.UpdatePondUsageFromStaging);
            }
        }
        else
        {
            LogInfo(context, CommonConstants.WARNING, LogCommonStrings.POND_USAGE_REPORT_WILL_NOT_BE_LOADED);
        }
    }

    private DataTable InitFileNameDownloadFailedDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add(CommonColumnNames.Id);
        table.Columns.Add(CommonColumnNames.FileName);
        table.Columns.Add(CommonColumnNames.Status);
        table.Columns.Add(CommonColumnNames.ErrorDetail);
        table.Columns.Add(CommonColumnNames.WriteTime);
        table.Columns.Add(CommonColumnNames.ServiceProviderId);
        table.Columns.Add(CommonColumnNames.CreatedBy);
        table.Columns.Add(CommonColumnNames.CreatedDate);
        table.Columns.Add(CommonColumnNames.ModifiedBy);
        table.Columns.Add(CommonColumnNames.ModifiedDate);

        return table;
    }

    private static List<UsageFile> GetLatestUsageFileList(string path, SftpClient client, int limitFilesDownloadNumber, List<string> fileNameThresholdDays, DateTime startDate, DateTime endDate)
    {
        var newestFileList = new List<UsageFile>();

        foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
        {
            if (item.IsRegularFile)
            {
                if (Regex.IsMatch(item.Name, RegexConstants.REGEX_MATCH_POND_DEVICE_USAGE_SFTP_FILE) && item.FullName.EndsWith(PondHelper.CommonString.CSV_FORMAT))
                {
                    var timeinSFTP = item.LastWriteTime;
                    var isAlreadySynced = fileNameThresholdDays.Any(x => x.Equals(item.FullName));
                    if (timeinSFTP >= startDate && timeinSFTP <= endDate && !isAlreadySynced)
                    {
                        var newestFile = new UsageFile()
                        {
                            WriteTime = timeinSFTP,
                            FilePath = item.FullName,
                            WriteTimeUtc = item.LastWriteTimeUtc
                        };
                        newestFileList.Add(newestFile);
                    }
                }
            }
        }

        if (newestFileList.Count > 0)
        {
            newestFileList = newestFileList.OrderByDescending(x => x.WriteTime).Take(limitFilesDownloadNumber).ToList();
        }

        return newestFileList;
    }

    private async Task<DataTable?> GetLatestDeviceUsage(KeySysLambdaContext context, DataTable fileDownloadAgainDt, int serviceProviderId, bool isFromCloudwatchEvent, string username, string password, string server, string path, UsageFile newestFile)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId},{username},{server},{path}");

        return await InsertUsageRecords(context, fileDownloadAgainDt, serviceProviderId, isFromCloudwatchEvent, username, password, server, path, newestFile);
    }

    public async Task<DataTable?> InsertUsageRecords(KeySysLambdaContext context, DataTable retryDataTable, int serviceProviderId, bool isFromCloudwatchEvent, string username, string password,
           string server, string path, UsageFile file, bool shouldMarkedFileAsSuccess = false, bool shouldRetrySync = true)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId},{username},{server},{path}");
        DataTable? usage = null;
        var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));

        using (TransactionScope transactionScope = new TransactionScope(TransactionScopeOption.Required, new TimeSpan(0, 15, 0), TransactionScopeAsyncFlowOption.Enabled))
        {
            using (SftpClient client = new SftpClient(connectionInfo))
            {
                client.Connect();

                try
                {
                    var policyFactory = new PolicyFactory(context.logger);
                    var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);
                    var savedFileId = GetSFTPFileNameId(context, file.FilePath, sqlRetryPolicy);
                    var savedRecordCount = GetRecordCountFromDatabase(context, savedFileId, sqlRetryPolicy);

                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.SAVED_FILE_ID, savedFileId));
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.SAVED_RECORD_COUNT, savedRecordCount));

                    // If there is no record found, then start at 0
                    if (savedRecordCount < 0)
                    {
                        savedRecordCount = 0;
                    }

                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.READING_FILE, file.FilePath));
                    if (file.WriteTimeUtc == null)
                    {
                        var usageFileWriteTime = GetUsageFileWriteTimes(path, client, file.FilePath);
                        if (usageFileWriteTime == null)
                        {
                            LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.FILE_PATH_COULD_NOT_BE_FOUND, file.FilePath));
                            return usage;
                        }

                        file = usageFileWriteTime;
                    }

                    using (var memoryStream = new MemoryStream())
                    {
                        client.DownloadFile(file.FilePath, memoryStream);

                        memoryStream.Position = 0;
                        PondBatchedUsageRecords records;
                        PondUsageReportReader fileReader;

                        if (memoryStream.Length < USAGE_FILE_HEADER_BYTE_SIZE)
                        {
                            LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.FILE_IS_NOT_COMPLETELY_UPLOADED, file.FileName));
                            return null;
                        }

                        using (var reader = new StreamReader(memoryStream, Encoding.UTF8, true))
                        {
                            if (!IsNewCDRFormat)
                            {
                                fileReader = new PondUsageReportReader(new PondUsageRecordFactory());
                            }
                            else
                            {
                                fileReader = new PondUsageReportReader(new PondUsageNewCDRFormatRecordFactory());
                            }
                            records = fileReader.ReadBatchedRecords(serviceProviderId, file.FilePath, savedRecordCount + 1, reader, UsageRowsCountLimit, IsNewCDRFormat);
                            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.FILE_HAS_NUMBER_USAGE_RECORDS, records?.Records?.Rows?.Count));
                        }

                        if (records?.Records?.Rows?.Count > 0)
                        {
                            var fileId = -1;
                            // Save file name and check if file already synced
                            if (savedFileId <= 0)
                            {
                                // Save file name and check if file already synced
                                fileId = InsertUsageTableNameMapping(context, file, sqlRetryPolicy);
                                if (fileId <= 0)
                                {
                                    var errorMsg = string.Format(LogCommonStrings.ERROR_WHEN_SAVING_FILE_NAME, file.FilePath);
                                    LogInfo(context, CommonConstants.ERROR, errorMsg);
                                    throw new Exception(errorMsg);
                                }
                                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.INSERTED_FILE_WITH_NAME_AS_ID, file.FileName, fileId));
                            }
                            else
                            {
                                fileId = savedFileId;
                            }

                            records = UpdateFileIdForPondUsageRecords(fileId, records);

                            if (!records.IsEndOfFile)
                            {
                                await SendMessageToQueueDownloadAsync(context, serviceProviderId, isFromCloudwatchEvent, file, PondHelper.CommonConfig.DEFAULT_DELAY_SQS);
                            }
                        }
                        else
                        {
                            if (savedFileId <= 0)
                            {
                                // Insert file as blank
                                var fileId = InsertUsageTableNameMapping(context, file, sqlRetryPolicy, true);
                                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.INSERTED_BLANK_FILE_WITH_NAME_AS_ID, file.FileName, fileId));
                            }
                            else
                            {
                                if (savedRecordCount > 0)
                                {
                                    //File already synced but got queued again
                                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.FILE_HAVE_ALREADY_BEEN_SYNCED, file.FilePath, savedFileId));
                                }
                                else
                                {
                                    // Mark file as blank with savedFileId
                                    UpdateUsageFileAsBlank(context, savedFileId, sqlRetryPolicy);
                                }
                            }
                        }

                        usage = records?.Records;
                    }

                    if (shouldMarkedFileAsSuccess)
                    {
                        // Update status SUCCESS in db
                        UpdateFileNamesDownloadSuccess(context, serviceProviderId, file.FilePath, sqlRetryPolicy);
                        LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.UPDATE_SUCCESSFULLY_DOWNLOADED_FILE_WITH_FILE_PATH, file.FilePath));
                    }
                }
                catch (Exception ex)
                {
                    transactionScope.Dispose();
                    // Save filename
                    LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.ERROR_WHEN_DOWNLOADING_FILE_NAME, file.FilePath, ex.Message, ex.StackTrace));
                    if (shouldRetrySync && retryDataTable != null)
                    {
                        AddToDataRow(retryDataTable, file, ex.Message, serviceProviderId);
                    }
                    return usage;
                }
            }
            transactionScope.Complete();
        }

        return usage;
    }

    private void UpdateUsageFileAsBlank(KeySysLambdaContext context, int fileId, ISyncPolicy sqlRetryPolicy)
    {

        sqlRetryPolicy.Execute(() =>
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.FILE_ID, fileId)
            };
            return SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
                context.CentralDbConnectionString,
                SQLConstant.StoredProcedureName.POND_UPDATE_USAGE_FILE_AS_BLANK,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
        });
        LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.MARKED_FILE_WITH_ID_AS_BLANK, fileId));
    }

    private PondBatchedUsageRecords UpdateFileIdForPondUsageRecords(int fileId, PondBatchedUsageRecords records)
    {
        foreach (DataRow row in records.Records.Rows)
        {
            row[CommonColumnNames.FtpFileId] = fileId;
        }
        return records;
    }

    private void AddToDataRow(DataTable table, UsageFile usageFile, string errorDetail, int serviceProviderId)
    {
        var dataRow = table.NewRow();
        dataRow[CommonColumnNames.FileName] = usageFile.FilePath;
        dataRow[CommonColumnNames.Status] = CommonConstants.FAILED_STATUS;
        dataRow[CommonColumnNames.ErrorDetail] = errorDetail;
        dataRow[CommonColumnNames.WriteTime] = usageFile.WriteTime;
        dataRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
        dataRow[CommonColumnNames.CreatedBy] = PondHelper.CommonString.POND_AWS_GET_DEVICE_USAGE_LAMBDA;
        dataRow[CommonColumnNames.CreatedDate] = DateTime.UtcNow;
        dataRow[CommonColumnNames.ModifiedBy] = null;
        dataRow[CommonColumnNames.ModifiedDate] = null;
        table.Rows.Add(dataRow);
    }

    private static UsageFile GetUsageFileWriteTimes(string path, SftpClient client, string filePath)
    {
        var usageFileList = client.ListDirectory(path).ToList();

        return usageFileList.Where(x => x.FullName.Equals(filePath)).Select(x => new UsageFile()
        {
            WriteTime = x.LastWriteTime,
            FilePath = x.FullName,
            WriteTimeUtc = x.LastWriteTimeUtc
        }).FirstOrDefault();
    }

    private void CleanUpFtp(KeySysLambdaContext context, string username, string password, string server, string path)
    {
        LogInfo(context, CommonConstants.SUB);
        var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));

        if (!int.TryParse(DaysToKeep, out var daysToKeep))
        {
            daysToKeep = PondHelper.CommonConfig.DEFAULT_DAYS_TO_KEEP;
        }

        using (var client = new SftpClient(connectionInfo))
        {
            client.Connect();

            var cutOffTime = DateTime.Now.AddDays(-1 * daysToKeep);
            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item != null && item.IsRegularFile)
                {
                    var time = item.LastWriteTime;
                    if (time < cutOffTime)
                    {
                        try
                        {
                            item.Delete();
                        }
                        catch (Exception ex)
                        {
                            // This is only for cleaning up old usage records on SFTP, so only shown as warning
                            LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.DELETE_FILE_FAILED_BY_FOLLOWING_ISSUE, item.Name, ex.Message, ex.StackTrace));
                        }
                    }
                }
            }
        }
    }

    public async Task ProcessDownloadFileAgain(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent,
            string username, string password, string server, string path, List<string> downloadFailedIds, ISyncPolicy sqlRetryPolicy)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId},{username},{server},{path}");
        if (!string.IsNullOrWhiteSpace(path))
        {
            // Process Pond Usage
            var downLoadFailedId = int.Parse(downloadFailedIds[0]);
            var usageData = await DownloadAgainUsageFile(context, serviceProviderId, isFromCloudwatchEvent, username, password, server, path, downLoadFailedId, sqlRetryPolicy);
            if (usageData != null && usageData.Rows.Count > 0)
            {
                LogInfo(context, CommonConstants.INFO, LogCommonStrings.POND_USAGE_SQL_BULK_COPY_START);
                List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(usageData);
                SqlBulkCopy(context, context.CentralDbConnectionString, usageData, DatabaseTableNames.POND_DEVICE_USAGE_STAGING, columnMappings);
            }

            downloadFailedIds.RemoveAt(0);
            if (downloadFailedIds.Count > 0)
            {
                var downloadFailedIdsString = string.Join(",", downloadFailedIds);
                await SendMessageToQueueDownloadAgainAsync(context, serviceProviderId, isFromCloudwatchEvent, downloadFailedIdsString, 0);
            }
            else
            {
                await SendProcessMessageToQueueAsync(context, serviceProviderId, isFromCloudwatchEvent, (int)PondSyncDataStepEnum.UpdatePondUsageFromStaging);
            }
        }
        else
        {
            LogInfo(context, CommonConstants.WARNING, LogCommonStrings.POND_USAGE_REPORT_WILL_NOT_BE_LOADED);
        }
    }

    public async Task ProcessDownloadFileNextInstance(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent,
            string username, string password, string server, string path,
            List<string> fileNamesNextDownload, List<string> writeTimesNextDownload, List<string> downloadFailedIds)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId},{username},{server},{path}");
        if (!string.IsNullOrWhiteSpace(path))
        {
            var fileDownloadAgainDt = InitFileNameDownloadFailedDataTable();

            // Process Pond Usage
            var writeTime = DateTime.Parse(writeTimesNextDownload[0]);
            var usageData = await DownloadUsageFile(context, fileDownloadAgainDt, serviceProviderId, isFromCloudwatchEvent, username, password, server, path, fileNamesNextDownload[0], writeTime);
            if (usageData != null && usageData.Rows.Count > 0)
            {
                LogInfo(context, CommonConstants.INFO, LogCommonStrings.POND_USAGE_SQL_BULK_COPY_START);
                List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(usageData);
                SqlBulkCopy(context, context.CentralDbConnectionString, usageData, DatabaseTableNames.POND_DEVICE_USAGE_STAGING, columnMappings);
            }

            if (fileDownloadAgainDt != null && fileDownloadAgainDt.Rows.Count > 0)
            {
                LogInfo(context, CommonConstants.INFO, LogCommonStrings.INSERT_THE_FILE_DOWNLOAD_FAILED_TO_DATABASE);
                SqlBulkCopy(context, context.CentralDbConnectionString, fileDownloadAgainDt, DatabaseTableNames.POND_SFTP_FILE_DOWNLOAD_STATUS);
            }

            writeTimesNextDownload.RemoveAt(0);
            fileNamesNextDownload.RemoveAt(0);
            if (writeTimesNextDownload.Count > 0)
            {
                var fileNamesNextDownloadString = string.Join(",", fileNamesNextDownload);
                var writeTimesNextDownloadString = string.Join(",", writeTimesNextDownload);
                var fileDownLoadFailedIds = downloadFailedIds?.Count > 0 ? string.Join(",", downloadFailedIds) : "";

                await SendMessageToQueueNextDownloadAsync(context, serviceProviderId, isFromCloudwatchEvent, fileNamesNextDownloadString,
                    writeTimesNextDownloadString, fileDownLoadFailedIds);
            }
            else if (downloadFailedIds?.Count > 0)
            {
                var downloadFailedIdsString = string.Join(",", downloadFailedIds);
                await SendMessageToQueueDownloadAgainAsync(context, serviceProviderId, isFromCloudwatchEvent, downloadFailedIdsString, 0);
            }
            else
            {
                await SendProcessMessageToQueueAsync(context, serviceProviderId, isFromCloudwatchEvent, (int)PondSyncDataStepEnum.UpdatePondUsageFromStaging);
            }
        }
        else
        {
            LogInfo(context, CommonConstants.WARNING, LogCommonStrings.POND_USAGE_REPORT_WILL_NOT_BE_LOADED);
        }
    }

    private async Task<DataTable> DownloadUsageFile(KeySysLambdaContext context, DataTable fileDownloadAgainDt, int serviceProviderId, bool isFromCloudwatchEvent, string username, string password, string server, string path, string fileName, DateTime writeTime)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId},{username},{server},{path}");
        var newestFile = new UsageFile()
        {
            FilePath = fileName,
            WriteTime = writeTime
        };

        return await InsertUsageRecords(context, fileDownloadAgainDt, serviceProviderId, isFromCloudwatchEvent, username, password, server, path, newestFile);
    }

    private async Task<DataTable?> DownloadAgainUsageFile(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent, string username, string password, string server, string path, int downLoadFailId, ISyncPolicy sqlRetryPolicy)
    {
        LogInfo(context, CommonConstants.SUB, $"{downLoadFailId}");

        // Get file download error from DB
        PondSFTPFileDownloadStatus? fileFromDb = GetFileDownloadFailedByFileName(context, downLoadFailId, sqlRetryPolicy);
        if (fileFromDb == null || fileFromDb.Id < 1)
        {
            LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.FILE_NAME_COULD_NOT_BE_FOUND, fileFromDb.FileName));
            return null;
        }
        var newestFile = new UsageFile()
        {
            FilePath = fileFromDb.FileName,
            WriteTime = fileFromDb.WriteTime
        };

        return await InsertUsageRecords(context, null, serviceProviderId, isFromCloudwatchEvent, username, password, server, path, newestFile, true, false);
    }

    public static Action<string, string> ParameterizedLog(KeySysLambdaContext context)
    {
        return (type, message) => LogInfo(context, type, message);
    }

    private string ReadStagedFileName(SqlDataReader dataReader)
    {
        var columns = dataReader.GetColumnsFromReader();
        return dataReader.StringFromReader(columns, CommonColumnNames.FtpFileName);
    }

    private string ReadStagedFileNameId(SqlDataReader dataReader)
    {
        var columns = dataReader.GetColumnsFromReader();
        return dataReader.StringFromReader(columns, CommonColumnNames.Id);
    }

    private PondSFTPFileDownloadStatus ReadPondSFTPFileDownloadStatus(SqlDataReader pondDataReader)
    {
        return new PondSFTPFileDownloadStatus(pondDataReader);
    }

    private List<string> GetFilesDownLoaded(KeySysLambdaContext context, DateTime startDate, DateTime endDate, ISyncPolicy sqlRetryPolicy)
    {
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.START_DATE, startDate),
            new SqlParameter(CommonSQLParameterNames.END_DATE, endDate),
        };

        return sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context), context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_GET_FILES_DOWNLOADED,
            (dataReader) => ReadStagedFileName(dataReader),
            parameters,
            SQLConstant.ShortTimeoutSeconds));
    }

    private List<string> GetFileNamesDownloadFailed(KeySysLambdaContext context, int serviceProviderId, ISyncPolicy sqlRetryPolicy)
    {
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.STATUS, CommonConstants.FAILED_STATUS),
            new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId),
        };

        return sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context), context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_GET_SFTP_FILE_DOWNLOAD_STATUS_ID,
            (dataReader) => ReadStagedFileNameId(dataReader),
            parameters,
            SQLConstant.ShortTimeoutSeconds));
    }

    private PondSFTPFileDownloadStatus? GetFileDownloadFailedByFileName(KeySysLambdaContext context, int Id, ISyncPolicy sqlRetryPolicy)
    {
        List<PondSFTPFileDownloadStatus> pondSFTPFileDownloadStatus = new List<PondSFTPFileDownloadStatus>();
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.ID_PASCAL_CASE, Id),
        };

        sqlRetryPolicy.Execute(() =>
            pondSFTPFileDownloadStatus = SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(context), context.CentralDbConnectionString,
                SQLConstant.StoredProcedureName.POND_GET_SFTP_FILE_DOWNLOAD_BY_ID,
                (dataReader) => ReadPondSFTPFileDownloadStatus(dataReader),
                parameters,
                SQLConstant.ShortTimeoutSeconds));
        return pondSFTPFileDownloadStatus.FirstOrDefault();
    }

    private static void TruncateDeviceUsageStaging(KeySysLambdaContext context, ISyncPolicy sqlRetryPolicy)
    {
        sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
            context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_TRUNCATE_USAGE_STAGING,
            null,
            SQLConstant.TimeoutSeconds));
    }

    private int GetSFTPFileNameId(KeySysLambdaContext context, string fileName, ISyncPolicy sqlRetryPolicy)
    {
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.FILE_NAME, fileName),
        };

        return sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
            context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_GET_SFTP_FILENAME_ID,
            parameters,
            SQLConstant.ShortTimeoutSeconds));
    }

    private int InsertUsageTableNameMapping(KeySysLambdaContext context, UsageFile file, ISyncPolicy sqlRetryPolicy, bool isBlankFile = false)
    {
        return sqlRetryPolicy.Execute(() =>
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.FILE_NAME, file.FilePath),
                // Current sync time
                new SqlParameter(CommonSQLParameterNames.WRITE_TIME, DateTime.UtcNow),
                // Last modified time of file
                new SqlParameter(CommonSQLParameterNames.WRITE_TIME_UTC, file.WriteTimeUtc),
                new SqlParameter(CommonSQLParameterNames.IS_BLANK, isBlankFile.ToString())
            };

            return SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
                context.CentralDbConnectionString,
                SQLConstant.StoredProcedureName.POND_INSERT_USAGE_TABLE_NAME_MAPPING,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
        });
    }

    private int GetRecordCountFromDatabase(KeySysLambdaContext context, int savedFileId, ISyncPolicy sqlRetryPolicy)
    {
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.FILE_ID, savedFileId),
        };

        return sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
            context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_GET_RECORD_COUNT_FROM_DATABASE,
            parameters,
            SQLConstant.ShortTimeoutSeconds));
    }

    private void UpdateFileNamesDownloadSuccess(KeySysLambdaContext context, int serviceProviderId, string fileName, ISyncPolicy sqlRetryPolicy)
    {
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId),
            new SqlParameter(CommonSQLParameterNames.STATUS, CommonConstants.SUCCESS_STATUS),
            new SqlParameter(CommonSQLParameterNames.FILE_NAME, fileName),
        };

        sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
            context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_UPDATE_FILENAMES_DOWNLOAD_STATUS,
            parameters,
            SQLConstant.TimeoutSeconds));
    }

    private void UpdatePondUsageFromStaging(KeySysLambdaContext context, int serviceProviderId, ISyncPolicy sqlRetryPolicy)
    {
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId),
        };

        sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
            context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_UPDATE_DEVICE_USAGE_FROM_STAGING,
            parameters,
            SQLConstant.TimeoutSeconds));
    }

    private void UpdateDeviceUsageFromPond(KeySysLambdaContext context, int serviceProviderId, ISyncPolicy sqlRetryPolicy)
    {
        LogInfo(context, CommonConstants.SUB);
        var parameters = new List<SqlParameter>()
        {
            new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId),
        };

        sqlRetryPolicy.Execute(() =>
        SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
            context.CentralDbConnectionString,
            SQLConstant.StoredProcedureName.POND_DEVICE_SYNC,
            parameters,
            SQLConstant.TimeoutSeconds));
    }

    private async Task SendMessageToQueueDownloadAgainAsync(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent, string fileName, int secondDelays)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId}, {fileName}");

        if (string.IsNullOrEmpty(PondDeviceUsageQueueURL))
        {
            return;
        }

        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
        {
            var request = new SendMessageRequest
            {
                DelaySeconds = secondDelays,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.INITIALIZE_PROCESSING, new MessageAttributeValue { DataType = nameof(String), StringValue = false.ToString() } },
                        { SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue { DataType = nameof(String), StringValue = serviceProviderId.ToString() } },
                        { SQSMessageKeyConstant.IS_DOWNLOAD_FILE_AGAIN, new MessageAttributeValue { DataType = nameof(String), StringValue = true.ToString() } },
                        { SQSMessageKeyConstant.IS_FROM_CLOUDWATCH_EVENT, new MessageAttributeValue { DataType = nameof(String), StringValue = isFromCloudwatchEvent.ToString() } },
                    },
                MessageBody = LogCommonStrings.SENDING_SQS_MESSAGE_TO_POND_GET_DEVICE_USAGE_LAMBDA,
                QueueUrl = PondDeviceUsageQueueURL
            };

            if (!string.IsNullOrWhiteSpace(fileName))
            {
                request.MessageAttributes.Add(SQSMessageKeyConstant.DOWNLOAD_FAILED_IDS, new MessageAttributeValue { DataType = nameof(String), StringValue = fileName });
            }

            LogInfo(context, CommonConstants.INFO, request.MessageBody);

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }

    private async Task SendMessageToQueueNextDownloadAsync(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent,
           string fileNamesNextDownloadString, string writeTimesNextDownloadString, string fileDownLoadFailedIds)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId}, {fileNamesNextDownloadString}, {writeTimesNextDownloadString}, {fileDownLoadFailedIds}");

        if (string.IsNullOrEmpty(PondDeviceUsageQueueURL))
        {
            return;
        }

        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
        {
            var request = new SendMessageRequest
            {
                DelaySeconds = PondHelper.CommonConfig.DEFAULT_DELAY_SQS,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.INITIALIZE_PROCESSING, new MessageAttributeValue { DataType = nameof(String), StringValue = false.ToString() } },
                        { SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue { DataType = nameof(String), StringValue = serviceProviderId.ToString() } },
                        { SQSMessageKeyConstant.IS_DOWNLOAD_NEXT_INSTANCE, new MessageAttributeValue { DataType = nameof(String), StringValue = true.ToString() } },
                        { SQSMessageKeyConstant.IS_FROM_CLOUDWATCH_EVENT, new MessageAttributeValue { DataType = nameof(String), StringValue = isFromCloudwatchEvent.ToString() } },
                    },
                MessageBody = LogCommonStrings.SENDING_SQS_MESSAGE_TO_POND_GET_DEVICE_USAGE_LAMBDA,
                QueueUrl = PondDeviceUsageQueueURL
            };

            if (!string.IsNullOrWhiteSpace(fileDownLoadFailedIds))
            {
                request.MessageAttributes.Add(SQSMessageKeyConstant.DOWNLOAD_FAILED_IDS, new MessageAttributeValue { DataType = nameof(String), StringValue = fileDownLoadFailedIds });
            }
            if (!string.IsNullOrWhiteSpace(fileNamesNextDownloadString))
            {
                request.MessageAttributes.Add(SQSMessageKeyConstant.FILE_NAMES_NEXT_DOWNLOAD, new MessageAttributeValue { DataType = nameof(String), StringValue = fileNamesNextDownloadString });
            }
            if (!string.IsNullOrWhiteSpace(writeTimesNextDownloadString))
            {
                request.MessageAttributes.Add(SQSMessageKeyConstant.WRITE_TIMES_NEXT_DOWNLOAD, new MessageAttributeValue { DataType = nameof(String), StringValue = writeTimesNextDownloadString });
            }

            LogInfo(context, CommonConstants.INFO, request.MessageBody);

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }

    private async Task SendProcessMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent, int pondSyncDataStep = (int)PondSyncDataStepEnum.None, int delay = 0)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId}, {pondSyncDataStep}, {PondDeviceUsageQueueURL}");

        if (string.IsNullOrEmpty(PondDeviceUsageQueueURL))
        {
            return;
        }

        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
        {
            var request = new SendMessageRequest
            {
                DelaySeconds = (int)TimeSpan.FromSeconds(delay).TotalSeconds,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.INITIALIZE_PROCESSING, new MessageAttributeValue { DataType = nameof(String), StringValue = false.ToString() } },
                        { SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue { DataType = nameof(String), StringValue = serviceProviderId.ToString() } },
                        { SQSMessageKeyConstant.POND_SYNC_DATA_STEP, new MessageAttributeValue { DataType = nameof(String), StringValue = pondSyncDataStep.ToString() } },
                        { SQSMessageKeyConstant.IS_FROM_CLOUDWATCH_EVENT, new MessageAttributeValue { DataType = nameof(String), StringValue = isFromCloudwatchEvent.ToString() } },
                    },
                MessageBody = LogCommonStrings.SENDING_SQS_MESSAGE_TO_POND_GET_DEVICE_USAGE_LAMBDA,
                QueueUrl = PondDeviceUsageQueueURL
            };

            LogInfo(context, CommonConstants.INFO, request.MessageBody);

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }

    private async Task SendMessageToQueueDownloadAsync(KeySysLambdaContext context, int serviceProviderId, bool isFromCloudwatchEvent, UsageFile usageFile, int secondDelays)
    {
        LogInfo(context, CommonConstants.SUB, $"{serviceProviderId}");

        if (string.IsNullOrEmpty(PondDeviceUsageQueueURL))
        {
            return;
        }

        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
        {
            var request = new SendMessageRequest
            {
                DelaySeconds = secondDelays,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.INITIALIZE_PROCESSING, new MessageAttributeValue { DataType = nameof(String), StringValue = false.ToString() } },
                        { SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue { DataType = nameof(String), StringValue = serviceProviderId.ToString() } },
                        { SQSMessageKeyConstant.FILE_NAME, new MessageAttributeValue { DataType = nameof(String), StringValue = usageFile.FilePath } },
                        { SQSMessageKeyConstant.WRITE_TIME, new MessageAttributeValue { DataType = nameof(String), StringValue =  usageFile.WriteTime.ToString(PondHelper.CommonString.POND_DATE_TIME_FORMAT) } },
                        { SQSMessageKeyConstant.IS_DOWNLOAD_NEXT_INSTANCE, new MessageAttributeValue { DataType = nameof(String), StringValue = true.ToString() } },
                        { SQSMessageKeyConstant.IS_FROM_CLOUDWATCH_EVENT, new MessageAttributeValue { DataType = nameof(String), StringValue = isFromCloudwatchEvent.ToString() } },
                    },
                MessageBody = LogCommonStrings.SENDING_SQS_MESSAGE_TO_POND_GET_DEVICE_USAGE_LAMBDA,
                QueueUrl = PondDeviceUsageQueueURL
            };

            LogInfo(context, CommonConstants.INFO, request.MessageBody);

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }

    private async Task SendMessageToJasperDeviceCleanUpQueue(KeySysLambdaContext context, string? cleanUpQueueURL, int serviceProviderId)
    {
        LogInfo(context, CommonConstants.SUB, $"{cleanUpQueueURL}, {serviceProviderId}");

        if (string.IsNullOrEmpty(cleanUpQueueURL))
        {
            return;
        }

        var retryCount = 0;
        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
        {
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, cleanUpQueueURL));

            var request = new SendMessageRequest
            {
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.RETRY_COUNT, new MessageAttributeValue { DataType = nameof(String), StringValue = retryCount.ToString() } },
                        { SQSMessageKeyConstant.INTEGRATION_TYPE, new MessageAttributeValue { DataType = nameof(String), StringValue = ((int)IntegrationType.Pond).ToString() } },
                        { SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue { DataType = nameof(String), StringValue = serviceProviderId.ToString() } }
                    },
                MessageBody = LogCommonStrings.END_PROCESS_GET_DEVICES,
                QueueUrl = cleanUpQueueURL
            };

            LogInfo(context, CommonConstants.INFO, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, response.HttpStatusCode);
        }
    }
}
