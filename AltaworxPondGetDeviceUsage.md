## AltaworxPondGetDeviceUsage Lambda Flow Documentation

### Overview
The AltaworxPondGetDeviceUsage Lambda function synchronizes device usage data (e.g., CDR/usage records) from the Pond platform into the database. It can initialize a device usage sync session (seeding page work) and then process device usage pages in batches via SQS messages.

### HIGH-LEVEL FLOW (Sequential Function Flow)

#### Main Entry Point
- **FunctionHandler (SQSEvent sqsEvent, ILambdaContext context)**
  - **Receives**: SQS event and Lambda context
  - **Initializes**: base function handler
  - **Iterates**: through SQS records and routes per-message

#### Initialization Flow (ServiceProviderId not supplied in SQS message)
- **InitializeSyncDeviceUsageProcess**
  - **TruncateStagingTables**: staging reset
  - **GetAllServiceProviderIds(IntegrationType.Pond)**
  - **For each Service Provider (SP)**:
    - **GetPondAuthentication**
    - **TryGetTotalPageCount** via Pond API (PondGetDeviceUsageEndpoint, PageSize)
    - **LoadPagesToProcessTable**: seed page markers
    - **InitGetDeviceUsagePages**: enqueue one SQS message per page
    - **Shape**

#### Processing Flow (ServiceProviderId supplied in SQS message)
- **ProcessSyncPageByServiceProviderId**
  - **GetPondAuthentication**
  - **Instantiate** PondApiService
  - **SyncDeviceUsage**
    - **GetSinglePageListFromPondAPIAsync** (paged fetch)
    - **LoadDeviceUsageToStagingTable** (bulk copy to staging)
    - **CheckSyncDeviceUsageStepProgress** (emit progress/completion message)
  - **Shape**

### LOW-LEVEL FLOW (Detailed Method Explanations)

#### FunctionHandler (Main Entry Point)
- **Input**: SQSEvent sqsEvent, ILambdaContext context
- **Purpose**: Processes SQS messages to orchestrate device usage synchronization
- **What happens**:
  - Initializes AmopLambdaContext via BaseAmopFunctionHandler()
  - Reads environment variables via TryGetAllEnvironmentVariables():
    - POND_GET_DEVICE_USAGE_QUEUE_URL (PondHelper.CommonString.POND_GET_DEVICE_USAGE_QUEUE_URL_VARIABLE_KEY)
    - POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL (PondHelper.CommonString.POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL_VARIABLE_KEY)
    - POND_GET_DEVICE_USAGE_ENDPOINT (PondHelper.CommonString.POND_GET_DEVICE_USAGE_ENDPOINT_VARIABLE_KEY)
    - PAGE_SIZE (PondHelper.CommonString.PAGE_SIZE, default PondHelper.CommonConfig.DEFAULT_PAGE_SIZE)
  - Ensures SQS trigger validity and iterates each record
  - For each record:
    - Logs diagnostics
    - Parses attributes with GetMessageValues()
      - ServiceProviderId (required for processing mode)
      - PageNumber (required for processing mode)
      - IsSuccessful (used in downstream stage processing queue)
    - If ServiceProviderId <= 0 or missing: routes to InitializeSyncDeviceUsageProcess()
    - Else: routes to ProcessSyncPageByServiceProviderId()
  - Handles exceptions and calls CleanUp()

#### InitializeSyncDeviceUsageProcess (Initialization Mode)
- **Input**: AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository
- **Purpose**: Seeds the sync process and fans out processing across pages
- **What happens**:
  - Reset staging via pondRepository.TruncateStagingTables
  - Retrieve all Pond service provider IDs via GetAllServiceProviderIds
  - For each serviceProviderId:
    - Retrieve auth via pondRepository.GetPondAuthentication
    - Call API once to get total page count via pondApiService.TryGetTotalPageCount<T> using PondGetDeviceUsageEndpoint
    - LoadPagesToProcessTable(context, serviceProviderId, totalPages) seeds DB page markers (POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS)
    - For page in [0, totalPages):
      - InitGetDeviceUsagePages(context, serviceProviderId, page) enqueues SQS message to GetDeviceUsageQueueURL
    - Shape

#### ProcessSyncPageByServiceProviderId (Processing Mode)
- **Input**: AmopLambdaContext context, SqsValues sqsValues
- **Purpose**: Pulls one page of device usage records from Pond and loads to staging
- **What happens**:
  - Retrieve auth via pondRepository.GetPondAuthentication
  - Create PondApiService
  - SyncDeviceUsage(context, sqsValues, sqlTransientRetryPolicy, pondApiService)
    - Calls GetSinglePageListFromPondAPIAsync<PondDeviceUsageItem, PondDeviceUsageListResponse>:
      - Calculates offset = pageNumber * PageSize
      - Fetches from Pond via GetPondListAsync<PondDeviceUsageListResponse>(HttpClientSingleton.Instance, PondGetDeviceUsageEndpoint, offset, PageSize)
      - Extracts list via response => response.Elements
    - LoadDeviceUsageToStagingTable builds a DataTable and executes SqlBulkCopy to PondDeviceUsageStaging
    - On each page, calls CheckSyncDeviceUsageStepProgress with IsSuccessful, which emits an SQS message to ProcessStagedDeviceUsageQueueURL for downstream processing
  - Shape

### Utility Functions

- **GetMessageValues**
  - Parses SQS attributes from the incoming message into SqsValues
  - Attributes used (via SQSMessageKeyConstant):
    - SERVICE_PROVIDER_ID
    - PAGE_NUMBER
    - IS_SUCCESSFUL (used for progress/completion signaling downstream)

- **TryGetAllEnvironmentVariables**
  - Reads Lambda, API, and sync configuration from environment variables:
    - POND_GET_DEVICE_USAGE_QUEUE_URL
    - POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL
    - POND_GET_DEVICE_USAGE_ENDPOINT
    - PAGE_SIZE

- **InitializeRepositories**
  - Instantiates PondRepository and ServiceProviderRepository using CentralDbConnectionString

- **LoadDeviceUsageToStagingTable**
  - Shapes DataTable schema with columns (typical):
    - Id
    - Iccid
    - Msisdn
    - Subscriber_Id
    - Device_Id
    - UsageType
    - UsageAmount
    - UsageUnit
    - StartDate
    - EndDate
    - CreatedDate
    - ServiceProviderId
  - Executes SqlBulkCopy into DatabaseTableNames.PondDeviceUsageStaging

- **LoadPagesToProcessTable**
  - Builds DataTable with PageNumber and ServiceProviderId for all pages
  - Executes SqlBulkCopy into DatabaseTableNames.POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS

- **InitGetDeviceUsagePages**
  - Sends an SQS message per page to GetDeviceUsageQueueURL with attributes:
    - SERVICE_PROVIDER_ID
    - PAGE_NUMBER

- **CheckSyncDeviceUsageStepProgress**
  - Sends an SQS message to ProcessStagedDeviceUsageQueueURL with attributes:
    - SERVICE_PROVIDER_ID
    - PAGE_NUMBER
    - IS_SUCCESSFUL

- **SyncDeviceUsage**
  - Orchestrates single-page fetch, staging load, and progress signaling using retry policy

### Key Dependencies and Integrations
- **AwsFunctionBase**: logging, config, DB connections, bulk copy, cleanup
- **PondRepository**: DB CRUD for Pond sync, staging, and progress tracking
- **PondApiService**: list API calls, query param construction, request building
- **ServiceProviderRepository**: service provider enumeration and metadata
- **EnvironmentRepository**: environment variable access
- **SqsService**: SQS message publishing
- **RetryPolicyHelper**: SQL transient retry policy
- **HttpClientSingleton and HttpRequestFactory**: HTTP client and request construction
- **Shape**

### Data Flow Summary
- **Initialization**: seed page markers per service provider and enqueue SQS messages per page
- **Fetch**: pull one page of device usage from Pond using offset = pageNumber * pageSize
- **Stage**: bulk insert to PondDeviceUsageStaging
- **Advance**: emit progress messages to ProcessStagedDeviceUsageQueueURL for downstream processing
- **Note**: Page-to-process tracking is staged into POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS; downstream components can update progress via repository methods (e.g., UpdateDeviceUsagePageStatusAndCheckSyncProgress) as applicable
- **Shape**

## AltaworxPondGetDeviceUsage — Integration & Operations Guide

### 1) Triggers & Scheduling
- **Publisher of initial SQS messages**: This Lambda, when invoked without ServiceProviderId or with an empty SQS event, enumerates total pages and enqueues one SQS message per page to the Get-Device-Usage SQS queue.
- **EventBridge schedule**: Triggered by AWS EventBridge.
  - **Cron**: 0 9 * * ? *
  - **Time zone**: UTC
  - **Frequency**: Daily at 09:00 UTC
  - **Next runs (examples)**: Fri, 19 Sep 2025 09:00 UTC; Sat, 20 Sep 2025 09:00 UTC (and daily thereafter)
  - This means your Lambda will run once daily at 9:00 AM UTC, which is 2:30 PM IST.

### 2) Message Handling
- **SQS message attributes (seed/page messages)**:
  - SERVICE_PROVIDER_ID
  - PAGE_NUMBER
- **SQS message attributes (progress messages)**:
  - SERVICE_PROVIDER_ID
  - PAGE_NUMBER
  - IS_SUCCESSFUL
- **Continuation for pagination**: One SQS message per page; each message processes exactly one page.
- **Manual/default invocation**: If the event has no SERVICE_PROVIDER_ID, the Lambda initializes a full run: truncates staging, computes total pages, and enqueues page messages.

### 3) Batch & Pagination
- **Configured API page size**: Default 10; can be overridden via env var PAGE_SIZE.
- **Pagination mechanics**: offset = pageNumber * pageSize, count = pageSize.
- **Completion determination**: Each page emits a progress message; DB page status is updated by the downstream processor, which determines when all pages are complete.

### 4) Integration Details (Authentication)
- **Credential source**: DB stored procedure GET_POND_AUTHENTICATION; decoded into PondAuthentication.
- **Usage**:
  - **List endpoints**: header x-api-key: <APIKey> plus Accept: application/json.
  - **Token** (if needed by other endpoints) supplied via TokenValue.

### 5) Data Handling & Staging
- **Staging tables**:
  - PondDeviceUsageStaging (bulk-inserts usage page results)
  - POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS (pages to process)
- **Clearing staging**: At start of initialize run via POND_TRUNCATE_STAGING.
- **Final sync to AMOP 2.0 tables**: Performed by downstream “process staged device usage” flow using stored procedures (e.g., UPDATE_POND_DEVICE_USAGE_FROM_STAGING) after all page data is staged.

### 6) Error Handling & Retry
- **HTTP retries (Polly)**:
  - **Attempts**: Config-driven (CommonConstants.NUMBER_OF_RETRIES)
  - **Backoff**: Exponential, delay = API_ERROR_DELAY_IN_SECONDS^attempt seconds
  - **Retries** on exceptions and non-2xx responses; logs details.
- **API failures**: Logged; page marked unsuccessful via progress message. Data load for that page is skipped or empty.
- **Re-enqueue of incomplete jobs**: Not handled by this Lambda; retried on next scheduled run or by downstream processor policy.

### 7) Failed/Unprocessed Records
- **Validation failures**: This Lambda stages raw device usage items; validation/merging is downstream.
- **Failure logging**: CloudWatch logs; page-level success/failure emitted via progress SQS and recorded in DB by downstream flow.
- **Retry policy**: Via daily schedule or downstream logic; no automatic per-record retry here.

### 8) Cleanup Processes
- **Retention (DaysToKeep)**: Not implemented in this Lambda.
- **Cleanup batch size (RecordsPerCycle)**: Not implemented in this Lambda.
- **Cleanup logging**: Not applicable.

### 9) Notifications & Reporting
- **Notifications**: None in this Lambda beyond CloudWatch logs.
- **Sync summary reports**: Not produced here. Progress captured via SQS and DB flags by the downstream processor.

### 10) External Dependencies (Prerequisites)
- **Environment variables**:
  - POND_GET_DEVICE_USAGE_QUEUE_URL_VARIABLE_KEY
  - POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL_VARIABLE_KEY
  - POND_GET_DEVICE_USAGE_ENDPOINT_VARIABLE_KEY
  - PAGE_SIZE (optional; default 10)
- **Infrastructure**:
  - EventBridge rule with cron 0 9 * * ? * (UTC) targeting this Lambda
  - SQS queues for “get device usage” and “process staged device usage”
  - DB connectivity for credentials and staging/final merge
  - Outbound access to Pond API
- **Credentials and API info**:
  - As those Usage Comes from Sftp

| SettingKey       | SettingValue                                   |
|------------------|-------------------------------------------------|
| PondSFTPPassword | R2FcLDg+PCQ=                                    |
| PondSFTPPath     | /Carriers/Altaworx/Pond/CDRs                    |
| PondSFTPServer   | ec2-35-169-206-35.compute-1.amazonaws.com       |
| PondSFTPUsername | jasper                                          |

