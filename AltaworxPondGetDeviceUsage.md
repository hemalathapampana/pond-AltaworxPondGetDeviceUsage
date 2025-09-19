## AltaworxPondGetDeviceUsage Lambda — Flow & Method Reference

A comprehensive, implementation-oriented reference for the Lambda that synchronizes device usage data (CDR/usage records) from Pond into the database. This document explains the end-to-end flow, configuration, and each method/function in detail for both initialization and page-processing modes.

---

## Purpose & Overview

- **Primary goal**: Synchronize device usage data from Pond into staging tables and trigger downstream processing.
- **Two modes**:
  - **Initialization mode** (no ServiceProviderId in SQS message): Seeds work by discovering pages, creating page records, and enqueueing one SQS message per page.
  - **Processing mode** (ServiceProviderId present): Fetches one usage page, stages the data, and notifies downstream processors.
- **Key actions**:
  - Discover total page count per service provider.
  - Enqueue page-processing SQS messages.
  - For each page: fetch usage from Pond, bulk copy to staging, emit a progress/completion message.

---

## High-Level Flow

- Receives SQS batch → iterate records
- For each record:
  - Parse attributes (ServiceProviderId, PageNumber, IsSuccessful)
  - If ServiceProviderId missing/<=0 → run initialization
  - Else → run page-processing for the specified provider/page
- Handle errors, log diagnostics, cleanup

---

## Entry Point: FunctionHandler

- **Signature**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Input**:
  - `SQSEvent` containing one or more SQS records
  - `ILambdaContext` for AWS Lambda context
- **Responsibilities**:
  1. Initialize the base Lambda context via `BaseAmopFunctionHandler()` (from AwsFunctionBase) to set up logging, configuration, and DB access.
  2. Load environment variables using `TryGetAllEnvironmentVariables()`:
     - `POND_GET_DEVICE_USAGE_QUEUE_URL`
     - `POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL`
     - `POND_GET_DEVICE_USAGE_ENDPOINT`
     - `PAGE_SIZE` (default: `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)
  3. Validate the SQS trigger; iterate through each record:
     - Log record ID/body/attributes for diagnostics
     - Use `GetMessageValues()` to parse attributes into `SqsValues`:
       - `ServiceProviderId` (int)
       - `PageNumber` (int)
       - `IsSuccessful` (bool; for downstream stage processing)
  4. Route the message:
     - If `ServiceProviderId` is missing or <= 0 → call `InitializeSyncDeviceUsageProcess()`
     - Else → call `ProcessSyncPageByServiceProviderId()`
  5. Handle exceptions per record; ensure `CleanUp()` is called after processing.
- **Error handling**:
  - Try/catch around record processing; log exception context (SP, page)
  - Continue processing other records in the batch where possible
- **Observability**:
  - Structured logs for start/finish, env config, routing decision, timing
  - Metrics/timers around API calls and bulk copy operations

---

## Initialization Mode: InitializeSyncDeviceUsageProcess

- **Signature**: `InitializeSyncDeviceUsageProcess(AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository)`
- **Purpose**: Seed page work and enqueue page-processing messages for all Pond service providers.
- **Inputs**:
  - `AmopLambdaContext` (includes DB connections, logger, env config)
  - `ServiceProviderRepository` (for enumerating Pond-integrated service providers)
- **Behavior**:
  1. Reset staging via `pondRepository.TruncateStagingTables()` to ensure a clean run.
  2. Fetch Pond-enabled service provider IDs via `serviceProviderRepository.GetAllServiceProviderIds(IntegrationType.Pond)`.
  3. For each `serviceProviderId`:
     - Retrieve auth via `pondRepository.GetPondAuthentication(serviceProviderId)`
     - Compute total page count via `pondApiService.TryGetTotalPageCount(PondGetDeviceUsageEndpoint, PageSize)`
     - Persist page markers via `LoadPagesToProcessTable(context, serviceProviderId, totalPages)` into `POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS`
     - For each page in `[0, totalPages)` → `InitGetDeviceUsagePages(context, serviceProviderId, page)` to enqueue an SQS message on `POND_GET_DEVICE_USAGE_QUEUE_URL`
- **Edge cases**:
  - `totalPages == 0`: skip enqueueing; optionally log no-work needed
  - Missing/invalid auth: log and continue (or fail fast based on policy)
- **Concurrency**:
  - Fan-out by pages enables parallel processing across multiple Lambdas/SQS consumers

---

## Processing Mode: ProcessSyncPageByServiceProviderId

- **Signature**: `ProcessSyncPageByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)`
- **Purpose**: Fetch one page of device usage from Pond and load it into staging.
- **Inputs**:
  - `AmopLambdaContext`
  - `SqsValues` with `ServiceProviderId` and `PageNumber`
- **Behavior**:
  1. Retrieve Pond authentication via `pondRepository.GetPondAuthentication(serviceProviderId)`
  2. Instantiate `PondApiService`
  3. Call `SyncDeviceUsage(context, sqsValues, sqlTransientRetryPolicy, pondApiService)` to orchestrate:
     - Fetch one page from Pond API
     - Bulk copy to staging (`PondDeviceUsageStaging`)
     - Emit progress/completion message to `POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL`
- **Error handling**:
  - Wrap API and DB operations in a SQL transient retry policy (e.g., `RetryPolicyHelper`)
  - On failure, log context and optionally re-throw to allow SQS redrive/dead-letter

---

## Orchestrator: SyncDeviceUsage

- **Signature**: `SyncDeviceUsage(AmopLambdaContext context, SqsValues sqsValues, IRetryPolicy sqlTransientRetryPolicy, PondApiService pondApiService)`
- **Purpose**: End-to-end orchestration for a single page.
- **Steps**:
  1. Compute `offset = sqsValues.PageNumber * PageSize`.
  2. Invoke `GetSinglePageListFromPondAPIAsync<PondDeviceUsageItem, PondDeviceUsageListResponse>(endpoint, offset, PageSize)`:
     - Uses `HttpClientSingleton.Instance` and `PondApiService.GetPondListAsync`
     - Extracts `IEnumerable<PondDeviceUsageItem>` via `response => response.Elements`
  3. Transform items to `DataTable` and call `LoadDeviceUsageToStagingTable(context, serviceProviderId, dataTable)`.
  4. Call `CheckSyncDeviceUsageStepProgress(context, sqsValues.ServiceProviderId, sqsValues.PageNumber, isSuccessful: true)` to emit a downstream message.
  5. On error, emit `isSuccessful: false` and re-throw or handle per policy.
- **Retries**:
  - SQL bulk copy wrapped in `sqlTransientRetryPolicy`
  - Upstream SQS/Lambda retries handle transient failures

---

## API Page Fetch: GetSinglePageListFromPondAPIAsync

- **Signature**: `GetSinglePageListFromPondAPIAsync<TItem, TResponse>(string endpoint, int offset, int pageSize)`
- **Purpose**: Retrieve a single page of Pond list data.
- **Inputs**:
  - `endpoint`: `POND_GET_DEVICE_USAGE_ENDPOINT`
  - `offset`: `pageNumber * pageSize`
  - `pageSize`: `PAGE_SIZE`
- **Behavior**:
  - Build request with query parameters `offset` and `limit` (or equivalent, depending on Pond spec)
  - Send via `HttpClientSingleton.Instance` through `PondApiService.GetPondListAsync`
  - Deserialize to `TResponse` and map to `IEnumerable<TItem>` via the provided selector (here: `response => response.Elements`)
- **Output**: `IEnumerable<PondDeviceUsageItem>` representing device usage records
- **Errors**: HTTP errors, deserialization failures, auth issues (handled/logged in caller)

---

## Staging Load: LoadDeviceUsageToStagingTable

- **Signature**: `LoadDeviceUsageToStagingTable(AmopLambdaContext context, int serviceProviderId, IEnumerable<PondDeviceUsageItem> items)`
- **Purpose**: Bulk insert one page of usage data into the staging table.
- **Process**:
  1. Build a `DataTable` with the following columns:
     - `Id`
     - `Iccid`
     - `Msisdn`
     - `Subscriber_Id`
     - `Device_Id`
     - `UsageType`
     - `UsageAmount`
     - `UsageUnit`
     - `StartDate`
     - `EndDate`
     - `CreatedDate`
     - `ServiceProviderId`
  2. Populate rows from `items`, adding `ServiceProviderId` to each row.
  3. Execute `SqlBulkCopy` into `DatabaseTableNames.PondDeviceUsageStaging`.
- **Validation**:
  - Ensure types and nullability match DB schema
  - Normalize/convert units and dates if required by schema
- **Performance**:
  - Use table-valued bulk copy for throughput; batch size tuned via config when available

---

## Page Tracking Seed: LoadPagesToProcessTable

- **Signature**: `LoadPagesToProcessTable(AmopLambdaContext context, int serviceProviderId, int totalPages)`
- **Purpose**: Persist all page markers for later progress tracking.
- **Process**:
  1. Create a `DataTable` with columns: `ServiceProviderId`, `PageNumber`.
  2. For `page in [0, totalPages)` add a row for each page.
  3. Execute `SqlBulkCopy` into `DatabaseTableNames.POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS`.
- **Usage**:
  - Enables downstream components (e.g., `UpdateDeviceUsagePageStatusAndCheckSyncProgress`) to update per-page status and detect completion.

---

## Page Enqueue: InitGetDeviceUsagePages

- **Signature**: `InitGetDeviceUsagePages(AmopLambdaContext context, int serviceProviderId, int pageNumber)`
- **Purpose**: Enqueue a single page-processing SQS message.
- **Behavior**:
  - Publish to `POND_GET_DEVICE_USAGE_QUEUE_URL` (env var)
  - Message attributes:
    - `SERVICE_PROVIDER_ID`: the `serviceProviderId`
    - `PAGE_NUMBER`: the `pageNumber`
  - Message body may be minimal; attributes carry routing data
- **Reliability**:
  - SQS delivery guarantees combined with Lambda retries ensure at-least-once processing

---

## Progress Notification: CheckSyncDeviceUsageStepProgress

- **Signature**: `CheckSyncDeviceUsageStepProgress(AmopLambdaContext context, int serviceProviderId, int pageNumber, bool isSuccessful)`
- **Purpose**: Notify downstream processors that one page has been staged (success or failure).
- **Behavior**:
  - Publish to `POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL`
  - Message attributes:
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL`
  - Downstream consumers update page status and possibly trigger aggregation/finalization

---

## Utilities

### GetMessageValues
- **Purpose**: Parse SQS message attributes into a typed structure (`SqsValues`).
- **Reads attributes** (via `SQSMessageKeyConstant`):
  - `SERVICE_PROVIDER_ID` (int)
  - `PAGE_NUMBER` (int)
  - `IS_SUCCESSFUL` (bool; mainly for downstream progress messages)
- **Validation**:
  - Missing/invalid `ServiceProviderId` routes to initialization mode
  - Missing `PageNumber` in processing mode causes validation failure

### TryGetAllEnvironmentVariables
- **Purpose**: Load Lambda configuration and provide defaults.
- **Environment variables**:
  - `POND_GET_DEVICE_USAGE_QUEUE_URL`
  - `POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL`
  - `POND_GET_DEVICE_USAGE_ENDPOINT`
  - `PAGE_SIZE` (default `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)
- **Notes**:
  - Validate URIs and integers; log final effective values

### InitializeRepositories
- **Purpose**: Construct repositories and services using the central DB connection and context.
- **Creates**:
  - `PondRepository` (Pond-specific data access and auth retrieval)
  - `ServiceProviderRepository` (SP enumeration and metadata)
- **Dependencies**:
  - `CentralDbConnectionString` obtained from configuration (via `AwsFunctionBase`)

---

## Key Dependencies & Integrations

- **AwsFunctionBase**: Common Lambda bootstrapping, logging, configuration, SQL bulk copy, cleanup
- **PondRepository**: Auth retrieval, CRUD for staging and progress tracking tables
- **PondApiService**: API calls to Pond, request building, pagination parameters
- **ServiceProviderRepository**: Enumerate Pond-linked service providers
- **EnvironmentRepository**: Access to env vars, typed parsing
- **SqsService**: SQS message publishing abstraction
- **RetryPolicyHelper**: SQL transient retry policy (e.g., exponential backoff)
- **HttpClientSingleton / HttpRequestFactory**: HTTP client reuse, request construction

---

## Configuration

- `POND_GET_DEVICE_USAGE_QUEUE_URL`: SQS queue for per-page processing messages
- `POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL`: SQS queue for downstream staged-processing
- `POND_GET_DEVICE_USAGE_ENDPOINT`: Pond API endpoint for device usage
- `PAGE_SIZE`: Number of items to fetch per page (default from `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)

---

## Data & Tables

- **Staging table**: `DatabaseTableNames.PondDeviceUsageStaging`
  - Columns: `Id`, `Iccid`, `Msisdn`, `Subscriber_Id`, `Device_Id`, `UsageType`, `UsageAmount`, `UsageUnit`, `StartDate`, `EndDate`, `CreatedDate`, `ServiceProviderId`
- **Page tracking table**: `DatabaseTableNames.POND_GET_DEVICE_USAGE_PAGE_TO_PROCESS`
  - Columns: `ServiceProviderId`, `PageNumber` (and any status columns managed downstream)

---

## SQS Message Attribute Schema

- **GetDeviceUsage (page-processing) queue** (`POND_GET_DEVICE_USAGE_QUEUE_URL`):
  - `SERVICE_PROVIDER_ID`: number
  - `PAGE_NUMBER`: number
- **ProcessStagedDeviceUsage queue** (`POND_PROCESS_STAGED_DEVICE_USAGE_QUEUE_URL`):
  - `SERVICE_PROVIDER_ID`: number
  - `PAGE_NUMBER`: number
  - `IS_SUCCESSFUL`: boolean

---

## Control Flow Details

1. Initialization SQS message arrives with missing/invalid `ServiceProviderId`:
   - Truncate staging
   - Enumerate SPs, get `totalPages` per SP from Pond
   - Seed page markers into DB
   - Enqueue one SQS message per page
2. Processing SQS message arrives with `ServiceProviderId` and `PageNumber`:
   - Fetch auth and build API client
   - Calculate `offset = pageNumber * pageSize`
   - Fetch page from Pond
   - Bulk copy into staging
   - Emit progress message (`IS_SUCCESSFUL = true`)

---

## Operational Considerations

- **Idempotency**:
  - Staging table can be truncated on initialization; per-page processing should be safe to retry due to at-least-once delivery
  - Consider dedupe keys in staging if upstream can resend overlapping pages
- **Concurrency**:
  - Page-level fan-out allows horizontal scaling; ensure DB locks/bulk copy settings handle concurrency
- **Error handling & retries**:
  - Lambda retries on unhandled exceptions; SQL operations use transient retry
  - Consider DLQ on both queues
- **Timeouts**:
  - Ensure Lambda timeout exceeds worst-case Pond API latency + bulk copy time
  - Use HTTP client timeouts and retry/backoff appropriately
- **Security**:
  - IAM permissions for SQS send/receive and Secrets/SSM if used for DB/auth
  - Secure handling of authentication to Pond (no logging secrets)
- **Observability**:
  - Emit metrics for pages discovered, pages processed, bulk copy rows, failures
  - Correlate logs by `ServiceProviderId` and `PageNumber`

---

## Examples

### Example: Initialization-trigger message (attributes)
```text
SERVICE_PROVIDER_ID = (missing or 0)
```

### Example: Page-processing message (attributes)
```text
SERVICE_PROVIDER_ID = 123
PAGE_NUMBER = 7
```

### Example: Downstream progress message (attributes)
```text
SERVICE_PROVIDER_ID = 123
PAGE_NUMBER = 7
IS_SUCCESSFUL = true
```

---

## Quick Reference (Methods)

- `FunctionHandler(SQSEvent, ILambdaContext)`: Entry point; routes to init or process
- `InitializeSyncDeviceUsageProcess(context, serviceProviderRepository)`: Seed pages and enqueue
- `ProcessSyncPageByServiceProviderId(context, sqsValues)`: Process a single page
- `SyncDeviceUsage(context, sqsValues, retryPolicy, pondApiService)`: Orchestrate fetch → stage → notify
- `GetSinglePageListFromPondAPIAsync<TItem, TResponse>(endpoint, offset, pageSize)`: Fetch one page
- `LoadDeviceUsageToStagingTable(context, serviceProviderId, items)`: Bulk copy to staging
- `LoadPagesToProcessTable(context, serviceProviderId, totalPages)`: Seed page markers
- `InitGetDeviceUsagePages(context, serviceProviderId, pageNumber)`: Enqueue page-processing message
- `CheckSyncDeviceUsageStepProgress(context, serviceProviderId, pageNumber, isSuccessful)`: Notify downstream
- `GetMessageValues(record)`: Parse SQS attributes
- `TryGetAllEnvironmentVariables()`: Load env/config
- `InitializeRepositories(connectionString)`: Construct repositories/services

---

## Glossary

- **Pond**: External platform providing device usage records
- **Page**: A chunk of results determined by `PAGE_SIZE` and `offset`
- **Staging**: Intermediate DB area for raw usage data prior to transformation/processing
- **Downstream**: Subsequent Lambda or service that processes staged usage

---

If you need this document in a different structure or with code snippets from your implementation, let me know and I can augment sections with concrete examples.