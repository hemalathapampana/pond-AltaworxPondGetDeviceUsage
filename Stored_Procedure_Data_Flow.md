## Stored procedure data flow and functionality

### [dbo].[usp_Teal_Update_Device_From_Staging]

- **Data flow (table ➜ table):**
  - **TealDeviceStaging ➜ TealDeviceDetailLastSyncDate**: Inserts one row recording the sync time, queue count (COUNT of staging rows), and `ServiceProviderId`.
  - **TealDeviceStaging ➜ TealDevice (MERGE/Upsert)**:
    - Match key: `EID` (and `TARGET.IsActive = 1`).
    - When matched: updates identifiers (ICCID/IMEI/MSISDN), plan fields, device status/id (mapped via `DeviceStatus` lookup), device metadata, SKU, billing fields (`BillMonth`, `BillYear`, `NextBillCycleDate`). Conditionally sets `LastActivatedDate` when status changes into Activated.
    - When not matched in target (new device for the same `ServiceProviderId`): inserts a new active `TealDevice` row with source fields and billing values.
    - When not matched in source (missing from staging for the same `ServiceProviderId`): updates existing `TealDevice` row to Unknown status and stamps modified metadata.
  - **TealDevice ➜ TealDeviceSyncAudit**: Inserts a daily summary row with counts of devices by status (pivoted into `ActiveCount` = ACTIVATED, `SuspendCount` = DEACTIVATED), along with bill period and provider.

- **Lookups/reads used (no writes):**
  - **DeviceStatus**: Joined to map case-insensitive `TealDeviceStaging.DeviceStatus` to `DeviceStatus.Id` for the Teal integration (Id = 12).

- **Functionality (in sentences):**
  - Logs the latest device sync by inserting a timestamped summary into `TealDeviceDetailLastSyncDate` using the current contents of `TealDeviceStaging`.
  - Upserts device master data from `TealDeviceStaging` into `TealDevice`, mapping textual statuses to IDs via `DeviceStatus` and maintaining plan, client, and billing fields.
  - Sets `LastActivatedDate` the moment a device transitions into the Activated status; devices missing from the current staging set are marked with Unknown status for the same provider.
  - Computes and inserts an audit snapshot in `TealDeviceSyncAudit` containing counts by status for the provider and bill period derived from `@BillingCycleEndDay` and current UTC time, all within a single transaction.

- **Key parameters/assumptions:**
  - `@ServiceProviderId` scopes staging, upsert, and Unknown-marking behavior to one provider.
  - `@BillMonth`, `@BillYear`, `@NextBillCycleDate` are written to `TealDevice` rows.
  - `@BillingCycleEndDay` influences bill period fields written to `TealDeviceSyncAudit`.

---

### [dbo].[usp_Teal_Truncate_Device_And_Usage_Staging]

- **Data flow (table ➜ table):**
  - None; this procedure clears data rather than moving it.

- **Functionality (in sentences):**
  - Truncates the following staging tables to reset them for fresh loads: `TealDeviceStaging`, `TealDeviceUsageStaging`, `TealDeviceUsageDailyStaging`, and `TealDeviceSMSUsageStaging`.