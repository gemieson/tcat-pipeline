Stand-ins for `google.cloud.storage` and `google.api_core.exceptions`, put on
`PYTHONPATH` only for the consumer process the tests start. Uploads become
files under `$FAKE_GCS_DIR`. Faults are injected by writing a count to
`$FAKE_GCS_DIR/.fault_<name>`:

- `fail`: the upload raises `ServiceUnavailable` (the client's retries ran out)
- `lose_response`: the object is stored, then the call raises
  `PreconditionFailed`, which is what the real client raises when it retries
  a create-only upload whose first attempt succeeded but whose response was lost
