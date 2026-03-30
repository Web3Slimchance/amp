## Examples

List all jobs for a dataset revision:
  $ ampctl dataset jobs my_namespace/my_dataset@1.0.0

List jobs for the latest version:
  $ ampctl dataset jobs my_namespace/my_dataset@latest

Output as JSON for scripting:
  $ ampctl dataset jobs my_namespace/my_dataset@1.0.0 --json

## Output

Returns job information including:
- Job ID (unique identifier)
- Status (Scheduled, Running, Completed, Stopped, Failed)
- Node ID (assigned worker)
- Creation and update timestamps