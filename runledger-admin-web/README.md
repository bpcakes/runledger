# @runledger/admin

Reusable, read-only Runledger administration for React applications. The package
does not choose your application router, query cache, CSS framework, or component
library.

```sh
npm install @runledger/admin react
```

Mount `runledger_admin::router` in the host backend at
`/api/admin/runledger/v1`. Authenticate the request in host middleware and add a
`runledger_admin::AdminAccess` extension before the shared router runs.

Use the client without React:

```ts
import { createRunledgerAdminClient } from "@runledger/admin/client";

const client = createRunledgerAdminClient({
  baseUrl: "/api/admin/runledger/v1",
  headers: () => ({ "X-CSRF-Token": readCsrfToken() }),
});
```

The client request paths, query parameters, and response types are generated
from the backend's OpenAPI 3.1 contract. The raw document is also exported as
`@runledger/admin/openapi.json` for host documentation or other generators.
`RunledgerAdminClient` remains the supported application boundary; generated
library types are intentionally not exported.

This minimal client performs typed Fetch calls and normalizes Runledger HTTP
errors. It rejects malformed or empty JSON responses, but it does not perform a
second full runtime schema validation of successful JSON payloads.

Render the controlled React panel:

```tsx
import { useState } from "react";
import { createRunledgerAdminClient } from "@runledger/admin/client";
import {
  RunledgerAdminPanel,
  type RunledgerAdminRoute,
} from "@runledger/admin/react";
import "@runledger/admin/styles.css";

const client = createRunledgerAdminClient();

export function RunledgerPage() {
  const [route, setRoute] = useState<RunledgerAdminRoute>({ name: "overview" });
  return (
    <RunledgerAdminPanel
      client={client}
      onRouteChange={setRoute}
      route={route}
    />
  );
}
```

Keep the client identity stable for the lifetime of the panel, as in the
module-scoped example above (or create it with `useMemo`). Passing a newly
created client on every render restarts the active reads and polling cycle.
List, detail, aggregate metrics, and capabilities each load once by default.
Set `pollIntervalMs` or `metricsPollIntervalMs` only after choosing a cadence
appropriate for the database size and number of open panels. Set
`capabilitiesPollIntervalMs` only when the host can change the current admin
grant without remounting the panel.

Translate `route` to your own URL if deep linking is needed. Override the
`--rla-*` CSS custom properties or omit the stylesheet and target the semantic
`rla-*` class names from the host design system.

The panel discovers its available sections from `/capabilities`; it does not
request the service-wide definition catalog unless the host granted that
resource. Job events and logs start with the newest records and expose Older /
Newer controls backed by opaque cursors. Offset-paged lists and workflow graph
collections use the API's `has_more` value instead of guessing from page length.
The API caps offsets at 10,000 skipped rows; narrow filters for deeper data.
