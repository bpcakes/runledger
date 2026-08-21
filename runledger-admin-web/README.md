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

Translate `route` to your own URL if deep linking is needed. Override the
`--rla-*` CSS custom properties or omit the stylesheet and target the semantic
`rla-*` class names from the host design system.

The panel discovers its available sections from `/capabilities`; it does not
request the service-wide definition catalog unless the host granted that
resource. Job events and logs start with the newest records and expose Older /
Newer controls backed by opaque cursors. Offset-paged lists and workflow graph
collections use the API's `has_more` value instead of guessing from page length.
