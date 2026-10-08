/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package orcraft

import (
	"io"
)

// SnapshotCreatorApplier produces and restores the snapshot of the whole FSM. This
// package treats the snapshot as opaque bytes (file_snapshot.go only checksums
// them; rel_store.go stores log entries and stable keys, no tenant dependence), so
// there is exactly ONE snapshot for the Raft group, whatever the number of tenants.
//
// Contract for the implementation (go/logic) of the multi-tenant format:
//   - GetData writes FormatVersion:2 with Tenants map[tenantID]TenantSnapshot and the
//     global fields (e.g. LeaderURI) at top level.
//   - Restore must accept v1 (no FormatVersion/Tenants): the whole content belongs
//     to LegacyTenantID. encoding/json ignores unknown fields, so v1 decodes.
//   - Restore applies each tenant only to its own local runtime; a tenant present in
//     the snapshot but not local is skipped with a warning; a local tenant absent
//     from the snapshot is left untouched. A tenant that could not be read at
//     GetData time must be omitted or flagged, never emitted as empty.
//   - GetData runs in Persist concurrently with Apply, and Restore at startup before
//     Apply of newer entries: both may be invoked with no leadership.
type SnapshotCreatorApplier interface {
	GetData() (data []byte, err error)
	Restore(rc io.ReadCloser) error
}
