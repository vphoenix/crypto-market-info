package mysql

import "strings"

// storageDDL installs the current physical storage policy without changing the
// reviewed catalog DDL. Catalog digests identify the existing logical mapping
// and frozen migration checkpoints; compression does not change typed values,
// keys or multiplicity. Existing tables require an explicit administrative ALTER.
func storageDDL(ddl string) string {
	return strings.Replace(ddl, "ENGINE=InnoDB ROW_FORMAT=DYNAMIC", "ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8", 1)
}
