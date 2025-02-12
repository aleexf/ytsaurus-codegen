#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const TString CacheUserName("yt-clickhouse-cache");
const TString InternalRemoteUserName("$remote");
const std::vector<TString> TableAttributesToFetch{
    "id",
    // TODO(dakovalkov): Eliminate this with "schema_id" (CHYT-687).
    "schema",
    "type",
    "dynamic",
    "chunk_count",
    "external",
    "external_cell_tag",
    "revision",
    "boundary_keys",
    "enable_dynamic_store_read",
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

// This is an override of weak symbol from Common/Allocator.h.
// We do not want ClickHouse allocator to use raw mmaps as ytalloc already
// does that by himself.
__attribute__((__used__)) extern const size_t MMAP_THRESHOLD = static_cast<size_t>(1) << 60;
