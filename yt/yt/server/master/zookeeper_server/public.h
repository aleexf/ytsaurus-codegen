#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/zookeeper_master/public.h>

namespace NYT::NZookeeperServer {

////////////////////////////////////////////////////////////////////////////////

using NZookeeperClient::TZookeeperPath;

using NZookeeperMaster::TZookeeperShardId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IZookeeperManager)

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(TZookeeperShard, TZookeeperShardId, NObjectClient::TDirectObjectIdHash)
DECLARE_MASTER_OBJECT_TYPE(TZookeeperShard)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
