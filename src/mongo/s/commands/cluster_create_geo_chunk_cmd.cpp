/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <list>
#include <set>
#include <vector>

#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connpool.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/hasher.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/commands/cluster_write.h"
#include "mongo/s/config.h"
#include "mongo/s/config_server_client.h"
#include "mongo/s/grid.h"
#include "mongo/s/migration_secondary_throttle_options.h"
#include "mongo/s/shard_util.h"
#include "mongo/util/log.h"

namespace mongo {

using std::shared_ptr;
using std::list;
using std::set;
using std::string;
using std::vector;

namespace {

class CreateGeoChunkCmd : public Command {
public:
    CreateGeoChunkCmd() : Command("createGeoChunk", false, "creategeochunk") {}

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return true;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    virtual void help(std::stringstream& help) const {
        help << "Create a Geo chunk. Requires shardName, longitude and latitude."
             << " Sharding must already be enabled for the database.\n"
             << "   { enablesharding : \"<dbname>\" }\n";
    }

    virtual Status checkAuthForCommand(Client* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) {
        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forExactNamespace(NamespaceString(parseNs(dbname, cmdObj))),
                ActionType::enableSharding)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }

        return Status::OK();
    }

    virtual std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        uassert(ErrorCodes::InvalidNamespace, "Invalid namespace", nss.isValid());

        auto config = uassertStatusOK(grid.catalogCache()->getDatabase(txn, nss.db().toString()));
        uassert(ErrorCodes::IllegalOperation,
                str::stream() << "sharding not enabled for db " << nss.db(),
                config->isShardingEnabled());


        // NOTE: We *must* take ownership of the parameters here - otherwise the shared BSONObj
        // becomes corrupt as soon as the command ends.
        const char * shardName = cmdObj.getStringField("shardName");
        if (strcmp(shardName, "") == 0) {
            errmsg = "no shard name";
            return false;
        }

        double longitude = cmdObj.getField("longitude").Double();
        if (longitude < -90 || longitude > 90) {
            errmsg = "longitude must be between -90 and 90 degrees";
            return false;
        }

        double latitude = cmdObj.getField("latitude").Double();
        if (latitude < -180 || latitude > 180) {
            errmsg = "latitude must be between -180 and 180 degrees";
            return false;
        }
        
        bool shardExists = false;

        vector<ShardId> shardIds;
        grid.shardRegistry()->getAllShardIds(&shardIds);

        for (std::vector<ShardId>::const_iterator it = shardIds.begin(); it != shardIds.end(); it++) {
            if (it->toString() == shardName) {
                shardExists = true;
            }
        }

        if (!shardExists) {
            errmsg = "the given shard name wasn't found";
            return false;
        }

        LOG(0) << "CMD: createGeoChunk: " << cmdObj;
        
        // Make sure the cached metadata for the collection knows that we are now sharded
        //config = uassertStatusOK(grid.catalogCache()->getDatabase(txn, nss.db().toString()));
        //config->getChunkManager(txn, nss.ns(), true /* force */);

        //result << "collectionsharded" << nss.ns();
        result << "Not implemented yet";

        return true;
    }

} createGeoChunkCmd;

}  // namespace
}  // namespace mongo
