#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void SimplifyPreserveTopologyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	BinaryExecutor::Execute<geometry_t, double, geometry_t>(
	    args.data[0], args.data[1], result, args.size(), [&](geometry_t input, double distance) {
		    auto geom = lstate.ctx.Deserialize(input);
		    auto simplified = make_uniq_geos(ctx, GEOSTopologyPreserveSimplify_r(ctx, geom.get(), distance));
		    return lstate.ctx.Serialize(result, simplified);
	    });
}

void GEOSScalarFunctions::RegisterStSimplifyPreserveTopology(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_SimplifyPreserveTopology");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(),
	                               SimplifyPreserveTopologyFunction, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
