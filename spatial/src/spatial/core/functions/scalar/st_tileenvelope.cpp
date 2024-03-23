#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace spatial {

namespace core {

static void TileEnvelopeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];
	auto &bounds = args.data[3];

	using INT_TYPE = PrimitiveType<uint8_t>;
	using GEOMETRY_TYPE = PrimitiveType<geometry_t>;

	GenericExecutor::ExecuteQuaternary<INT_TYPE, INT_TYPE, INT_TYPE, GEOMETRY_TYPE, GEOMETRY_TYPE>(
	    x, y, z, bounds, result, count,
	    [&](INT_TYPE x, INT_TYPE y, INT_TYPE z, GEOMETRY_TYPE bounds) {
			BoundingBox bbox;
			if (GeometryFactory::TryGetSerializedBoundingBox(bounds.val, bbox)) {
			}
		    uint32_t capacity = 5;
		    Polygon envelope_geom(lstate.factory.allocator, 1, &capacity, false, false);
		    auto &shell = envelope_geom[0];
		    // Create the exterior ring in CCW order
		    shell.Set(0, 0, 0);
		    shell.Set(1, 0, 0);
		    shell.Set(2, 0, 0);
		    shell.Set(3, 0, 0);
		    shell.Set(4, 0, 0);
		    return lstate.factory.Serialize(result, envelope_geom, false, false);
	    });
}

void CoreScalarFunctions::RegisterStTileEnvelope(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_TileEnvelope");

	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER, GeoTypes::GEOMETRY()},
	                               GeoTypes::GEOMETRY(), TileEnvelopeFunction, nullptr, nullptr, nullptr,
	                               GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial
