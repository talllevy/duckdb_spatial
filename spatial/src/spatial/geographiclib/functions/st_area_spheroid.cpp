#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/functions/common.hpp"

#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"
#include "GeographicLib/PolygonArea.hpp"

#include "cmath"

namespace spatial {

namespace geographiclib {

using namespace core;

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------

static void GeodesicPolygon2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();

	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();
	auto polygon_area = GeographicLib::PolygonArea(geod, false);

	UnaryExecutor::Execute<list_entry_t, double>(input, result, count, [&](list_entry_t polygon) {
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;

		bool first = true;
		double area = 0;
		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			polygon_area.Clear();
			// Note: the last point is the same as the first point, but geographiclib doesn't know that,
			// so skip it.
			for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
				polygon_area.AddPoint(x_data[coord_idx], y_data[coord_idx]);
			}
			double ring_area;
			double _perimeter;
			polygon_area.Compute(false, true, _perimeter, ring_area);
			if (first) {
				// Add outer ring
				area += std::abs(ring_area);
				first = false;
			} else {
				// Subtract holes
				area -= std::abs(ring_area);
			}
		}
		return std::abs(area);
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static double PolygonArea(const Polygon &poly, GeographicLib::PolygonArea &comp) {
	double total_area = 0;
	for (uint32_t ring_idx = 0; ring_idx < poly.RingCount(); ring_idx++) {
		comp.Clear();
		auto &ring = poly[ring_idx];
		// Note: the last point is the same as the first point, but geographiclib doesn't know that,
		for (uint32_t coord_idx = 0; coord_idx < ring.Count() - 1; coord_idx++) {
			auto coord = ring.Get(coord_idx);
			comp.AddPoint(coord.x, coord.y);
		}
		double ring_area;
		double _perimeter;
		// We use the absolute value here so that the actual winding order of the polygon rings dont matter.
		comp.Compute(false, true, _perimeter, ring_area);
		if (ring_idx == 0) {
			// Add outer ring
			total_area += std::abs(ring_area);
		} else {
			// Subtract holes
			total_area -= std::abs(ring_area);
		}
	}
	return std::abs(total_area);
}

static double GeometryArea(const Geometry &geom, GeographicLib::PolygonArea &comp) {
	switch (geom.Type()) {
	case GeometryType::POLYGON: {
		auto &poly = geom.As<Polygon>();
		return PolygonArea(poly, comp);
	}
	case GeometryType::MULTIPOLYGON: {
		auto &mpoly = geom.As<MultiPolygon>();
		double total_area = 0;
		for (auto &poly : mpoly) {
			total_area += PolygonArea(poly, comp);
		}
		return total_area;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &coll = geom.As<GeometryCollection>();
		double total_area = 0;
		for (auto &item : coll) {
			total_area += GeometryArea(item, comp);
		}
		return total_area;
	}
	default: {
		return 0.0;
	}
	}
}

static void GeodesicGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();
	auto comp = GeographicLib::PolygonArea(geod, false);

	UnaryExecutor::Execute<geometry_t, double>(input, result, count, [&](geometry_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		return GeometryArea(geometry, comp);
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void GeographicLibFunctions::RegisterArea(DatabaseInstance &db) {

	// Area
	ScalarFunctionSet set("ST_Area_Spheroid");
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, GeodesicPolygon2DFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeodesicGeometryFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geographiclib

} // namespace spatial