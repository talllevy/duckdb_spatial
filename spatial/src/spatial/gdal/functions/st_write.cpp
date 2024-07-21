#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"
#include "spatial/gdal/functions.hpp"
#include "spatial/gdal/file_handler.hpp"

#include "ogrsf_frmts.h"

namespace spatial {

namespace gdal {

struct BindData : public TableFunctionData {

	string file_path;
	vector<LogicalType> field_sql_types;
	vector<string> field_names;
	string driver_name;
	string layer_name;
	CPLStringList dataset_creation_options;
	CPLStringList layer_creation_options;
	string target_srs;
	OGRwkbGeometryType geometry_type = wkbUnknown;

	BindData(string file_path, vector<LogicalType> field_sql_types, vector<string> field_names)
	    : file_path(std::move(file_path)), field_sql_types(std::move(field_sql_types)),
	      field_names(std::move(field_names)) {
	}
};

struct LocalState : public LocalFunctionData {
	ArenaAllocator arena;
	explicit LocalState(ClientContext &context) : arena(BufferAllocator::Get(context)) {
	}
};

struct GlobalState : public GlobalFunctionData {
	mutex lock;
	GDALDatasetUniquePtr dataset;
	OGRLayer *layer;
	vector<unique_ptr<OGRFieldDefn>> field_defs;

	GlobalState(GDALDatasetUniquePtr dataset, OGRLayer *layer, vector<unique_ptr<OGRFieldDefn>> field_defs)
	    : dataset(std::move(dataset)), layer(layer), field_defs(std::move(field_defs)) {
	}
};

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names,
                                     const vector<LogicalType> &sql_types) {

	auto bind_data = make_uniq<BindData>(input.info.file_path, sql_types, names);

	// check all the options in the copy info
	// and set
	for (auto &option : input.info.options) {
		if (StringUtil::Upper(option.first) == "DRIVER") {
			auto set = option.second.front();
			if (set.type().id() == LogicalTypeId::VARCHAR) {
				bind_data->driver_name = set.GetValue<string>();
			} else {
				throw BinderException("Driver name must be a string");
			}
		} else if (StringUtil::Upper(option.first) == "LAYER_NAME") {
			auto set = option.second.front();
			if (set.type().id() == LogicalTypeId::VARCHAR) {
				bind_data->layer_name = set.GetValue<string>();
			} else {
				throw BinderException("Layer name must be a string");
			}
		} else if (StringUtil::Upper(option.first) == "LAYER_CREATION_OPTIONS") {
			auto set = option.second;
			for (auto &s : set) {
				if (s.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("Layer creation options must be strings");
				}
				auto str = s.GetValue<string>();
				bind_data->layer_creation_options.AddString(str.c_str());
			}
		} else if (StringUtil::Upper(option.first) == "DATASET_CREATION_OPTIONS") {
			auto set = option.second;
			for (auto &s : set) {
				if (s.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("Dataset creation options must be strings");
				}
				auto str = s.GetValue<string>();
				bind_data->dataset_creation_options.AddString(str.c_str());
			}
		} else if (StringUtil::Upper(option.first) == "GEOMETRY_TYPE") {
			auto &set = option.second.front();
			if (set.type().id() == LogicalTypeId::VARCHAR) {
				auto type = set.GetValue<string>();
				if (StringUtil::CIEquals(type, "POINT")) {
					bind_data->geometry_type = wkbPoint;
				} else if (StringUtil::CIEquals(type, "LINESTRING")) {
					bind_data->geometry_type = wkbLineString;
				} else if (StringUtil::CIEquals(type, "POLYGON")) {
					bind_data->geometry_type = wkbPolygon;
				} else if (StringUtil::CIEquals(type, "MULTIPOINT")) {
					bind_data->geometry_type = wkbMultiPoint;
				} else if (StringUtil::CIEquals(type, "MULTILINESTRING")) {
					bind_data->geometry_type = wkbMultiLineString;
				} else if (StringUtil::CIEquals(type, "MULTIPOLYGON")) {
					bind_data->geometry_type = wkbMultiPolygon;
				} else if (StringUtil::CIEquals(type, "GEOMETRYCOLLECTION")) {
					bind_data->geometry_type = wkbGeometryCollection;
				} else {
					throw BinderException(
					    "Unknown geometry type '%s', expected one of 'POINT', 'LINESTRING', 'POLYGON', 'MULTIPOINT', "
					    "'MULTILINESTRING', 'MULTIPOLYGON', 'GEOMETRYCOLLECTION'",
					    type);
				}
			} else {
				throw BinderException("Geometry type must be a string");
			}
		} else if (StringUtil::Upper(option.first) == "SRS") {
			auto &set = option.second.front();
			if (set.type().id() == LogicalTypeId::VARCHAR) {
				bind_data->target_srs = set.GetValue<string>();
			} else {
				throw BinderException("SRS must be a string");
			}
		} else {
			throw BinderException("Unknown option '%s'", option.first);
		}
		// save dataset open options.. i guess?
	}

	if (bind_data->driver_name.empty()) {
		throw BinderException("Driver name must be specified");
	}

	if (bind_data->layer_name.empty()) {
		// Default to the base name of the file
		auto &fs = FileSystem::GetFileSystem(context);
		bind_data->layer_name = fs.ExtractBaseName(bind_data->file_path);
	}

	auto driver = GetGDALDriverManager()->GetDriverByName(bind_data->driver_name.c_str());
	if (!driver) {
		throw BinderException("Unknown driver '%s'", bind_data->driver_name);
	}

	// Try get the file extension from the driver
	auto file_ext = driver->GetMetadataItem(GDAL_DMD_EXTENSION);
	if (file_ext) {
		input.file_extension = file_ext;
	} else {
		// Space separated list of file extensions
		auto file_exts = driver->GetMetadataItem(GDAL_DMD_EXTENSIONS);
		if (file_exts) {
			auto exts = StringUtil::Split(file_exts, ' ');
			if (!exts.empty()) {
				input.file_extension = exts[0];
			}
		}
	}

	// Driver specific checks
	if (bind_data->driver_name == "OpenFileGDB" && bind_data->geometry_type == wkbUnknown) {
		throw BinderException("OpenFileGDB requires 'GEOMETRY_TYPE' parameter to be set when writing!");
	}

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Local
//===--------------------------------------------------------------------===//
static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {
	auto local_data = make_uniq<LocalState>(context.client);
	return std::move(local_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//
static bool IsGeometryType(const LogicalType &type) {
	return type == core::GeoTypes::WKB_BLOB() || type == core::GeoTypes::POINT_2D() ||
	       type == core::GeoTypes::GEOMETRY();
}

static unique_ptr<OGRFieldDefn> OGRFieldTypeFromLogicalType(const string &name, const LogicalType &type) {
	// TODO: Set OGRFieldSubType for integers and integer lists
	// TODO: Set string width?

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto field = make_uniq<OGRFieldDefn>(name.c_str(), OFTInteger);
		field->SetSubType(OFSTBoolean);
		return field;
	}
	case LogicalTypeId::TINYINT: {
		// There is no subtype for byte?
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTInteger);
	}
	case LogicalTypeId::SMALLINT: {
		auto field = make_uniq<OGRFieldDefn>(name.c_str(), OFTInteger);
		field->SetSubType(OFSTInt16);
		return field;
	}
	case LogicalTypeId::INTEGER: {
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTInteger);
	}
	case LogicalTypeId::BIGINT:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTInteger64);
	case LogicalTypeId::FLOAT: {
		auto field = make_uniq<OGRFieldDefn>(name.c_str(), OFTReal);
		field->SetSubType(OFSTFloat32);
		return field;
	}
	case LogicalTypeId::DOUBLE:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTReal);
	case LogicalTypeId::VARCHAR:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTString);
	case LogicalTypeId::BLOB:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTBinary);
	case LogicalTypeId::DATE:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTDate);
	case LogicalTypeId::TIME:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTTime);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_TZ:
		return make_uniq<OGRFieldDefn>(name.c_str(), OFTDateTime);
	case LogicalTypeId::LIST: {
		auto child_type = ListType::GetChildType(type);
		switch (child_type.id()) {
		case LogicalTypeId::BOOLEAN: {
			auto field = make_uniq<OGRFieldDefn>(name.c_str(), OFTIntegerList);
			field->SetSubType(OFSTBoolean);
			return field;
		}
		case LogicalTypeId::TINYINT: {
			// There is no subtype for byte?
			return make_uniq<OGRFieldDefn>(name.c_str(), OFTIntegerList);
		}
		case LogicalTypeId::SMALLINT: {
			auto field = make_uniq<OGRFieldDefn>(name.c_str(), OFTIntegerList);
			field->SetSubType(OFSTInt16);
			return field;
		}
		case LogicalTypeId::INTEGER:
			return make_uniq<OGRFieldDefn>(name.c_str(), OFTIntegerList);
		case LogicalTypeId::BIGINT:
			return make_uniq<OGRFieldDefn>(name.c_str(), OFTInteger64List);
		case LogicalTypeId::FLOAT: {
			auto field = make_uniq<OGRFieldDefn>(name.c_str(), OFTRealList);
			field->SetSubType(OFSTFloat32);
			return field;
		}
		case LogicalTypeId::DOUBLE:
			return make_uniq<OGRFieldDefn>(name.c_str(), OFTRealList);
		case LogicalTypeId::VARCHAR:
			return make_uniq<OGRFieldDefn>(name.c_str(), OFTStringList);
		default:
			throw NotImplementedException("Unsupported type for OGR: %s", type.ToString());
		}
	}
	default:
		throw NotImplementedException("Unsupported type for OGR: %s", type.ToString());
	}
}
static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data,
                                                 const string &file_path) {

	auto &gdal_data = bind_data.Cast<BindData>();
	GDALDriver *driver = GetGDALDriverManager()->GetDriverByName(gdal_data.driver_name.c_str());
	if (!driver) {
		throw IOException("Could not open driver");
	}

	// Create the dataset
	auto &client_ctx = GDALClientContextState::GetOrCreate(context);
	auto prefixed_path = client_ctx.GetPrefix(file_path);
	auto dataset = GDALDatasetUniquePtr(
	    driver->Create(prefixed_path.c_str(), 0, 0, 0, GDT_Unknown, gdal_data.dataset_creation_options));
	if (!dataset) {
		throw IOException("Could not open dataset");
	}

	// Set the SRS if provided
	OGRSpatialReference srs;
	if (!gdal_data.target_srs.empty()) {
		srs.SetFromUserInput(gdal_data.target_srs.c_str());
	}
	// Not all GDAL drivers check if the SRS is empty (cough cough GeoJSONSeq)
	// so we have to pass nullptr if we want the default behavior.
	OGRSpatialReference *srs_ptr = gdal_data.target_srs.empty() ? nullptr : &srs;

	auto layer = dataset->CreateLayer(gdal_data.layer_name.c_str(), srs_ptr, gdal_data.geometry_type,
	                                  gdal_data.layer_creation_options);
	if (!layer) {
		throw IOException("Could not create layer");
	}

	// Create the layer field definitions
	idx_t geometry_field_count = 0;
	vector<unique_ptr<OGRFieldDefn>> field_defs;
	for (idx_t i = 0; i < gdal_data.field_names.size(); i++) {
		auto &name = gdal_data.field_names[i];
		auto &type = gdal_data.field_sql_types[i];

		if (IsGeometryType(type)) {
			geometry_field_count++;
			if (geometry_field_count > 1) {
				throw NotImplementedException("Multiple geometry fields not supported yet");
			}
		} else {
			auto field = OGRFieldTypeFromLogicalType(name, type);
			if (layer->CreateField(field.get()) != OGRERR_NONE) {
				throw IOException("Could not create attribute field");
			}
			// TODO: ^ Like we do here vvv
			field_defs.push_back(std::move(field));
		}
	}
	auto global_data = make_uniq<GlobalState>(std::move(dataset), layer, std::move(field_defs));

	return std::move(global_data);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

static OGRGeometryUniquePtr OGRGeometryFromValue(const LogicalType &type, const Value &value, ArenaAllocator &arena) {
	if (type == core::GeoTypes::WKB_BLOB()) {
		auto str = value.GetValueUnsafe<string_t>();

		OGRGeometry *ptr;
		size_t consumed;
		auto ok = OGRGeometryFactory::createFromWkb(str.GetDataUnsafe(), nullptr, &ptr, str.GetSize(), wkbVariantIso,
		                                            consumed);

		if (ok != OGRERR_NONE) {
			throw IOException("Could not parse WKB");
		}
		return OGRGeometryUniquePtr(ptr);
	} else if (type == core::GeoTypes::GEOMETRY()) {
		auto blob = value.GetValueUnsafe<string_t>();
		uint32_t size;
		auto wkb = core::WKBWriter::Write(core::geometry_t(blob), &size, arena);
		OGRGeometry *ptr;
		auto ok = OGRGeometryFactory::createFromWkb(wkb, nullptr, &ptr, size, wkbVariantIso);
		if (ok != OGRERR_NONE) {
			throw IOException("Could not parse WKB");
		}
		return OGRGeometryUniquePtr(ptr);
	} else if (type == core::GeoTypes::POINT_2D()) {
		auto children = StructValue::GetChildren(value);
		auto x = children[0].GetValue<double>();
		auto y = children[1].GetValue<double>();
		auto ogr_point = new OGRPoint(x, y);
		return OGRGeometryUniquePtr(ogr_point);
	} else {
		throw NotImplementedException("Unsupported geometry type");
	}
}

static void SetOgrFieldFromValue(OGRFeature *feature, int field_idx, const LogicalType &type, const Value &value) {
	// TODO: Set field by index always instead of by name for performance.
	if (value.IsNull()) {
		feature->SetFieldNull(field_idx);
		return;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		feature->SetField(field_idx, value.GetValue<bool>());
		break;
	case LogicalTypeId::TINYINT:
		feature->SetField(field_idx, value.GetValue<int8_t>());
		break;
	case LogicalTypeId::SMALLINT:
		feature->SetField(field_idx, value.GetValue<int16_t>());
		break;
	case LogicalTypeId::INTEGER:
		feature->SetField(field_idx, value.GetValue<int32_t>());
		break;
	case LogicalTypeId::BIGINT:
		feature->SetField(field_idx, (GIntBig)value.GetValue<int64_t>());
		break;
	case LogicalTypeId::FLOAT:
		feature->SetField(field_idx, value.GetValue<float>());
		break;
	case LogicalTypeId::DOUBLE:
		feature->SetField(field_idx, value.GetValue<double>());
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB: {
		auto str = value.GetValueUnsafe<string_t>();
		feature->SetField(field_idx, (int)str.GetSize(), str.GetDataUnsafe());
	} break;
	case LogicalTypeId::DATE: {
		auto date = value.GetValueUnsafe<date_t>();
		auto year = Date::ExtractYear(date);
		auto month = Date::ExtractMonth(date);
		auto day = Date::ExtractDay(date);
		feature->SetField(field_idx, year, month, day, 0, 0, 0, 0);
	} break;
	case LogicalTypeId::TIME: {
		auto time = value.GetValueUnsafe<dtime_t>();
		auto hour = static_cast<int>(time.micros / Interval::MICROS_PER_HOUR);
		auto minute = static_cast<int>((time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE);
		auto second = static_cast<float>(static_cast<double>(time.micros % Interval::MICROS_PER_MINUTE) /
		                                 static_cast<double>(Interval::MICROS_PER_SEC));
		feature->SetField(field_idx, 0, 0, 0, hour, minute, second, 0);
	} break;
	case LogicalTypeId::TIMESTAMP: {
		auto timestamp = value.GetValueUnsafe<timestamp_t>();
		auto date = Timestamp::GetDate(timestamp);
		auto time = Timestamp::GetTime(timestamp);
		auto year = Date::ExtractYear(date);
		auto month = Date::ExtractMonth(date);
		auto day = Date::ExtractDay(date);
		auto hour = static_cast<int>((time.micros % Interval::MICROS_PER_DAY) / Interval::MICROS_PER_HOUR);
		auto minute = static_cast<int>((time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE);
		auto second = static_cast<float>(static_cast<double>(time.micros % Interval::MICROS_PER_MINUTE) /
		                                 static_cast<double>(Interval::MICROS_PER_SEC));
		feature->SetField(field_idx, year, month, day, hour, minute, second, 0);
	} break;
	case LogicalTypeId::TIMESTAMP_NS: {
		auto timestamp = value.GetValueUnsafe<timestamp_t>();
		timestamp = Timestamp::FromEpochNanoSeconds(timestamp.value);
		auto date = Timestamp::GetDate(timestamp);
		auto time = Timestamp::GetTime(timestamp);
		auto year = Date::ExtractYear(date);
		auto month = Date::ExtractMonth(date);
		auto day = Date::ExtractDay(date);
		auto hour = static_cast<int>((time.micros % Interval::MICROS_PER_DAY) / Interval::MICROS_PER_HOUR);
		auto minute = static_cast<int>((time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE);
		auto second = static_cast<float>(static_cast<double>(time.micros % Interval::MICROS_PER_MINUTE) /
		                                 static_cast<double>(Interval::MICROS_PER_SEC));
		feature->SetField(field_idx, year, month, day, hour, minute, second, 0);
	} break;
	case LogicalTypeId::TIMESTAMP_MS: {
		auto timestamp = value.GetValueUnsafe<timestamp_t>();
		timestamp = Timestamp::FromEpochMs(timestamp.value);
		auto date = Timestamp::GetDate(timestamp);
		auto time = Timestamp::GetTime(timestamp);
		auto year = Date::ExtractYear(date);
		auto month = Date::ExtractMonth(date);
		auto day = Date::ExtractDay(date);
		auto hour = static_cast<int>((time.micros % Interval::MICROS_PER_DAY) / Interval::MICROS_PER_HOUR);
		auto minute = static_cast<int>((time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE);
		auto second = static_cast<float>(static_cast<double>(time.micros % Interval::MICROS_PER_MINUTE) /
		                                 static_cast<double>(Interval::MICROS_PER_SEC));
		feature->SetField(field_idx, year, month, day, hour, minute, second, 0);
	} break;
	case LogicalTypeId::TIMESTAMP_SEC: {
		auto timestamp = value.GetValueUnsafe<timestamp_t>();
		timestamp = Timestamp::FromEpochSeconds(timestamp.value);
		auto date = Timestamp::GetDate(timestamp);
		auto time = Timestamp::GetTime(timestamp);
		auto year = Date::ExtractYear(date);
		auto month = Date::ExtractMonth(date);
		auto day = Date::ExtractDay(date);
		auto hour = static_cast<int>((time.micros % Interval::MICROS_PER_DAY) / Interval::MICROS_PER_HOUR);
		auto minute = static_cast<int>((time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE);
		auto second = static_cast<float>(static_cast<double>(time.micros % Interval::MICROS_PER_MINUTE) /
		                                 static_cast<double>(Interval::MICROS_PER_SEC));
		feature->SetField(field_idx, year, month, day, hour, minute, second, 0);
	} break;
	case LogicalTypeId::TIMESTAMP_TZ: {
		// Not sure what to with the timezone, just let GDAL parse it?
		auto timestamp = value.GetValueUnsafe<timestamp_t>();
		auto time_str = Timestamp::ToString(timestamp);
		feature->SetField(field_idx, time_str.c_str());
	} break;
	default:
		// TODO: Handle list types
		throw NotImplementedException("Unsupported field type");
	}
}

static void Sink(ExecutionContext &context, FunctionData &bdata, GlobalFunctionData &gstate, LocalFunctionData &lstate,
                 DataChunk &input) {
	auto &bind_data = bdata.Cast<BindData>();
	auto &global_state = gstate.Cast<GlobalState>();
	auto &local_state = lstate.Cast<LocalState>();
	local_state.arena.Reset();

	lock_guard<mutex> d_lock(global_state.lock);
	auto layer = global_state.layer;

	// Create the feature
	input.Flatten();
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {

		auto feature = OGRFeatureUniquePtr(OGRFeature::CreateFeature(layer->GetLayerDefn()));

		// Geometry fields do not count towards the field index, so we need to keep track of them separately.
		idx_t field_idx = 0;
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto &type = bind_data.field_sql_types[col_idx];
			auto value = input.GetValue(col_idx, row_idx);

			if (IsGeometryType(type)) {
				// TODO: check how many geometry fields there are and use the correct one.
				auto geom = OGRGeometryFromValue(type, value, local_state.arena);
				if (bind_data.geometry_type != wkbUnknown && geom->getGeometryType() != bind_data.geometry_type) {
					auto got_name =
					    StringUtil::Replace(StringUtil::Upper(OGRGeometryTypeToName(geom->getGeometryType())), " ", "");
					auto expected_name =
					    StringUtil::Replace(StringUtil::Upper(OGRGeometryTypeToName(bind_data.geometry_type)), " ", "");
					throw InvalidInputException("Expected all geometries to be of type '%s', but got one of type '%s'",
					                            expected_name, got_name);
				}

				if (feature->SetGeometry(geom.get()) != OGRERR_NONE) {
					throw IOException("Could not set geometry");
				}
			} else {
				SetOgrFieldFromValue(feature.get(), (int)field_idx, type, value);
				field_idx++;
			}
		}
		if (layer->CreateFeature(feature.get()) != OGRERR_NONE) {
			throw IOException("Could not create feature");
		}
	}
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//

static void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                    LocalFunctionData &lstate) {
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = (GlobalState &)gstate;
	global_state.dataset->FlushCache();
	global_state.dataset->Close();
}

void GdalCopyFunction::Register(DatabaseInstance &db) {
	// register the copy function
	CopyFunction info("GDAL");
	info.copy_to_bind = Bind;
	info.copy_to_initialize_local = InitLocal;
	info.copy_to_initialize_global = InitGlobal;
	info.copy_to_sink = Sink;
	info.copy_to_combine = Combine;
	info.copy_to_finalize = Finalize;
	info.extension = "gdal";

	ExtensionUtil::RegisterFunction(db, info);
}

} // namespace gdal

} // namespace spatial
